// Copyright (C) 2026 Carel Kuun (Flogent FloSync fork of Syncthing).
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

// This file holds the FloSync additions to the model. They back the two REST
// endpoints /rest/flosync/stream and /rest/flosync/delete (see
// lib/api/api_flosync.go). Everything here reuses the existing machinery
// (global index, block availability, the puller's request path and the local
// index update used by the scanner), nothing here writes to the folder on
// disk.

package model

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/syncthing/syncthing/internal/itererr"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/semaphore"
)

// Errors returned by the FloSync model methods. They are wrapped with a
// detail message, test with errors.Is. The API maps them to HTTP statuses
// (400, 404, 409, 416 and 503).
var (
	ErrFloSyncBadRequest  = errors.New("bad request")
	ErrFloSyncNotFound    = errors.New("not found")
	ErrFloSyncHeld        = errors.New("this device holds it as a normal file")
	ErrFloSyncBadRange    = errors.New("range not satisfiable")
	ErrFloSyncNoSource    = errors.New("no connected device can provide the current version")
	ErrFloSyncUnavailable = errors.New("folder is not available")
)

const (
	// Number of blocks a stream fetches ahead of the reader, and the most
	// data it keeps in memory for that (the puller's default pending data).
	floSyncWindowBlocks   = 8
	floSyncWindowMaxBytes = 2 * protocol.MaxBlockSize
)

// floSyncBlockTimeout bounds one block request to one device. The puller
// puts no limit of its own on a block request (only the folder context and
// the connection level ReceiveTimeout of 300 s apply), a stream that a person
// is waiting for must move on to the next device sooner. A variable so that
// tests can shorten it.
var floSyncBlockTimeout = 2 * time.Minute

// FloSyncStream delivers the bytes of the current global version of a file
// that this device does not hold, fetched from connected devices.
type FloSyncStream interface {
	// Size is the number of bytes the stream delivers in total.
	Size() int64
	// Info is the global file the stream was opened for (without blocks).
	Info() protocol.FileInfo
	// Next returns the next verified piece of the requested range, in order.
	// It returns io.EOF after the last piece. The slice is not reused. After
	// an error every further call returns the same error.
	Next() ([]byte, error)
	// Close stops all outstanding requests and waits for them to end.
	Close() error
}

// FloSyncDeleteResult tells what FloSyncDelete did.
type FloSyncDeleteResult struct {
	// Deleted is the number of entries announced as deleted by this device.
	Deleted int `json:"deleted"`
	// Held is the number of entries below a directory that this device holds
	// as normal entries. They are left to the normal scan and delete.
	Held int `json:"held"`
}

// floSyncCleanName checks a path from the API and returns it in the form the
// index uses (slash separated, relative, no trailing slash).
func floSyncCleanName(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: file is empty", ErrFloSyncBadRequest)
	}
	if strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("%w: file contains a NUL character", ErrFloSyncBadRequest)
	}
	if filepath.VolumeName(name) != "" {
		return "", fmt.Errorf("%w: file must be a relative path", ErrFloSyncBadRequest)
	}
	n := strings.TrimRight(filepath.ToSlash(name), "/")
	if n == "" || strings.HasPrefix(n, "/") {
		return "", fmt.Errorf("%w: file must be a relative path inside the folder", ErrFloSyncBadRequest)
	}
	for _, part := range strings.Split(n, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("%w: file must be a clean relative path", ErrFloSyncBadRequest)
		}
	}
	if fs.IsInternal(filepath.FromSlash(n)) {
		return "", fmt.Errorf("%w: file is internal to Syncthing", ErrFloSyncBadRequest)
	}
	return n, nil
}

// floSyncHeld reports whether a local index entry means that this device
// holds the item as a normal file or directory (so the normal scan and delete
// apply, not the FloSync delete). Ignored, deleted and unsupported entries
// mean the device does not hold it.
func floSyncHeld(lf protocol.FileInfo) bool {
	return !lf.IsDeleted() && !lf.IsIgnored() && !lf.IsUnsupported()
}

func floSyncVerifyBlock(buf []byte, block protocol.BlockInfo) error {
	if len(buf) != block.Size {
		return fmt.Errorf("length mismatch %d != %d", len(buf), block.Size)
	}
	hash := sha256.Sum256(buf)
	if !bytes.Equal(hash[:], block.Hash) {
		return fmt.Errorf("hash mismatch %x != %x", hash, block.Hash)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Stream

// floSyncPiece is one block of the file together with the part of it that
// belongs to the requested range. A whole block is always fetched, the hash
// can only be checked on whole blocks.
type floSyncPiece struct {
	block protocol.BlockInfo
	from  int // start within the block, inclusive
	to    int // end within the block, exclusive
}

type floSyncResult struct {
	buf []byte
	err error
}

type floSyncJob struct {
	piece floSyncPiece
	res   chan floSyncResult // buffered, one result
}

type floSyncStream struct {
	m      *model
	cfg    config.FolderConfiguration
	file   protocol.FileInfo // the global file, pinned at open
	size   int64
	pieces []floSyncPiece

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	jobs    chan *floSyncJob // jobs in file order, filled by the producer
	limiter *semaphore.Semaphore

	badMut sync.Mutex
	bad    map[protocol.DeviceID]struct{} // devices that failed during this stream, asked last

	// used by the reader only
	delivered int
	err       error
}

// OpenFloSyncStream opens a stream of length bytes from offset of the current
// global version of the file. A negative length means up to the end of the
// file. The stream is cancelled when ctx is, and Close must be called.
//
// The errors are ErrFloSyncBadRequest (bad parameters), ErrFloSyncNotFound
// (not in the global index, deleted, a directory or a symlink),
// ErrFloSyncHeld (this device holds the file as a normal file, read it from
// disk), ErrFloSyncBadRange and ErrFloSyncNoSource (no connected device has
// the version). It does not write to the folder and does not change any index
// entry.
func (m *model) OpenFloSyncStream(ctx context.Context, folder, name string, offset, length int64) (FloSyncStream, error) {
	name, err := floSyncCleanName(name)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, fmt.Errorf("%w: offset is negative", ErrFloSyncBadRequest)
	}

	m.mut.RLock()
	err = m.checkFolderRunningRLocked(folder)
	cfg, cfgOK := m.folderCfgs[folder]
	m.mut.RUnlock()
	if err != nil {
		return nil, err
	}
	if !cfgOK {
		return nil, ErrFolderMissing
	}
	if cfg.Type == config.FolderTypeReceiveEncrypted {
		return nil, fmt.Errorf("%w: the folder holds encrypted data", ErrFloSyncBadRequest)
	}

	gf, ok, err := m.sdb.GetGlobalFile(folder, name)
	if err != nil {
		return nil, err
	}
	switch {
	case !ok:
		return nil, fmt.Errorf("%w: %q is not in the global index", ErrFloSyncNotFound, name)
	case gf.IsDeleted():
		return nil, fmt.Errorf("%w: %q is deleted", ErrFloSyncNotFound, name)
	case gf.IsDirectory():
		return nil, fmt.Errorf("%w: %q is a directory", ErrFloSyncNotFound, name)
	case gf.IsSymlink():
		return nil, fmt.Errorf("%w: %q is a symbolic link, not a regular file", ErrFloSyncNotFound, name)
	case gf.IsInvalid():
		return nil, fmt.Errorf("%w: no device has a valid copy of %q", ErrFloSyncNotFound, name)
	}

	lf, lok, err := m.sdb.GetDeviceFile(folder, protocol.LocalDeviceID, name)
	if err != nil {
		return nil, err
	}
	if lok && floSyncHeld(lf) {
		return nil, fmt.Errorf("%w: %q is on this device, read it from disk", ErrFloSyncHeld, name)
	}

	size := gf.Size
	if offset > size {
		return nil, fmt.Errorf("%w: offset %d is beyond the end of the file (%d bytes)", ErrFloSyncBadRange, offset, size)
	}
	if length < 0 {
		length = size - offset
	} else if length > size-offset {
		return nil, fmt.Errorf("%w: offset %d and length %d go beyond the end of the file (%d bytes)", ErrFloSyncBadRange, offset, length, size)
	}

	pieces, err := floSyncPlan(gf, offset, length)
	if err != nil {
		return nil, err
	}
	// Fail now, with a proper status, when no connected device has the data.
	// A block of zeroes needs no device, so look at the first block that does.
	for _, p := range pieces {
		if p.block.IsEmpty() {
			continue
		}
		if len(m.blockAvailability(cfg, gf, p.block)) == 0 {
			return nil, fmt.Errorf("%w: %q is on no connected device", ErrFloSyncNoSource, name)
		}
		break
	}

	s := &floSyncStream{
		m:      m,
		cfg:    cfg,
		file:   gf,
		size:   length,
		pieces: pieces,
		jobs:   make(chan *floSyncJob, floSyncWindowBlocks+2),
		bad:    make(map[protocol.DeviceID]struct{}),
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.limiter = semaphore.New(min(floSyncWindowBlocks*gf.BlockSize(), floSyncWindowMaxBytes))
	s.markInvalidHolders()
	s.wg.Add(1)
	go s.produce()

	slog.Debug("FloSync stream opened", slog.String("folder", folder), slogutil.FilePath(name), slog.Int64("offset", offset), slog.Int64("length", length))
	return s, nil
}

// floSyncPlan lists the blocks that cover [offset, offset+length) and checks
// that the block list of the file is consistent.
func floSyncPlan(file protocol.FileInfo, offset, length int64) ([]floSyncPiece, error) {
	if file.Size == 0 {
		// An empty file has no data. The scanner gives it one block of zero
		// size, which is never requested.
		if len(file.Blocks) > 1 || (len(file.Blocks) == 1 && file.Blocks[0].Size != 0) {
			return nil, fmt.Errorf("inconsistent block list for %q: file size is 0", file.Name)
		}
		return nil, nil
	}

	var next int64
	for _, b := range file.Blocks {
		if b.Offset != next || b.Size <= 0 {
			return nil, fmt.Errorf("inconsistent block list for %q", file.Name)
		}
		next += int64(b.Size)
	}
	if next != file.Size {
		return nil, fmt.Errorf("inconsistent block list for %q: blocks add up to %d, file size is %d", file.Name, next, file.Size)
	}
	if length == 0 {
		return nil, nil
	}

	end := offset + length
	i := sort.Search(len(file.Blocks), func(i int) bool {
		return file.Blocks[i].Offset+int64(file.Blocks[i].Size) > offset
	})
	var pieces []floSyncPiece
	for ; i < len(file.Blocks) && file.Blocks[i].Offset < end; i++ {
		b := file.Blocks[i]
		pieces = append(pieces, floSyncPiece{
			block: b,
			from:  int(max(offset-b.Offset, 0)),
			to:    int(min(end-b.Offset, int64(b.Size))),
		})
	}
	return pieces, nil
}

func (s *floSyncStream) Size() int64 { return s.size }

func (s *floSyncStream) Info() protocol.FileInfo {
	fi := s.file
	fi.Blocks = nil
	return fi
}

// produce starts the block fetches in file order, at most a window ahead of
// what the reader has taken.
func (s *floSyncStream) produce() {
	defer s.wg.Done()
	defer close(s.jobs)

	for _, p := range s.pieces {
		if err := s.limiter.TakeWithContext(s.ctx, p.block.Size); err != nil {
			return
		}
		job := &floSyncJob{piece: p, res: make(chan floSyncResult, 1)}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			buf, err := s.fetch(p.block)
			job.res <- floSyncResult{buf: buf, err: err}
		}()
		select {
		case s.jobs <- job:
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *floSyncStream) Next() ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.delivered >= len(s.pieces) {
		return nil, io.EOF
	}

	var job *floSyncJob
	select {
	case job = <-s.jobs:
	case <-s.ctx.Done():
		s.err = s.ctx.Err()
		return nil, s.err
	}
	if job == nil {
		// The producer stopped early, which only happens when cancelled.
		s.err = firstErr(s.ctx.Err(), errors.New("stream stopped"))
		return nil, s.err
	}

	var res floSyncResult
	select {
	case res = <-job.res:
	case <-s.ctx.Done():
		s.err = s.ctx.Err()
		return nil, s.err
	}
	s.limiter.Give(job.piece.block.Size)
	s.delivered++
	if res.err != nil {
		s.err = res.err
		s.cancel()
		return nil, s.err
	}
	return res.buf[job.piece.from:job.piece.to], nil
}

func firstErr(a, b error) error {
	if a != nil {
		return a
	}
	return b
}

func (s *floSyncStream) Close() error {
	s.cancel()
	s.wg.Wait()
	return nil
}

func (s *floSyncStream) markBad(id protocol.DeviceID) {
	s.badMut.Lock()
	s.bad[id] = struct{}{}
	s.badMut.Unlock()
}

// markInvalidHolders puts the devices that have this version of the file only
// as an invalid entry (in FloSync mostly an ignored entry, a placeholder) last
// in line. The index counts them as having the file, but they usually cannot
// serve it, so asking them first costs a round trip for nothing.
func (s *floSyncStream) markInvalidHolders() {
	devs, err := s.m.sdb.GetGlobalAvailability(s.cfg.ID, s.file.Name)
	if err != nil {
		return
	}
	for _, dev := range devs {
		if fi, ok, err := s.m.sdb.GetDeviceFile(s.cfg.ID, dev, s.file.Name); err == nil && ok && fi.IsInvalid() {
			s.markBad(dev)
		}
	}
}

// fetch gets one block, verified against its hash, the way the puller does:
// try the least busy device that has it and go on to the next one when the
// request fails or the data is wrong. Devices that failed earlier in this
// stream are asked last.
func (s *floSyncStream) fetch(block protocol.BlockInfo) ([]byte, error) {
	if block.IsEmpty() {
		// The puller does not request a block of zeroes either.
		return make([]byte, block.Size), nil
	}

	var good, late []Availability
	for _, a := range s.m.blockAvailability(s.cfg, s.file, block) {
		s.badMut.Lock()
		_, bad := s.bad[a.ID]
		s.badMut.Unlock()
		if bad {
			late = append(late, a)
		} else {
			good = append(good, a)
		}
	}

	blockNo := int(block.Offset / int64(s.file.BlockSize()))
	var lastErr error
	for {
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}

		candidates := &good
		found := activity.leastBusy(good)
		if found == -1 {
			candidates = &late
			found = activity.leastBusy(late)
		}
		if found == -1 {
			if lastErr != nil {
				return nil, fmt.Errorf("%w: %w", ErrFloSyncNoSource, lastErr)
			}
			return nil, fmt.Errorf("%w: no connected device has the block at offset %d", ErrFloSyncNoSource, block.Offset)
		}
		selected := (*candidates)[found]
		(*candidates)[found] = (*candidates)[len(*candidates)-1]
		*candidates = (*candidates)[:len(*candidates)-1]

		activity.using(selected)
		reqCtx, cancel := context.WithTimeout(s.ctx, floSyncBlockTimeout)
		buf, err := s.m.RequestGlobal(reqCtx, selected.ID, s.cfg.ID, s.file.Name, blockNo, block.Offset, block.Size, block.Hash, selected.FromTemporary)
		cancel()
		activity.done(selected)
		if err == nil {
			err = floSyncVerifyBlock(buf, block)
		}
		if err != nil {
			slog.Debug("FloSync stream block request failed", slog.String("folder", s.cfg.ID), slogutil.FilePath(s.file.Name), slog.Int64("offset", block.Offset), selected.ID.LogAttr(), slogutil.Error(err))
			if s.ctx.Err() == nil {
				s.markBad(selected.ID)
			}
			lastErr = err
			continue
		}
		return buf, nil
	}
}

// ---------------------------------------------------------------------------
// Delete

// FloSyncDelete announces a file, or a whole directory tree, as deleted by
// this device, as if it had existed here and been deleted: each entry gets a
// new version (the global version bumped for this device) with Deleted set,
// this device as the modifier, stored in the local index and sent to the
// peers. It is meant for entries this device holds only as ignored or not at
// all. Nothing is downloaded and nothing on disk is touched.
//
// For a directory every live global entry below it is announced (files first,
// then directories, deepest first) and then the directory itself. Entries
// this device holds as normal entries are not touched (counted in Held),
// the normal scan announces those when they are gone from disk.
//
// The errors are ErrFloSyncNotFound (not in the global index, or already
// deleted), ErrFloSyncHeld (this device holds the file as a normal file, or
// everything announceable under the directory is held) and
// ErrFloSyncBadRequest. It works inside the folder's own loop, so it never
// runs at the same time as a scan or a pull.
func (m *model) FloSyncDelete(ctx context.Context, folder, name string) (FloSyncDeleteResult, error) {
	name, err := floSyncCleanName(name)
	if err != nil {
		return FloSyncDeleteResult{}, err
	}

	m.mut.RLock()
	err = m.checkFolderRunningRLocked(folder)
	runner, _ := m.folderRunners.Get(folder)
	cfg := m.folderCfgs[folder]
	m.mut.RUnlock()
	if err != nil {
		return FloSyncDeleteResult{}, err
	}
	if cfg.Type != config.FolderTypeSendReceive && cfg.Type != config.FolderTypeSendOnly {
		return FloSyncDeleteResult{}, fmt.Errorf("%w: a delete cannot be announced from a receive only or encrypted folder", ErrFloSyncBadRequest)
	}
	return runner.floSyncDelete(ctx, name)
}

// doInSyncCtx is doInSync that stops waiting when ctx is done. Once the folder
// has taken the request it runs to the end.
func (f *folder) doInSyncCtx(ctx context.Context, fn func(context.Context) error) error {
	req := syncRequest{
		fn:  fn,
		err: make(chan error, 1),
	}

	select {
	case f.doInSyncChan <- req:
		return <-req.err
	case <-f.done:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *folder) floSyncDelete(ctx context.Context, name string) (FloSyncDeleteResult, error) {
	var res FloSyncDeleteResult
	var opErr error

	// Like a scan request, wait for the initial scan: until then the local
	// index may not show what is on disk.
	select {
	case <-f.initialScanFinished:
	case <-f.done:
		return res, context.Canceled
	case <-ctx.Done():
		return res, ctx.Err()
	}

	// Whatever the function returns is taken as an error of the folder, so
	// only an unhealthy folder is returned. It must never announce a delete
	// while the folder root or its marker is missing.
	err := f.doInSyncCtx(ctx, func(_ context.Context) error {
		if err := f.getHealthErrorWithoutIgnores(); err != nil {
			opErr = fmt.Errorf("%w: %w", ErrFloSyncUnavailable, err)
			return err
		}
		res, opErr = f.floSyncDeleteSynced(ctx, name)
		return nil
	})
	if opErr != nil {
		return res, opErr
	}
	return res, err
}

type floSyncEntry struct {
	name  string
	isDir bool
}

// floSyncDeleteSynced does the work of floSyncDelete, inside the folder loop.
func (f *folder) floSyncDeleteSynced(ctx context.Context, name string) (FloSyncDeleteResult, error) {
	var res FloSyncDeleteResult

	gf, ok, err := f.db.GetGlobalFile(f.folderID, name)
	if err != nil {
		return res, err
	}
	alive := ok && !gf.IsDeleted() && !gf.IsInvalid()

	var entries []floSyncEntry
	if alive && !gf.IsDirectory() {
		entries = append(entries, floSyncEntry{name: name})
	} else {
		// A directory, or a name that is deleted or unknown but may still
		// have live entries below it (the directory itself can already be
		// announced as deleted by the normal scan).
		for fm, err := range itererr.Zip(f.db.AllGlobalFilesPrefix(f.folderID, name+"/")) {
			if err != nil {
				return res, err
			}
			if fm.Deleted || fm.IsInvalid() {
				continue
			}
			entries = append(entries, floSyncEntry{name: osutil.NormalizedFilename(fm.Name), isDir: fm.IsDirectory()})
		}
		if alive {
			entries = append(entries, floSyncEntry{name: name, isDir: true})
		}
	}
	if len(entries) == 0 {
		if ok && gf.IsDeleted() {
			return res, fmt.Errorf("%w: %q is already deleted", ErrFloSyncNotFound, name)
		}
		return res, fmt.Errorf("%w: %q is not in the global index", ErrFloSyncNotFound, name)
	}

	// Files first, then directories, deepest first.
	slices.SortFunc(entries, func(a, b floSyncEntry) int {
		if a.isDir != b.isDir {
			if !a.isDir {
				return -1
			}
			return 1
		}
		if a.isDir {
			if da, db := strings.Count(a.name, "/"), strings.Count(b.name, "/"); da != db {
				return db - da
			}
		}
		return strings.Compare(a.name, b.name)
	})

	batch := NewFileInfoBatch(func(fs []protocol.FileInfo) error {
		if err := f.updateLocals(fs); err != nil {
			return err
		}
		f.emitDiskChangeEvents(fs, events.LocalChangeDetected)
		return nil
	})

	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			_ = batch.Flush()
			return res, err
		}
		if err := batch.FlushIfFull(); err != nil {
			return res, err
		}

		lf, lok, err := f.db.GetDeviceFile(f.folderID, protocol.LocalDeviceID, e.name)
		if err != nil {
			return res, err
		}
		if lok && floSyncHeld(lf) {
			if e.name == name && !e.isDir {
				return res, fmt.Errorf("%w: %q is on this device, delete it normally", ErrFloSyncHeld, name)
			}
			res.Held++
			continue
		}

		// The global entry may have changed since the listing.
		ge, ok, err := f.db.GetGlobalFile(f.folderID, e.name)
		if err != nil {
			return res, err
		}
		if !ok || ge.IsDeleted() || ge.IsInvalid() {
			continue
		}

		// The new version is the global version bumped for this device, so
		// it is newer than anything the cluster has seen, as if this device
		// had held the file at that version and then deleted it.
		nf := ge
		nf.Version = protocol.Vector{Counters: slices.Clone(ge.Version.Counters)}
		nf.SetDeleted(f.shortID)
		nf.LocalFlags = f.localFlags
		batch.Append(nf)
		res.Deleted++
	}

	if res.Deleted == 0 {
		if res.Held > 0 {
			return res, fmt.Errorf("%w: everything under %q is on this device, delete it normally", ErrFloSyncHeld, name)
		}
		return res, fmt.Errorf("%w: %q is already deleted", ErrFloSyncNotFound, name)
	}
	if err := batch.Flush(); err != nil {
		return res, err
	}

	slog.Info("Announced delete of items this device does not hold", f.LogAttr(), slogutil.FilePath(name), slog.Int("deleted", res.Deleted), slog.Int("held", res.Held))
	return res, nil
}
