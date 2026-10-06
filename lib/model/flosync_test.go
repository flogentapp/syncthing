// Copyright (C) 2026 Carel Kuun (Flogent FloSync fork of Syncthing).
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

const floSyncFolder = "default"

// A deterministic, non repeating data block of n bytes.
func floSyncData(n int, seed int64) []byte {
	buf := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(buf) //nolint:gosec
	return buf
}

func floSyncEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// requestLog records the block requests a fake device receives.
type requestLog struct {
	mut         sync.Mutex
	requests    []protocol.Request
	inFlight    int
	maxInFlight int
}

func (l *requestLog) count() int {
	l.mut.Lock()
	defer l.mut.Unlock()
	return len(l.requests)
}

// serve makes the fake connection answer block requests with the right slice
// of the file data, like a real device does. The delay function, when given,
// says how long to wait before answering a request.
func (f *fakeConnection) serve(log *requestLog, delay func(*protocol.Request) time.Duration) {
	f.RequestCalls(func(ctx context.Context, req *protocol.Request) ([]byte, error) {
		log.mut.Lock()
		log.requests = append(log.requests, *req)
		log.inFlight++
		log.maxInFlight = max(log.maxInFlight, log.inFlight)
		log.mut.Unlock()
		defer func() {
			log.mut.Lock()
			log.inFlight--
			log.mut.Unlock()
		}()

		if delay != nil {
			select {
			case <-time.After(delay(req)):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		f.mut.Lock()
		data := f.fileData[filepath.ToSlash(req.Name)]
		f.mut.Unlock()
		if req.Offset > int64(len(data)) {
			return nil, protocol.ErrNoSuchFile
		}
		end := min(int(req.Offset)+req.Size, len(data))
		return slices.Clone(data[req.Offset:end]), nil
	})
}

// indexCollector remembers every file a fake device was sent in an index or
// an index update.
type indexCollector struct {
	mut   sync.Mutex
	files []protocol.FileInfo
}

func collectIndex(fc *fakeConnection) *indexCollector {
	c := &indexCollector{}
	fc.setIndexFn(func(_ context.Context, _ string, fs []protocol.FileInfo) error {
		c.mut.Lock()
		for _, f := range fs {
			f.Name = filepath.ToSlash(f.Name)
			c.files = append(c.files, f)
		}
		c.mut.Unlock()
		return nil
	})
	return c
}

func (c *indexCollector) all() []protocol.FileInfo {
	c.mut.Lock()
	defer c.mut.Unlock()
	return slices.Clone(c.files)
}

// lastDelete returns the last record sent for the name that is a plain delete
// (deleted and valid). Records of other kinds are sent too, for example the
// ignored entries that the puller made.
func (c *indexCollector) lastDelete(name string) (protocol.FileInfo, bool) {
	c.mut.Lock()
	defer c.mut.Unlock()
	for i := len(c.files) - 1; i >= 0; i-- {
		if f := c.files[i]; f.Name == name && f.IsDeleted() && !f.IsInvalid() {
			return f, true
		}
	}
	return protocol.FileInfo{}, false
}

// deleteOrder returns the names of the plain deletes in the order sent.
func (c *indexCollector) deleteOrder() []string {
	c.mut.Lock()
	defer c.mut.Unlock()
	var names []string
	for _, f := range c.files {
		if f.IsDeleted() && !f.IsInvalid() {
			names = append(names, f.Name)
		}
	}
	return names
}

// invalidDeletes returns the records sent for the name that are deleted and
// invalid (an ignored delete record, which does not count as a delete).
func (c *indexCollector) invalidDeletes(name string) []protocol.FileInfo {
	c.mut.Lock()
	defer c.mut.Unlock()
	var res []protocol.FileInfo
	for _, f := range c.files {
		if f.Name == name && f.IsDeleted() && f.IsInvalid() {
			res = append(res, f)
		}
	}
	return res
}

// floSyncModel sets up the folder "default" (send receive) with one connected
// device, device1 (fc), and the given ignore patterns in force. Files that
// match the patterns are held by this device only as ignored entries.
func floSyncModel(t *testing.T, ignores []string, mod func(*config.FolderConfiguration)) (*testModel, *fakeConnection, config.FolderConfiguration) {
	t.Helper()
	w, fcfg := newDefaultCfgWrapper(t)
	if mod != nil {
		mod(&fcfg)
		setFolder(t, w, fcfg)
	}
	m := setupModel(t, w)

	folderIgnoresAlwaysReload(t, m, fcfg)
	if err := m.SetIgnores(floSyncFolder, ignores); err != nil {
		t.Fatal(err)
	}
	must(t, m.ScanFolder(floSyncFolder))

	fc := addFakeConn(m, device1, fcfg.ID)
	fc.folder = floSyncFolder
	fc.serve(&requestLog{}, nil)
	return m, fc, fcfg
}

// addRemote adds a file to what fc holds. The caller sends the index update.
func (f *fakeConnection) addRemote(name string, data []byte) {
	f.addFile(name, 0o644, protocol.FileInfoTypeFile, data)
}

// waitIgnored waits until this device holds the item as an ignored entry.
func waitIgnored(t *testing.T, m *testModel, names ...string) {
	t.Helper()
	for _, name := range names {
		floSyncEventually(t, "ignored entry for "+name, func() bool {
			fi, ok := m.testCurrentFolderFile(floSyncFolder, name)
			return ok && fi.IsIgnored()
		})
	}
}

// waitHeld waits until this device holds the item as a normal entry.
func waitHeld(t *testing.T, m *testModel, names ...string) {
	t.Helper()
	for _, name := range names {
		floSyncEventually(t, "normal entry for "+name, func() bool {
			fi, ok := m.testCurrentFolderFile(floSyncFolder, name)
			return ok && !fi.IsInvalid() && !fi.IsDeleted()
		})
	}
}

func readAllStream(t *testing.T, s FloSyncStream) []byte {
	t.Helper()
	var out bytes.Buffer
	for {
		piece, err := s.Next()
		if errors.Is(err, io.EOF) {
			return out.Bytes()
		}
		if err != nil {
			t.Fatalf("stream failed after %d bytes: %v", out.Len(), err)
		}
		out.Write(piece)
	}
}

func openStream(t *testing.T, m *testModel, name string, offset, length int64) FloSyncStream {
	t.Helper()
	s, err := m.OpenFloSyncStream(t.Context(), floSyncFolder, name, offset, length)
	if err != nil {
		t.Fatalf("open stream for %q: %v", name, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// ---------------------------------------------------------------------------
// Stream

func TestFloSyncStreamExactBytes(t *testing.T) {
	m, fc, _ := floSyncModel(t, []string{"/remote*"}, nil)
	defer cleanupModel(m)
	log := &requestLog{}
	fc.serve(log, nil)

	// Three and a bit blocks of 128 KiB.
	data := floSyncData(3*protocol.MinBlockSize+4097, 1)
	fc.addRemote("remote.bin", data)
	fc.sendIndexUpdate()
	waitIgnored(t, m, "remote.bin")

	size := int64(len(data))
	const bs = protocol.MinBlockSize
	cases := []struct{ offset, length int64 }{
		{0, -1},               // everything
		{0, 1},                // first byte
		{1, 10},               // inside the first block
		{bs - 1, 2},           // across the first block boundary
		{bs, bs},              // exactly the second block
		{bs / 2, 2 * bs},      // from the middle of a block over two boundaries
		{3*bs - 100, 200},     // into the last, partial block
		{3 * bs, size - 3*bs}, // exactly the last block
		{size - 1, 1},         // last byte
		{size - 5, -1},        // tail
		{size, 0},             // nothing, at the end
		{size, -1},            // nothing, at the end, to the end
		{7, 0},                // nothing, in the middle
	}
	for _, tc := range cases {
		length := tc.length
		if length < 0 {
			length = size - tc.offset
		}
		if tc.offset+length > size {
			t.Fatalf("bad test case %+v for a file of %d bytes", tc, size)
		}
		s := openStream(t, m, "remote.bin", tc.offset, tc.length)
		if s.Size() != length {
			t.Errorf("offset %d length %d: Size() = %d, want %d", tc.offset, tc.length, s.Size(), length)
		}
		got := readAllStream(t, s)
		want := data[tc.offset : tc.offset+length]
		if !bytes.Equal(got, want) {
			t.Errorf("offset %d length %d: got %d bytes that differ from the %d expected", tc.offset, tc.length, len(got), len(want))
		}
		// After the end every call says so.
		if _, err := s.Next(); !errors.Is(err, io.EOF) {
			t.Errorf("offset %d length %d: Next after the end = %v, want EOF", tc.offset, tc.length, err)
		}
	}

	s := openStream(t, m, "remote.bin", 0, -1)
	if info := s.Info(); info.Name != "remote.bin" || info.Size != size || len(info.Version.Counters) == 0 {
		t.Errorf("unexpected Info(): %v", info)
	}
}

func TestFloSyncStreamWritesNothing(t *testing.T) {
	m, fc, fcfg := floSyncModel(t, []string{"/remote*"}, nil)
	defer cleanupModel(m)
	fc.serve(&requestLog{}, nil)

	data := floSyncData(2*protocol.MinBlockSize+1, 2)
	fc.addRemote("remote.bin", data)
	fc.sendIndexUpdate()
	waitIgnored(t, m, "remote.bin")

	tfs := fcfg.Filesystem()
	namesBefore := mustV(tfs.DirNames("."))
	slices.Sort(namesBefore)
	seqBefore := mustV(m.sdb.GetDeviceSequence(floSyncFolder, protocol.LocalDeviceID))
	localBefore, _ := m.testCurrentFolderFile(floSyncFolder, "remote.bin")
	globalBefore, _, _ := m.CurrentGlobalFile(floSyncFolder, "remote.bin")

	readAllStream(t, openStream(t, m, "remote.bin", 0, -1))

	names := mustV(tfs.DirNames("."))
	slices.Sort(names)
	if !slices.Equal(names, namesBefore) {
		t.Errorf("the folder changed: %v -> %v", namesBefore, names)
	}
	if _, err := tfs.Lstat("remote.bin"); err == nil || !fs.IsNotExist(err) {
		t.Errorf("remote.bin must not exist on disk, got %v", err)
	}
	if seq := mustV(m.sdb.GetDeviceSequence(floSyncFolder, protocol.LocalDeviceID)); seq != seqBefore {
		t.Errorf("local sequence changed %d -> %d", seqBefore, seq)
	}
	if local, _ := m.testCurrentFolderFile(floSyncFolder, "remote.bin"); !local.IsEquivalent(localBefore, 0) || !local.Version.Equal(localBefore.Version) || local.Sequence != localBefore.Sequence {
		t.Errorf("local entry changed: %v -> %v", localBefore, local)
	}
	if global, _, _ := m.CurrentGlobalFile(floSyncFolder, "remote.bin"); !global.Version.Equal(globalBefore.Version) {
		t.Errorf("global entry changed: %v -> %v", globalBefore, global)
	}
}

func TestFloSyncStreamNotInLocalIndex(t *testing.T) {
	// A send only folder never takes the files of other devices, so the file
	// is not in the local index at all.
	m, fc, _ := floSyncModel(t, nil, func(c *config.FolderConfiguration) { c.Type = config.FolderTypeSendOnly })
	defer cleanupModel(m)
	fc.serve(&requestLog{}, nil)

	data := floSyncData(protocol.MinBlockSize+10, 3)
	fc.addRemote("remote.bin", data)
	fc.sendIndexUpdate()
	floSyncEventually(t, "global entry", func() bool {
		_, ok, _ := m.CurrentGlobalFile(floSyncFolder, "remote.bin")
		return ok
	})
	if _, ok := m.testCurrentFolderFile(floSyncFolder, "remote.bin"); ok {
		t.Fatal("expected no local entry")
	}

	if got := readAllStream(t, openStream(t, m, "remote.bin", 0, -1)); !bytes.Equal(got, data) {
		t.Errorf("got %d bytes that differ from the %d expected", len(got), len(data))
	}
}

func TestFloSyncStreamErrors(t *testing.T) {
	m, fc, _ := floSyncModel(t, []string{"/remote*"}, nil)
	defer cleanupModel(m)
	log := &requestLog{}
	fc.serve(log, nil)

	data := floSyncData(2*protocol.MinBlockSize, 4)
	fc.addRemote("remote.bin", data)
	fc.addFile("remote-dir", 0o755, protocol.FileInfoTypeDirectory, nil)
	fc.addRemote("remote-gone.bin", data)
	fc.addRemote("held.bin", data)
	fc.sendIndexUpdate()
	waitIgnored(t, m, "remote.bin", "remote-dir", "remote-gone.bin")
	waitHeld(t, m, "held.bin")
	fc.deleteFile("remote-gone.bin")
	fc.sendIndexUpdate()
	floSyncEventually(t, "remote-gone.bin deleted globally", func() bool {
		gf, ok, _ := m.CurrentGlobalFile(floSyncFolder, "remote-gone.bin")
		return ok && gf.IsDeleted()
	})
	requestsBefore := log.count()

	open := func(folder, name string, offset, length int64) error {
		s, err := m.OpenFloSyncStream(t.Context(), folder, name, offset, length)
		if err == nil {
			s.Close()
		}
		return err
	}

	size := int64(len(data))
	for _, tc := range []struct {
		what   string
		folder string
		name   string
		offset int64
		length int64
		want   error
	}{
		{"not in the global index", floSyncFolder, "remote-missing.bin", 0, -1, ErrFloSyncNotFound},
		{"directory", floSyncFolder, "remote-dir", 0, -1, ErrFloSyncNotFound},
		{"deleted", floSyncFolder, "remote-gone.bin", 0, -1, ErrFloSyncNotFound},
		{"held as a normal file", floSyncFolder, "held.bin", 0, -1, ErrFloSyncHeld},
		{"no such folder", "nosuchfolder", "remote.bin", 0, -1, ErrFolderMissing},
		{"empty name", floSyncFolder, "", 0, -1, ErrFloSyncBadRequest},
		{"parent directory", floSyncFolder, "../remote.bin", 0, -1, ErrFloSyncBadRequest},
		{"dots inside", floSyncFolder, "a/../remote.bin", 0, -1, ErrFloSyncBadRequest},
		{"absolute", floSyncFolder, "/remote.bin", 0, -1, ErrFloSyncBadRequest},
		{"internal", floSyncFolder, ".stfolder", 0, -1, ErrFloSyncBadRequest},
		{"internal below", floSyncFolder, ".stversions/x", 0, -1, ErrFloSyncBadRequest},
		{"negative offset", floSyncFolder, "remote.bin", -1, -1, ErrFloSyncBadRequest},
		{"offset after the end", floSyncFolder, "remote.bin", size + 1, -1, ErrFloSyncBadRange},
		{"length after the end", floSyncFolder, "remote.bin", 10, size, ErrFloSyncBadRange},
		{"length from the end", floSyncFolder, "remote.bin", size, 1, ErrFloSyncBadRange},
	} {
		if err := open(tc.folder, tc.name, tc.offset, tc.length); !errors.Is(err, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.what, err, tc.want)
		}
	}
	if n := log.count(); n != requestsBefore {
		t.Errorf("a rejected request still asked other devices for data (%d requests)", n-requestsBefore)
	}

	// When no connected device has the version.
	fc.Close(errors.New("test"))
	floSyncEventually(t, "device gone", func() bool { return !m.ConnectedTo(device1) })
	if err := open(floSyncFolder, "remote.bin", 0, -1); !errors.Is(err, ErrFloSyncNoSource) {
		t.Errorf("no device: got %v, want %v", err, ErrFloSyncNoSource)
	}
}

func TestFloSyncStreamPausedFolder(t *testing.T) {
	w, fcfg := newDefaultCfgWrapper(t)
	m := setupModel(t, w)
	defer cleanupModel(m)
	pauseFolder(t, w, fcfg.ID, true)

	if _, err := m.OpenFloSyncStream(t.Context(), fcfg.ID, "x", 0, -1); !errors.Is(err, ErrFolderPaused) {
		t.Errorf("got %v, want %v", err, ErrFolderPaused)
	}
	if _, err := m.FloSyncDelete(t.Context(), fcfg.ID, "x"); !errors.Is(err, ErrFolderPaused) {
		t.Errorf("got %v, want %v", err, ErrFolderPaused)
	}
}

func TestFloSyncStreamEmptyAndZeroBlocks(t *testing.T) {
	m, fc, _ := floSyncModel(t, []string{"/remote*"}, nil)
	defer cleanupModel(m)
	log := &requestLog{}
	fc.serve(log, nil)

	// A file of zero length has no blocks, a block of zeroes is never
	// requested (the puller does not either).
	zeroThenData := append(make([]byte, protocol.MinBlockSize), floSyncData(100, 5)...)
	fc.addRemote("remote-empty.bin", nil)
	fc.addRemote("remote-zero.bin", zeroThenData)
	fc.sendIndexUpdate()
	waitIgnored(t, m, "remote-empty.bin", "remote-zero.bin")

	s := openStream(t, m, "remote-empty.bin", 0, -1)
	if s.Size() != 0 {
		t.Errorf("empty file, Size() = %d", s.Size())
	}
	if got := readAllStream(t, s); len(got) != 0 {
		t.Errorf("empty file gave %d bytes", len(got))
	}
	if n := log.count(); n != 0 {
		t.Errorf("empty file made %d requests", n)
	}

	got := readAllStream(t, openStream(t, m, "remote-zero.bin", 0, -1))
	if !bytes.Equal(got, zeroThenData) {
		t.Errorf("zero block file: got %d bytes that differ from the %d expected", len(got), len(zeroThenData))
	}
	log.mut.Lock()
	defer log.mut.Unlock()
	if len(log.requests) != 1 || log.requests[0].Offset != protocol.MinBlockSize {
		t.Errorf("expected one request for the second block only, got %v", log.requests)
	}
}

func TestFloSyncStreamOrderAndWindow(t *testing.T) {
	m, fc, _ := floSyncModel(t, []string{"/remote*"}, nil)
	defer cleanupModel(m)
	log := &requestLog{}
	// The first blocks of a window answer slowest, so that the blocks come
	// back out of order.
	fc.serve(log, func(req *protocol.Request) time.Duration {
		return time.Duration(10-int(req.Offset/protocol.MinBlockSize)%10) * 3 * time.Millisecond
	})

	data := floSyncData(40*protocol.MinBlockSize+17, 6)
	fc.addRemote("remote.bin", data)
	fc.sendIndexUpdate()
	waitIgnored(t, m, "remote.bin")

	if got := readAllStream(t, openStream(t, m, "remote.bin", 0, -1)); !bytes.Equal(got, data) {
		t.Fatalf("got %d bytes that differ from the %d expected", len(got), len(data))
	}
	log.mut.Lock()
	defer log.mut.Unlock()
	if log.maxInFlight < 2 {
		t.Errorf("no blocks were fetched ahead (at most %d in flight)", log.maxInFlight)
	}
	if log.maxInFlight > floSyncWindowBlocks+1 {
		t.Errorf("more blocks in flight than the window allows: %d", log.maxInFlight)
	}
	if len(log.requests) != 41 {
		t.Errorf("expected one request per block (41), got %d", len(log.requests))
	}
}

// twoDevices sets up the folder with two connected devices that hold the same
// version of "remote.bin", which this device holds only as an ignored entry.
func twoDevices(t *testing.T, data []byte) (m *testModel, fc1, fc2 *fakeConnection, version protocol.Vector) {
	t.Helper()
	w, fcfg := newDefaultCfgWrapper(t)
	addDevice2(t, w, fcfg)
	fcfg, _ = w.Folder(fcfg.ID) // with the second device
	m = setupModel(t, w)
	folderIgnoresAlwaysReload(t, m, fcfg)
	must(t, m.SetIgnores(floSyncFolder, []string{"/remote*"}))
	must(t, m.ScanFolder(floSyncFolder))

	fc1 = addFakeConn(m, device1, fcfg.ID)
	fc1.folder = floSyncFolder
	fc2 = addFakeConn(m, device2, fcfg.ID)
	fc2.folder = floSyncFolder

	version = protocol.Vector{}.Update(device1.Short())
	for _, fc := range []*fakeConnection{fc1, fc2} {
		fc.mut.Lock()
		fc.addFileLocked("remote.bin", 0o644, protocol.FileInfoTypeFile, data, version, 0)
		fc.mut.Unlock()
		fc.serve(&requestLog{}, nil)
	}
	fc1.sendIndexUpdate()
	fc2.sendIndexUpdate()
	waitIgnored(t, m, "remote.bin")
	floSyncEventually(t, "two devices have the file", func() bool {
		av, _ := m.Availability(floSyncFolder, protocol.FileInfo{Name: "remote.bin", Version: version}, protocol.BlockInfo{})
		return len(av) == 2
	})
	return m, fc1, fc2, version
}

// A device that sends data that does not match the hash is skipped.
func TestFloSyncStreamBadDataFailsOver(t *testing.T) {
	data := floSyncData(3*protocol.MinBlockSize+5, 7)
	m, fc1, fc2, _ := twoDevices(t, data)
	defer cleanupModel(m)

	corrupt := func(fc *fakeConnection, log *requestLog) {
		fc.RequestCalls(func(_ context.Context, req *protocol.Request) ([]byte, error) {
			log.mut.Lock()
			log.requests = append(log.requests, *req)
			log.mut.Unlock()
			return floSyncData(req.Size, 99), nil // right length, wrong content
		})
	}

	// One device is bad and asked first, the other is good.
	for _, bad := range []int{1, 2} {
		good, badFc := fc2, fc1
		if bad == 2 {
			good, badFc = fc1, fc2
		}
		goodLog, badLog := &requestLog{}, &requestLog{}
		good.serve(goodLog, nil)
		corrupt(badFc, badLog)

		a := Availability{ID: good.id}
		activity.using(a)
		got := readAllStream(t, openStream(t, m, "remote.bin", 0, -1))
		activity.done(a)

		if !bytes.Equal(got, data) {
			t.Errorf("bad device %d: got %d bytes that differ from the %d expected", bad, len(got), len(data))
		}
		if badLog.count() == 0 {
			t.Errorf("bad device %d was never asked, the test did not test anything", bad)
		}
		if goodLog.count() == 0 {
			t.Errorf("bad device %d: the good device was never asked", bad)
		}
	}

	// Both devices bad: the stream fails, with the reason, and stays failed.
	corrupt(fc1, &requestLog{})
	corrupt(fc2, &requestLog{})
	s := openStream(t, m, "remote.bin", 0, -1)
	_, err := s.Next()
	if !errors.Is(err, ErrFloSyncNoSource) {
		t.Fatalf("both devices bad: got %v, want %v", err, ErrFloSyncNoSource)
	}
	if _, err2 := s.Next(); err2 == nil || err2.Error() != err.Error() {
		t.Errorf("a failed stream must keep failing with the same error, got %v", err2)
	}
}

// A device that has the version only as an ignored entry (a placeholder, in
// FloSync) cannot serve it. The index still counts it as having the file, so
// it is asked last.
func TestFloSyncStreamAsksInvalidHoldersLast(t *testing.T) {
	data := floSyncData(5*protocol.MinBlockSize+3, 31)
	m, fc1, fc2, version := twoDevices(t, data)
	defer cleanupModel(m)

	fc2.mut.Lock()
	fc2.files = nil
	fc2.addFileLocked("remote.bin", 0o644, protocol.FileInfoTypeFile, data, version, protocol.FlagLocalIgnored)
	fc2.mut.Unlock()
	fc2.sendIndexUpdate()
	floSyncEventually(t, "the entry of device2 is invalid", func() bool {
		fi, ok, _ := m.sdb.GetDeviceFile(floSyncFolder, device2, "remote.bin")
		return ok && fi.IsInvalid()
	})

	log1, log2 := &requestLog{}, &requestLog{}
	fc1.serve(log1, nil)
	fc2.RequestCalls(func(_ context.Context, req *protocol.Request) ([]byte, error) {
		log2.mut.Lock()
		log2.requests = append(log2.requests, *req)
		log2.mut.Unlock()
		return nil, protocol.ErrInvalid // what a device that ignores the file answers
	})

	// Make device1 look busy, so that device2 would be first in line if the
	// stream did not know better.
	busy := Availability{ID: device1}
	activity.using(busy)
	got := readAllStream(t, openStream(t, m, "remote.bin", 0, -1))
	activity.done(busy)
	if !bytes.Equal(got, data) {
		t.Errorf("got %d bytes that differ from the %d expected", len(got), len(data))
	}
	if n := log2.count(); n != 0 {
		t.Errorf("the device that cannot serve the file was asked %d times", n)
	}
	if n := log1.count(); n != 6 {
		t.Errorf("the device that has the file was asked %d times, want 6 (one per block)", n)
	}

	// When it is the only one left it is tried, and the stream fails.
	fc1.Close(errors.New("test"))
	floSyncEventually(t, "device1 gone", func() bool { return !m.ConnectedTo(device1) })
	s := openStream(t, m, "remote.bin", 0, -1)
	if _, err := s.Next(); !errors.Is(err, ErrFloSyncNoSource) {
		t.Errorf("got %v, want %v", err, ErrFloSyncNoSource)
	}
	if log2.count() == 0 {
		t.Error("the last device was never tried")
	}
}

// A device that never answers is given up on after the block timeout.
func TestFloSyncStreamBlockTimeout(t *testing.T) {
	old := floSyncBlockTimeout
	floSyncBlockTimeout = 300 * time.Millisecond
	t.Cleanup(func() { floSyncBlockTimeout = old })

	data := floSyncData(2*protocol.MinBlockSize, 8)
	m, fc1, fc2, _ := twoDevices(t, data)
	defer cleanupModel(m)

	// Whichever of the two never answers, and is asked first, the stream
	// completes with the other device.
	for _, hanging := range []*fakeConnection{fc1, fc2} {
		good := fc2
		if hanging == fc2 {
			good = fc1
		}
		var hung atomic.Int32
		good.serve(&requestLog{}, nil)
		hanging.RequestCalls(func(ctx context.Context, _ *protocol.Request) ([]byte, error) {
			hung.Add(1)
			<-ctx.Done()
			return nil, ctx.Err()
		})

		a := Availability{ID: good.id}
		activity.using(a)
		start := time.Now()
		got := readAllStream(t, openStream(t, m, "remote.bin", 0, -1))
		took := time.Since(start)
		activity.done(a)

		if !bytes.Equal(got, data) {
			t.Errorf("got %d bytes that differ from the %d expected", len(got), len(data))
		}
		if hung.Load() == 0 {
			t.Error("the hanging device was never asked, the test did not test anything")
		}
		if took < floSyncBlockTimeout {
			t.Errorf("finished in %v, before the block timeout, the hanging device cannot have been waited for", took)
		}
		if took > 10*time.Second {
			t.Errorf("took %v", took)
		}
	}
}

// Cancelling the context ends the stream and the requests in flight.
func TestFloSyncStreamCancel(t *testing.T) {
	m, fc, _ := floSyncModel(t, []string{"/remote*"}, nil)
	defer cleanupModel(m)

	started := make(chan struct{}, 64)
	ended := make(chan error, 64)
	fc.RequestCalls(func(ctx context.Context, req *protocol.Request) ([]byte, error) {
		fc.mut.Lock()
		data := fc.fileData[filepath.ToSlash(req.Name)]
		fc.mut.Unlock()
		if req.Offset >= protocol.MinBlockSize {
			// Everything after the first block waits to be cancelled.
			started <- struct{}{}
			<-ctx.Done()
			ended <- ctx.Err()
			return nil, ctx.Err()
		}
		return slices.Clone(data[req.Offset : int(req.Offset)+req.Size]), nil
	})

	data := floSyncData(5*protocol.MinBlockSize, 9)
	fc.addRemote("remote.bin", data)
	fc.sendIndexUpdate()
	waitIgnored(t, m, "remote.bin")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s, err := m.OpenFloSyncStream(ctx, floSyncFolder, "remote.bin", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if piece, err := s.Next(); err != nil || !bytes.Equal(piece, data[:protocol.MinBlockSize]) {
		t.Fatalf("first piece: %d bytes, %v", len(piece), err)
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the later blocks were not requested")
	}

	cancel()
	if _, err := s.Next(); !errors.Is(err, context.Canceled) {
		t.Errorf("Next after cancel = %v, want context canceled", err)
	}
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return, requests are still running")
	}
	select {
	case err := <-ended:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("the request in flight saw %v, want context canceled", err)
		}
	default:
		t.Error("no request in flight saw the cancel")
	}
}

// ---------------------------------------------------------------------------
// Delete

// floSyncTree is a tree of directories and files that fc holds.
func floSyncTree(fc *fakeConnection) {
	for _, d := range []string{"remote", "remote/sub", "remote/sub/deep"} {
		fc.addFile(d, 0o755, protocol.FileInfoTypeDirectory, nil)
	}
	fc.addRemote("remote/a.txt", floSyncData(100, 10))
	fc.addRemote("remote/sub/b.txt", floSyncData(200, 11))
	fc.addRemote("remote/sub/deep/c.txt", floSyncData(300, 12))
}

var floSyncTreeNames = []string{"remote", "remote/sub", "remote/sub/deep", "remote/a.txt", "remote/sub/b.txt", "remote/sub/deep/c.txt"}

func TestFloSyncDeleteFile(t *testing.T) {
	m, fc, fcfg := floSyncModel(t, []string{"/remote*"}, nil)
	defer cleanupModel(m)
	fc.addRemote("remote.txt", floSyncData(1000, 13))
	fc.addRemote("remote-other.txt", floSyncData(1000, 14))
	fc.sendIndexUpdate()
	waitIgnored(t, m, "remote.txt", "remote-other.txt")

	// A placeholder like Flogent's: on disk, but ignored, and it must never
	// be touched.
	tfs := fcfg.Filesystem()
	writeFile(t, tfs, "remote.txt", []byte("placeholder"))
	globalBefore, _, _ := m.CurrentGlobalFile(floSyncFolder, "remote.txt")
	otherBefore, _ := m.testCurrentFolderFile(floSyncFolder, "remote-other.txt")
	idx := collectIndex(fc)

	res, err := m.FloSyncDelete(t.Context(), floSyncFolder, "remote.txt")
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 1 || res.Held != 0 {
		t.Errorf("result %+v, want 1 deleted and none held", res)
	}

	local, ok := m.testCurrentFolderFile(floSyncFolder, "remote.txt")
	if !ok || !local.IsDeleted() || local.IsInvalid() || local.ModifiedBy != myID.Short() || len(local.Blocks) != 0 || local.Size != 0 {
		t.Errorf("local entry is not a plain delete by this device: %v", local)
	}
	if local.Version.Compare(globalBefore.Version) != protocol.Greater {
		t.Errorf("the version %v is not newer than the global version before %v", local.Version, globalBefore.Version)
	}
	if gf, ok, _ := m.CurrentGlobalFile(floSyncFolder, "remote.txt"); !ok || !gf.IsDeleted() || gf.ModifiedBy != myID.Short() {
		t.Errorf("global entry is not the delete: %v", gf)
	}
	if other, _ := m.testCurrentFolderFile(floSyncFolder, "remote-other.txt"); !other.Version.Equal(otherBefore.Version) || other.IsDeleted() {
		t.Errorf("another file changed: %v", other)
	}

	// The peer is sent it.
	floSyncEventually(t, "delete sent to the peer", func() bool {
		_, ok := idx.lastDelete("remote.txt")
		return ok
	})
	sent, _ := idx.lastDelete("remote.txt")
	if sent.ModifiedBy != myID.Short() || sent.Version.Compare(globalBefore.Version) != protocol.Greater || len(sent.Blocks) != 0 {
		t.Errorf("the peer was sent %v", sent)
	}
	if _, ok := idx.lastDelete("remote-other.txt"); ok {
		t.Error("the peer was sent a delete for another file too")
	}

	// Nothing on disk changed.
	if err := equalContents(tfs, "remote.txt", []byte("placeholder")); err != nil {
		t.Errorf("the file on disk changed: %v", err)
	}

	// Asked again it is already deleted, and the file stays as it is.
	if _, err := m.FloSyncDelete(t.Context(), floSyncFolder, "remote.txt"); !errors.Is(err, ErrFloSyncNotFound) {
		t.Errorf("second delete: got %v, want %v", err, ErrFloSyncNotFound)
	}
	if again, _ := m.testCurrentFolderFile(floSyncFolder, "remote.txt"); !again.Version.Equal(local.Version) {
		t.Errorf("the second delete changed the version %v -> %v", local.Version, again.Version)
	}
}

// The delete must survive the scans that follow: the name is still ignored.
func TestFloSyncDeleteSurvivesScan(t *testing.T) {
	m, fc, _ := floSyncModel(t, []string{"/remote*"}, nil)
	defer cleanupModel(m)
	fc.addRemote("remote.txt", floSyncData(1000, 15))
	fc.sendIndexUpdate()
	waitIgnored(t, m, "remote.txt")

	idx := collectIndex(fc)
	if _, err := m.FloSyncDelete(t.Context(), floSyncFolder, "remote.txt"); err != nil {
		t.Fatal(err)
	}
	before, _ := m.testCurrentFolderFile(floSyncFolder, "remote.txt")

	must(t, m.ScanFolder(floSyncFolder))
	must(t, m.ScanFolder(floSyncFolder))

	after, _ := m.testCurrentFolderFile(floSyncFolder, "remote.txt")
	if after.IsInvalid() || after.IsIgnored() || !after.IsDeleted() || !after.Version.Equal(before.Version) {
		t.Errorf("a scan changed the delete: %v -> %v", before, after)
	}
	if gf, _, _ := m.CurrentGlobalFile(floSyncFolder, "remote.txt"); !gf.IsDeleted() || gf.IsInvalid() {
		t.Errorf("the global entry is no longer the delete: %v", gf)
	}
	floSyncEventually(t, "delete sent to the peer", func() bool {
		_, ok := idx.lastDelete("remote.txt")
		return ok
	})
	time.Sleep(600 * time.Millisecond) // room for an unwanted second update
	if bad := idx.invalidDeletes("remote.txt"); len(bad) != 0 {
		t.Errorf("the peer was sent the delete as an invalid (ignored) record: %v", bad)
	}
}

func TestFloSyncDeleteNotInLocalIndex(t *testing.T) {
	m, fc, _ := floSyncModel(t, nil, func(c *config.FolderConfiguration) { c.Type = config.FolderTypeSendOnly })
	defer cleanupModel(m)
	fc.addRemote("remote.txt", floSyncData(1000, 16))
	fc.sendIndexUpdate()
	floSyncEventually(t, "global entry", func() bool {
		_, ok, _ := m.CurrentGlobalFile(floSyncFolder, "remote.txt")
		return ok
	})
	if _, ok := m.testCurrentFolderFile(floSyncFolder, "remote.txt"); ok {
		t.Fatal("expected no local entry")
	}
	globalBefore, _, _ := m.CurrentGlobalFile(floSyncFolder, "remote.txt")
	idx := collectIndex(fc)

	res, err := m.FloSyncDelete(t.Context(), floSyncFolder, "remote.txt")
	if err != nil || res.Deleted != 1 {
		t.Fatalf("got %+v, %v", res, err)
	}
	local, ok := m.testCurrentFolderFile(floSyncFolder, "remote.txt")
	if !ok || !local.IsDeleted() || local.IsInvalid() || local.Version.Compare(globalBefore.Version) != protocol.Greater {
		t.Errorf("local entry is not a newer plain delete: %v", local)
	}
	floSyncEventually(t, "delete sent to the peer", func() bool {
		_, ok := idx.lastDelete("remote.txt")
		return ok
	})
}

func TestFloSyncDeleteTree(t *testing.T) {
	m, fc, fcfg := floSyncModel(t, []string{"/remote"}, nil)
	defer cleanupModel(m)
	floSyncTree(fc)
	fc.addRemote("keep.txt", floSyncData(10, 17)) // not under the tree
	fc.sendIndexUpdate()
	waitIgnored(t, m, floSyncTreeNames...)
	waitHeld(t, m, "keep.txt")

	tfs := fcfg.Filesystem()
	must(t, tfs.MkdirAll("remote/sub", 0o755))
	writeFile(t, tfs, "remote/sub/b.txt", []byte("placeholder"))

	before := map[string]protocol.FileInfo{}
	for _, n := range floSyncTreeNames {
		before[n], _, _ = m.CurrentGlobalFile(floSyncFolder, n)
	}
	keepBefore, _ := m.testCurrentFolderFile(floSyncFolder, "keep.txt")
	idx := collectIndex(fc)

	res, err := m.FloSyncDelete(t.Context(), floSyncFolder, "remote")
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 6 || res.Held != 0 {
		t.Errorf("result %+v, want 6 deleted and none held", res)
	}
	for _, n := range floSyncTreeNames {
		local, ok := m.testCurrentFolderFile(floSyncFolder, n)
		if !ok || !local.IsDeleted() || local.IsInvalid() || local.ModifiedBy != myID.Short() || local.Version.Compare(before[n].Version) != protocol.Greater {
			t.Errorf("%s: not a newer plain delete by this device: %v", n, local)
		}
	}
	if keep, _ := m.testCurrentFolderFile(floSyncFolder, "keep.txt"); keep.IsDeleted() || !keep.Version.Equal(keepBefore.Version) {
		t.Errorf("a file outside the tree changed: %v", keep)
	}

	// Files first, then directories deepest first, the directory last.
	floSyncEventually(t, "whole tree sent to the peer", func() bool {
		return len(idx.deleteOrder()) >= len(floSyncTreeNames)
	})
	want := []string{"remote/a.txt", "remote/sub/b.txt", "remote/sub/deep/c.txt", "remote/sub/deep", "remote/sub", "remote"}
	if order := idx.deleteOrder(); !slices.Equal(order, want) {
		t.Errorf("sent in the order %v, want %v", order, want)
	}

	// Nothing on disk changed.
	if err := equalContents(tfs, "remote/sub/b.txt", []byte("placeholder")); err != nil {
		t.Errorf("a file on disk changed: %v", err)
	}

	// The directory is gone for good now.
	if _, err := m.FloSyncDelete(t.Context(), floSyncFolder, "remote"); !errors.Is(err, ErrFloSyncNotFound) {
		t.Errorf("second delete: got %v, want %v", err, ErrFloSyncNotFound)
	}
}

func TestFloSyncDeleteSubTreeAndLateDirectory(t *testing.T) {
	m, fc, _ := floSyncModel(t, []string{"/remote"}, nil)
	defer cleanupModel(m)
	floSyncTree(fc)
	fc.sendIndexUpdate()
	waitIgnored(t, m, floSyncTreeNames...)

	res, err := m.FloSyncDelete(t.Context(), floSyncFolder, "remote/sub/")
	if err != nil || res.Deleted != 4 {
		t.Fatalf("sub tree: got %+v, %v, want 4 deleted", res, err)
	}
	for _, n := range []string{"remote/sub", "remote/sub/deep", "remote/sub/b.txt", "remote/sub/deep/c.txt"} {
		if gf, _, _ := m.CurrentGlobalFile(floSyncFolder, n); !gf.IsDeleted() {
			t.Errorf("%s is not deleted", n)
		}
	}
	for _, n := range []string{"remote", "remote/a.txt"} {
		if gf, _, _ := m.CurrentGlobalFile(floSyncFolder, n); gf.IsDeleted() {
			t.Errorf("%s was deleted", n)
		}
	}

	// A directory that the normal scan has already announced as deleted
	// still gets the entries below it announced.
	fc.deleteFile("remote")
	fc.sendIndexUpdate()
	floSyncEventually(t, "directory deleted globally", func() bool {
		gf, ok, _ := m.CurrentGlobalFile(floSyncFolder, "remote")
		return ok && gf.IsDeleted()
	})
	res, err = m.FloSyncDelete(t.Context(), floSyncFolder, "remote")
	if err != nil || res.Deleted != 1 {
		t.Fatalf("late directory: got %+v, %v, want 1 deleted (remote/a.txt)", res, err)
	}
	if gf, _, _ := m.CurrentGlobalFile(floSyncFolder, "remote/a.txt"); !gf.IsDeleted() || gf.ModifiedBy != myID.Short() {
		t.Errorf("remote/a.txt is not deleted by this device: %v", gf)
	}
}

func TestFloSyncDeleteHeld(t *testing.T) {
	m, fc, _ := floSyncModel(t, []string{"/remote*"}, nil)
	defer cleanupModel(m)
	data := floSyncData(1000, 18)
	fc.addRemote("held.txt", data)
	fc.sendIndexUpdate()
	waitHeld(t, m, "held.txt")
	before, _ := m.testCurrentFolderFile(floSyncFolder, "held.txt")
	seqBefore := mustV(m.sdb.GetDeviceSequence(floSyncFolder, protocol.LocalDeviceID))

	if _, err := m.FloSyncDelete(t.Context(), floSyncFolder, "held.txt"); !errors.Is(err, ErrFloSyncHeld) {
		t.Fatalf("got %v, want %v", err, ErrFloSyncHeld)
	}
	after, _ := m.testCurrentFolderFile(floSyncFolder, "held.txt")
	if after.IsDeleted() || !after.Version.Equal(before.Version) {
		t.Errorf("the held file changed: %v -> %v", before, after)
	}
	if seq := mustV(m.sdb.GetDeviceSequence(floSyncFolder, protocol.LocalDeviceID)); seq != seqBefore {
		t.Errorf("the local index changed (sequence %d -> %d)", seqBefore, seq)
	}
}

func TestFloSyncDeleteTreeWithHeldEntries(t *testing.T) {
	// Only the files ending in .dat are ignored here. The directories and
	// the other files are held as normal entries.
	m, fc, _ := floSyncModel(t, []string{"*.dat"}, nil)
	defer cleanupModel(m)
	fc.addFile("tree", 0o755, protocol.FileInfoTypeDirectory, nil)
	fc.addFile("tree/sub", 0o755, protocol.FileInfoTypeDirectory, nil)
	fc.addRemote("tree/held.txt", floSyncData(10, 19))
	fc.addRemote("tree/x.dat", floSyncData(10, 20))
	fc.addRemote("tree/sub/y.dat", floSyncData(10, 21))
	fc.sendIndexUpdate()
	waitIgnored(t, m, "tree/x.dat", "tree/sub/y.dat")
	waitHeld(t, m, "tree", "tree/sub", "tree/held.txt")

	res, err := m.FloSyncDelete(t.Context(), floSyncFolder, "tree")
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 2 || res.Held != 3 {
		t.Errorf("result %+v, want 2 deleted (the .dat files) and 3 held", res)
	}
	for _, n := range []string{"tree/x.dat", "tree/sub/y.dat"} {
		if local, _ := m.testCurrentFolderFile(floSyncFolder, n); !local.IsDeleted() || local.IsInvalid() {
			t.Errorf("%s: not announced as deleted: %v", n, local)
		}
	}
	for _, n := range []string{"tree", "tree/sub", "tree/held.txt"} {
		if local, _ := m.testCurrentFolderFile(floSyncFolder, n); local.IsDeleted() || local.IsInvalid() {
			t.Errorf("%s: a held entry was touched: %v", n, local)
		}
	}

	// A directory with nothing left to announce.
	if _, err := m.FloSyncDelete(t.Context(), floSyncFolder, "tree"); !errors.Is(err, ErrFloSyncHeld) {
		t.Errorf("everything held: got %v, want %v", err, ErrFloSyncHeld)
	}
}

func TestFloSyncDeleteErrors(t *testing.T) {
	m, fc, fcfg := floSyncModel(t, []string{"/remote*"}, nil)
	defer cleanupModel(m)
	fc.addRemote("remote.txt", floSyncData(10, 22))
	fc.sendIndexUpdate()
	waitIgnored(t, m, "remote.txt")
	seqBefore := mustV(m.sdb.GetDeviceSequence(floSyncFolder, protocol.LocalDeviceID))

	for _, tc := range []struct {
		folder, name string
		want         error
	}{
		{floSyncFolder, "remote-missing.txt", ErrFloSyncNotFound},
		{floSyncFolder, "", ErrFloSyncBadRequest},
		{floSyncFolder, "../remote.txt", ErrFloSyncBadRequest},
		{floSyncFolder, "/remote.txt", ErrFloSyncBadRequest},
		{floSyncFolder, ".stfolder", ErrFloSyncBadRequest},
		{"nosuchfolder", "remote.txt", ErrFolderMissing},
	} {
		if _, err := m.FloSyncDelete(t.Context(), tc.folder, tc.name); !errors.Is(err, tc.want) {
			t.Errorf("%q in %q: got %v, want %v", tc.name, tc.folder, err, tc.want)
		}
	}

	// A folder that is not healthy never announces a delete: here the
	// folder marker is gone, like a drive that is not mounted.
	must(t, fcfg.Filesystem().RemoveAll(config.DefaultMarkerName))
	if _, err := m.FloSyncDelete(t.Context(), floSyncFolder, "remote.txt"); !errors.Is(err, ErrFloSyncUnavailable) {
		t.Errorf("unhealthy folder: got %v, want %v", err, ErrFloSyncUnavailable)
	}
	if seq := mustV(m.sdb.GetDeviceSequence(floSyncFolder, protocol.LocalDeviceID)); seq != seqBefore {
		t.Errorf("the local index changed (sequence %d -> %d)", seqBefore, seq)
	}
	if local, _ := m.testCurrentFolderFile(floSyncFolder, "remote.txt"); local.IsDeleted() {
		t.Errorf("the entry was deleted in an unhealthy folder: %v", local)
	}
}

func TestFloSyncDeleteReceiveOnly(t *testing.T) {
	m, _, _ := floSyncModel(t, nil, func(c *config.FolderConfiguration) { c.Type = config.FolderTypeReceiveOnly })
	defer cleanupModel(m)
	if _, err := m.FloSyncDelete(t.Context(), floSyncFolder, "x"); !errors.Is(err, ErrFloSyncBadRequest) {
		t.Errorf("got %v, want %v", err, ErrFloSyncBadRequest)
	}
}

func TestFloSyncCleanName(t *testing.T) {
	good := map[string]string{
		"a":                           "a",
		"a/b":                         "a/b",
		"a/b/":                        "a/b",
		"dir with space/file (1).txt": "dir with space/file (1).txt",
		"..hidden":                    "..hidden",
		"a..b/c":                      "a..b/c",
	}
	for in, want := range good {
		if got, err := floSyncCleanName(in); err != nil || got != want {
			t.Errorf("%q: got %q, %v, want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"", "/", "//", ".", "..", "../a", "a/..", "a/../b", "a/./b", "a//b", "/a", ".stfolder", ".stignore", ".stversions/a", "a\x00b"} {
		if got, err := floSyncCleanName(in); !errors.Is(err, ErrFloSyncBadRequest) {
			t.Errorf("%q: got %q, %v, want a bad request", in, got, err)
		}
	}
}
