// Copyright (C) 2026 Carel Kuun (Flogent FloSync fork of Syncthing).
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

// The FloSync endpoints. Like every /rest endpoint they need the API key (or
// a logged in session with a CSRF token).
//
//	GET  /rest/flosync/stream?folder=<id>&file=<path>[&offset=<bytes>][&length=<bytes>]
//	POST /rest/flosync/delete?folder=<id>&file=<path>

package api

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/model"
	"github.com/syncthing/syncthing/lib/protocol"
)

// floSyncStatus maps an error from the model to an HTTP status.
func floSyncStatus(err error) int {
	switch {
	case errors.Is(err, model.ErrFloSyncBadRequest):
		return http.StatusBadRequest
	case errors.Is(err, model.ErrFolderMissing), errors.Is(err, model.ErrFloSyncNotFound):
		return http.StatusNotFound
	case errors.Is(err, model.ErrFloSyncHeld):
		return http.StatusConflict
	case errors.Is(err, model.ErrFloSyncBadRange):
		return http.StatusRequestedRangeNotSatisfiable
	case errors.Is(err, model.ErrFloSyncNoSource),
		errors.Is(err, model.ErrFloSyncUnavailable),
		errors.Is(err, model.ErrFolderPaused),
		errors.Is(err, model.ErrFolderNotRunning):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func floSyncError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), floSyncStatus(err))
}

// parseFloSyncCount parses an optional non negative number from the query.
func parseFloSyncCount(value string, def int64) (int64, error) {
	if value == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q is not a number of bytes", value)
	}
	return n, nil
}

// getFloSyncStream streams the bytes of the current global version of a file
// that this device does not hold. It answers 400 for bad parameters, 404 when
// the file is not in the global index (or is deleted or not a regular file),
// 409 when this device holds the file as a normal file, 416 for a bad range
// and 503 when no connected device has the version. A failure after the
// first bytes were sent ends the response early, so that the reader sees a
// short body.
func (s *service) getFloSyncStream(w http.ResponseWriter, r *http.Request) {
	qs := r.URL.Query()
	folder := qs.Get("folder")
	file := qs.Get("file")
	if folder == "" || file == "" {
		http.Error(w, "folder and file are required", http.StatusBadRequest)
		return
	}
	offset, err := parseFloSyncCount(qs.Get("offset"), 0)
	if err != nil {
		http.Error(w, "bad offset: "+err.Error(), http.StatusBadRequest)
		return
	}
	length, err := parseFloSyncCount(qs.Get("length"), -1)
	if err != nil {
		http.Error(w, "bad length: "+err.Error(), http.StatusBadRequest)
		return
	}

	// The request context is cancelled when the client goes away, which
	// cancels the stream. (The server's ReadTimeout does not apply any more
	// once the request has been read, so a long stream is not cut by it.)
	rc := http.NewResponseController(w)
	ctx := r.Context()
	stream, err := s.model.OpenFloSyncStream(ctx, folder, file, offset, length)
	if err != nil {
		floSyncError(w, err)
		return
	}
	defer stream.Close()

	// Get the first piece before the status line is sent, so that a block no
	// device can supply still gets a proper error status.
	piece, err := stream.Next()
	if err != nil && !errors.Is(err, io.EOF) {
		if ctx.Err() == nil {
			slog.Warn("FloSync stream failed before the first byte", slog.String("folder", folder), slogutil.FilePath(file), slogutil.Error(err))
		}
		floSyncError(w, err)
		return
	}

	info := stream.Info()
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", strconv.FormatInt(stream.Size(), 10))
	h.Set("X-FloSync-File-Size", strconv.FormatInt(info.Size, 10))
	h.Set("X-FloSync-Version", floSyncVersionString(info.Version))
	w.WriteHeader(http.StatusOK)

	var written int64
	for err == nil {
		n, werr := w.Write(piece)
		written += int64(n)
		if werr != nil {
			slog.Debug("FloSync stream: client went away", slog.String("folder", folder), slogutil.FilePath(file), slogutil.Error(werr))
			return
		}
		_ = rc.Flush()
		piece, err = stream.Next()
	}

	if errors.Is(err, io.EOF) && written == stream.Size() {
		return
	}
	if ctx.Err() != nil {
		slog.Debug("FloSync stream: client went away", slog.String("folder", folder), slogutil.FilePath(file), slogutil.Error(ctx.Err()))
		return
	}
	if errors.Is(err, io.EOF) {
		err = fmt.Errorf("stream ended after %d of %d bytes", written, stream.Size())
	}
	slog.Warn("FloSync stream failed", slog.String("folder", folder), slogutil.FilePath(file), slog.Int64("written", written), slog.Int64("size", stream.Size()), slogutil.Error(err))
	// End the response without completing it, the reader sees a short body.
	panic(http.ErrAbortHandler)
}

// floSyncVersionString formats a version vector the way /rest/db/file does
// (device short id and counter), comma separated.
func floSyncVersionString(v protocol.Vector) string {
	parts := make([]string, len(v.Counters))
	for i, c := range v.Counters {
		parts[i] = fmt.Sprintf("%v:%d", c.ID, c.Value)
	}
	return strings.Join(parts, ",")
}

// postFloSyncDelete announces a file, or a directory tree, as deleted by this
// device, for items it holds only as ignored or not at all. It answers 400 for
// bad parameters, 404 when the file is not in the global index or already
// deleted, and 409 when this device holds the file as a normal file.
func (s *service) postFloSyncDelete(w http.ResponseWriter, r *http.Request) {
	qs := r.URL.Query()
	folder := qs.Get("folder")
	file := qs.Get("file")
	if folder == "" || file == "" {
		http.Error(w, "folder and file are required", http.StatusBadRequest)
		return
	}

	res, err := s.model.FloSyncDelete(r.Context(), folder, file)
	if err != nil {
		if floSyncStatus(err) == http.StatusInternalServerError && r.Context().Err() == nil {
			slog.Warn("FloSync delete failed", slog.String("folder", folder), slogutil.FilePath(file), slogutil.Error(err))
		}
		floSyncError(w, err)
		return
	}
	sendJSON(w, res)
}
