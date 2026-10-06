// Copyright (C) 2026 Carel Kuun (Flogent FloSync fork of Syncthing).
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thejerf/suture/v4"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	connmocks "github.com/syncthing/syncthing/lib/connections/mocks"
	discovermocks "github.com/syncthing/syncthing/lib/discover/mocks"
	"github.com/syncthing/syncthing/lib/events"
	eventmocks "github.com/syncthing/syncthing/lib/events/mocks"
	"github.com/syncthing/syncthing/lib/model"
	modelmocks "github.com/syncthing/syncthing/lib/model/mocks"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/ur"
)

// A second short device id for the version vectors in the tests.
var device2Short = func() protocol.ShortID {
	id, _ := protocol.DeviceIDFromString("GYRZZQB-IRNPV4Z-T7TC52W-EQYJ3TT-FDQW6MW-DFLMU42-SSSU6EM-FBK2VAY")
	return id.Short()
}()

// startHTTPWithModel is startHTTP with the model given by the test.
func startHTTPWithModel(t *testing.T, cfg config.Wrapper, m model.Model) string {
	t.Helper()
	eventSub := new(eventmocks.BufferedSubscription)
	diskEventSub := new(eventmocks.BufferedSubscription)
	discoverer := new(discovermocks.Manager)
	connections := new(connmocks.Service)
	errorLog := slogutil.NewRecorder(0)
	systemLog := slogutil.NewRecorder(0)
	addrChan := make(chan string)
	mockedSummary := &modelmocks.FolderSummaryService{}
	mockedSummary.SummaryReturns(new(model.FolderSummary), nil)

	urService := ur.New(cfg, m, connections, false)
	mdb, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mdb.Close() })
	kdb := db.NewMiscDB(mdb)
	svc := New(protocol.LocalDeviceID, cfg, "../../gui", "syncthing", m, eventSub, diskEventSub, events.NoopLogger, discoverer, connections, urService, mockedSummary, errorLog, systemLog, false, kdb).(*service)
	svc.started = addrChan

	supervisor := suture.New("API test", suture.Spec{PassThroughPanics: true})
	supervisor.Add(svc)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	supervisor.ServeBackground(ctx)

	addr := <-addrChan
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("http://%s", net.JoinHostPort("127.0.0.1", strconv.Itoa(tcpAddr.Port)))
}

// fakeStream is a model.FloSyncStream with pieces given by the test.
type fakeStream struct {
	info    protocol.FileInfo
	pieces  [][]byte
	failAt  int   // the piece at which Next fails, -1 for never
	err     error // the failure
	delay   time.Duration
	blockAt int // the piece at which Next waits for the context to be done, -1 for never

	ctx       context.Context
	mut       sync.Mutex
	next      int
	closed    chan struct{}
	closeOnce sync.Once
	ctxDone   chan struct{} // closed when ctx was seen done in Next
	ctxOnce   sync.Once
}

func newFakeStream(pieces ...[]byte) *fakeStream {
	var size int64
	for _, p := range pieces {
		size += int64(len(p))
	}
	return &fakeStream{
		info: protocol.FileInfo{
			Name:    "dir/file.bin",
			Size:    size,
			Version: protocol.Vector{Counters: []protocol.Counter{{ID: dev1.Short(), Value: 7}, {ID: device2Short, Value: 9}}},
		},
		pieces:  pieces,
		failAt:  -1,
		blockAt: -1,
		closed:  make(chan struct{}),
		ctxDone: make(chan struct{}),
	}
}

func (s *fakeStream) Size() int64 {
	var n int64
	for _, p := range s.pieces {
		n += int64(len(p))
	}
	return n
}

func (s *fakeStream) Info() protocol.FileInfo { return s.info }

func (s *fakeStream) Next() ([]byte, error) {
	s.mut.Lock()
	i := s.next
	s.next++
	s.mut.Unlock()

	if i == s.blockAt {
		<-s.ctx.Done()
		s.ctxOnce.Do(func() { close(s.ctxDone) })
		return nil, s.ctx.Err()
	}
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-s.ctx.Done():
			s.ctxOnce.Do(func() { close(s.ctxDone) })
			return nil, s.ctx.Err()
		}
	}
	if i == s.failAt {
		return nil, s.err
	}
	if i >= len(s.pieces) {
		return nil, io.EOF
	}
	return s.pieces[i], nil
}

func (s *fakeStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

// streams hands the streams that the mocked model opened to the test.
type streams struct {
	mut  sync.Mutex
	last *fakeStream
}

func (h *streams) set(s *fakeStream) {
	h.mut.Lock()
	h.last = s
	h.mut.Unlock()
}

func (h *streams) get() *fakeStream {
	h.mut.Lock()
	defer h.mut.Unlock()
	return h.last
}

func floSyncGet(t *testing.T, url string, key string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func floSyncPost(t *testing.T, url string, key string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestFloSyncStreamEndpoint(t *testing.T) {
	t.Parallel()

	m := new(modelmocks.Model)
	base := startHTTPWithModel(t, apiCfg, m)

	pieces := [][]byte{bytes.Repeat([]byte("a"), 1000), bytes.Repeat([]byte("b"), 1000), []byte("tail")}
	var opened streams
	openPieces := func(ctx context.Context, _, _ string, _, _ int64) (model.FloSyncStream, error) {
		s := newFakeStream(pieces...)
		s.ctx = ctx
		opened.set(s)
		return s, nil
	}
	m.OpenFloSyncStreamCalls(openPieces)

	t.Run("bytes and headers", func(t *testing.T) {
		resp := floSyncGet(t, base+"/rest/flosync/stream?folder=fl%20o&file=dir%2Ffile%20x.bin&offset=5&length=2004", testAPIKey)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %s", resp.Status)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if want := bytes.Join(pieces, nil); !bytes.Equal(body, want) {
			t.Errorf("got %d bytes that differ from the %d expected", len(body), len(want))
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/octet-stream" {
			t.Errorf("Content-Type %q", ct)
		}
		if resp.ContentLength != 2004 {
			t.Errorf("Content-Length %d, want 2004", resp.ContentLength)
		}
		if v := resp.Header.Get("X-FloSync-File-Size"); v != "2004" {
			t.Errorf("X-FloSync-File-Size %q", v)
		}
		if want := fmt.Sprintf("%v:7,%v:9", dev1.Short(), device2Short); resp.Header.Get("X-FloSync-Version") != want {
			t.Errorf("X-FloSync-Version %q, want %q", resp.Header.Get("X-FloSync-Version"), want)
		}

		_, folder, file, offset, length := lastOpenArgs(m)
		if folder != "fl o" || file != "dir/file x.bin" || offset != 5 || length != 2004 {
			t.Errorf("the model was asked for %q %q offset %d length %d", folder, file, offset, length)
		}
		select {
		case <-opened.get().closed:
		case <-time.After(5 * time.Second):
			t.Error("the stream was not closed")
		}
	})

	t.Run("defaults", func(t *testing.T) {
		resp := floSyncGet(t, base+"/rest/flosync/stream?folder=f&file=x", testAPIKey)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %s", resp.Status)
		}
		_, _, _, offset, length := lastOpenArgs(m)
		if offset != 0 || length != -1 {
			t.Errorf("defaults are offset %d length %d, want 0 and -1 (to the end)", offset, length)
		}
	})

	t.Run("empty stream", func(t *testing.T) {
		m.OpenFloSyncStreamCalls(func(ctx context.Context, _, _ string, _, _ int64) (model.FloSyncStream, error) {
			s := newFakeStream()
			s.ctx = ctx
			return s, nil
		})
		defer m.OpenFloSyncStreamCalls(openPieces)
		resp := floSyncGet(t, base+"/rest/flosync/stream?folder=f&file=x", testAPIKey)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || err != nil || len(body) != 0 || resp.ContentLength != 0 {
			t.Errorf("empty file: %s, %v, %d bytes, length %d", resp.Status, err, len(body), resp.ContentLength)
		}
	})
}

func lastOpenArgs(m *modelmocks.Model) (context.Context, string, string, int64, int64) {
	return m.OpenFloSyncStreamArgsForCall(m.OpenFloSyncStreamCallCount() - 1)
}

func TestFloSyncStreamEndpointErrors(t *testing.T) {
	t.Parallel()

	m := new(modelmocks.Model)
	base := startHTTPWithModel(t, apiCfg, m)

	// Parameters that are wrong never reach the model.
	for _, tc := range []struct{ query string }{
		{""},
		{"folder=f"},
		{"file=x"},
		{"folder=&file=x"},
		{"folder=f&file="},
		{"folder=f&file=x&offset=abc"},
		{"folder=f&file=x&offset=-1"},
		{"folder=f&file=x&offset=1.5"},
		{"folder=f&file=x&length=-5"},
		{"folder=f&file=x&length=ten"},
		{"folder=f&file=x&offset=99999999999999999999"},
	} {
		resp := floSyncGet(t, base+"/rest/flosync/stream?"+tc.query, testAPIKey)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("?%s: status %s, want 400", tc.query, resp.Status)
		}
	}
	if n := m.OpenFloSyncStreamCallCount(); n != 0 {
		t.Errorf("the model was called %d times for bad parameters", n)
	}

	// What the model says, as a status.
	for _, tc := range []struct {
		err  error
		code int
	}{
		{fmt.Errorf("%w: x", model.ErrFloSyncBadRequest), http.StatusBadRequest},
		{fmt.Errorf("%w: x", model.ErrFloSyncNotFound), http.StatusNotFound},
		{model.ErrFolderMissing, http.StatusNotFound},
		{fmt.Errorf("%w: x", model.ErrFloSyncHeld), http.StatusConflict},
		{fmt.Errorf("%w: x", model.ErrFloSyncBadRange), http.StatusRequestedRangeNotSatisfiable},
		{fmt.Errorf("%w: x", model.ErrFloSyncNoSource), http.StatusServiceUnavailable},
		{fmt.Errorf("%w: x", model.ErrFloSyncUnavailable), http.StatusServiceUnavailable},
		{model.ErrFolderPaused, http.StatusServiceUnavailable},
		{model.ErrFolderNotRunning, http.StatusServiceUnavailable},
		{errors.New("database is on fire"), http.StatusInternalServerError},
	} {
		m.OpenFloSyncStreamReturns(nil, tc.err)
		resp := floSyncGet(t, base+"/rest/flosync/stream?folder=f&file=x", testAPIKey)
		msg, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != tc.code {
			t.Errorf("%v: status %s, want %d", tc.err, resp.Status, tc.code)
		}
		if !strings.Contains(string(msg), tc.err.Error()) {
			t.Errorf("%v: the message is %q", tc.err, msg)
		}
	}

	// The first piece cannot be had: no device has the block. The status is
	// still an error status, not a 200 with a short body.
	m.OpenFloSyncStreamCalls(func(ctx context.Context, _, _ string, _, _ int64) (model.FloSyncStream, error) {
		s := newFakeStream([]byte("data"))
		s.ctx = ctx
		s.failAt = 0
		s.err = fmt.Errorf("%w: all devices failed", model.ErrFloSyncNoSource)
		return s, nil
	})
	resp := floSyncGet(t, base+"/rest/flosync/stream?folder=f&file=x", testAPIKey)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("first piece failed: status %s, want 503", resp.Status)
	}

	// Wrong methods.
	resp = floSyncPost(t, base+"/rest/flosync/stream?folder=f&file=x", testAPIKey)
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST stream: status %s, want 405", resp.Status)
	}
}

// A failure after the first bytes ends the response early: the reader sees
// a short body and an error, never a body that looks complete.
func TestFloSyncStreamEndpointFailsMidStream(t *testing.T) {
	t.Parallel()

	m := new(modelmocks.Model)
	base := startHTTPWithModel(t, apiCfg, m)
	pieces := [][]byte{bytes.Repeat([]byte("a"), 50000), bytes.Repeat([]byte("b"), 50000), bytes.Repeat([]byte("c"), 50000)}
	var opened streams
	m.OpenFloSyncStreamCalls(func(ctx context.Context, _, _ string, _, _ int64) (model.FloSyncStream, error) {
		s := newFakeStream(pieces...)
		s.ctx = ctx
		s.failAt = 2
		s.err = fmt.Errorf("%w: device went away", model.ErrFloSyncNoSource)
		opened.set(s)
		return s, nil
	})

	resp := floSyncGet(t, base+"/rest/flosync/stream?folder=f&file=x", testAPIKey)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ContentLength != 150000 {
		t.Fatalf("status %s, length %d", resp.Status, resp.ContentLength)
	}
	body, err := io.ReadAll(resp.Body)
	if err == nil {
		t.Fatalf("a failed stream looked complete: %d bytes and no error", len(body))
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("expected an unexpected EOF, got %v", err)
	}
	if want := bytes.Join(pieces[:2], nil); !bytes.Equal(body, want) {
		t.Errorf("got %d bytes, want the %d bytes before the failure", len(body), len(want))
	}
	select {
	case <-opened.get().closed:
	case <-time.After(5 * time.Second):
		t.Error("the stream was not closed")
	}
}

// When the client goes away the request context is cancelled, which cancels
// the stream.
func TestFloSyncStreamEndpointClientGoesAway(t *testing.T) {
	t.Parallel()

	m := new(modelmocks.Model)
	base := startHTTPWithModel(t, apiCfg, m)
	var opened streams
	m.OpenFloSyncStreamCalls(func(ctx context.Context, _, _ string, _, _ int64) (model.FloSyncStream, error) {
		// The second piece waits until the request is cancelled.
		s := newFakeStream([]byte("first"), []byte("second"), []byte("third"))
		s.ctx = ctx
		s.blockAt = 1
		opened.set(s)
		return s, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/rest/flosync/stream?folder=f&file=x", nil)
	req.Header.Set("X-API-Key", testAPIKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(resp.Body, buf); err != nil || string(buf) != "first" {
		t.Fatalf("first piece: %q, %v", buf, err)
	}
	cancel()
	resp.Body.Close()

	stream := opened.get()
	select {
	case <-stream.ctxDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the stream was not cancelled when the client went away")
	}
	select {
	case <-stream.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the stream was not closed")
	}
}

// The server's ReadTimeout (15 s) must not cut a stream that takes longer.
// Checked here with a short ReadTimeout on a server of its own.
func TestFloSyncStreamEndpointOutlivesReadTimeout(t *testing.T) {
	t.Parallel()

	m := new(modelmocks.Model)
	pieces := [][]byte{[]byte("one"), []byte("two"), []byte("three"), []byte("four"), []byte("five")}
	var opened streams
	m.OpenFloSyncStreamCalls(func(ctx context.Context, _, _ string, _, _ int64) (model.FloSyncStream, error) {
		s := newFakeStream(pieces...)
		s.ctx = ctx
		s.delay = 250 * time.Millisecond // 1.25 s in all
		opened.set(s)
		return s, nil
	})
	svc := &service{model: m}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(svc.getFloSyncStream))
	srv.Config.ReadTimeout = 300 * time.Millisecond
	srv.Start()
	defer srv.Close()

	start := time.Now()
	resp, err := http.Get(srv.URL + "?folder=f&file=x")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || string(body) != "onetwothreefourfive" {
		t.Errorf("got %q, %v", body, err)
	}
	if took := time.Since(start); took < time.Second {
		t.Errorf("the stream took %v, less than the ReadTimeout and delays say, the test did not test anything", took)
	}
}

func TestFloSyncDeleteEndpoint(t *testing.T) {
	t.Parallel()

	m := new(modelmocks.Model)
	base := startHTTPWithModel(t, apiCfg, m)

	m.FloSyncDeleteReturns(model.FloSyncDeleteResult{Deleted: 4, Held: 1}, nil)
	resp := floSyncPost(t, base+"/rest/flosync/delete?folder=fl%20o&file=dir%2Fsub%20x", testAPIKey)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %s: %s", resp.Status, body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type %q", ct)
	}
	var res model.FloSyncDeleteResult
	if err := json.Unmarshal(body, &res); err != nil || res.Deleted != 4 || res.Held != 1 {
		t.Errorf("body %s, %v", body, err)
	}
	_, folder, file := m.FloSyncDeleteArgsForCall(0)
	if folder != "fl o" || file != "dir/sub x" {
		t.Errorf("the model was asked to delete %q in %q", file, folder)
	}

	// Parameters that are wrong never reach the model.
	calls := m.FloSyncDeleteCallCount()
	for _, q := range []string{"", "folder=f", "file=x", "folder=&file=x", "folder=f&file="} {
		resp := floSyncPost(t, base+"/rest/flosync/delete?"+q, testAPIKey)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("?%s: status %s, want 400", q, resp.Status)
		}
	}
	if n := m.FloSyncDeleteCallCount(); n != calls {
		t.Errorf("the model was called %d times for bad parameters", n-calls)
	}

	for _, tc := range []struct {
		err  error
		code int
	}{
		{fmt.Errorf("%w: x", model.ErrFloSyncBadRequest), http.StatusBadRequest},
		{fmt.Errorf("%w: x", model.ErrFloSyncNotFound), http.StatusNotFound},
		{model.ErrFolderMissing, http.StatusNotFound},
		{fmt.Errorf("%w: x", model.ErrFloSyncHeld), http.StatusConflict},
		{fmt.Errorf("%w: x", model.ErrFloSyncUnavailable), http.StatusServiceUnavailable},
		{model.ErrFolderPaused, http.StatusServiceUnavailable},
		{context.Canceled, http.StatusInternalServerError},
		{errors.New("database is on fire"), http.StatusInternalServerError},
	} {
		m.FloSyncDeleteReturns(model.FloSyncDeleteResult{}, tc.err)
		resp := floSyncPost(t, base+"/rest/flosync/delete?folder=f&file=x", testAPIKey)
		resp.Body.Close()
		if resp.StatusCode != tc.code {
			t.Errorf("%v: status %s, want %d", tc.err, resp.Status, tc.code)
		}
	}

	// Wrong method: a delete is never done by a GET.
	calls = m.FloSyncDeleteCallCount()
	resp = floSyncGet(t, base+"/rest/flosync/delete?folder=f&file=x", testAPIKey)
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET delete: status %s, want 405", resp.Status)
	}
	if n := m.FloSyncDeleteCallCount(); n != calls {
		t.Error("a GET reached the model")
	}
}

// Like every /rest endpoint these need the API key.
func TestFloSyncEndpointsNeedTheAPIKey(t *testing.T) {
	t.Parallel()

	m := new(modelmocks.Model)
	base := startHTTPWithModel(t, apiCfg, m)

	for _, tc := range []struct {
		method string
		url    string
	}{
		{http.MethodGet, "/rest/flosync/stream?folder=f&file=x"},
		{http.MethodPost, "/rest/flosync/delete?folder=f&file=x"},
	} {
		for _, key := range []string{"", testAPIKey + "X"} {
			req, _ := http.NewRequest(tc.method, base+tc.url, nil)
			if key != "" {
				req.Header.Set("X-API-Key", key)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("%s %s with key %q: status %s, want 403", tc.method, tc.url, key, resp.Status)
			}
		}
		// As a bearer token the key works too.
		req, _ := http.NewRequest(tc.method, base+tc.url, nil)
		req.Header.Set("Authorization", "Bearer "+testAPIKey)
		m.OpenFloSyncStreamReturns(nil, model.ErrFloSyncNotFound)
		m.FloSyncDeleteReturns(model.FloSyncDeleteResult{}, model.ErrFloSyncNotFound)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s %s with a bearer token: status %s, want 404 from the model", tc.method, tc.url, resp.Status)
		}
	}
	if m.OpenFloSyncStreamCallCount()+m.FloSyncDeleteCallCount() != 2 {
		t.Errorf("the model was reached by %d requests, want only the 2 with a good key",
			m.OpenFloSyncStreamCallCount()+m.FloSyncDeleteCallCount())
	}
}

func TestFloSyncVersionString(t *testing.T) {
	if s := floSyncVersionString(protocol.Vector{}); s != "" {
		t.Errorf("empty vector: %q", s)
	}
	// The same text as the version in /rest/db/file.
	v := protocol.Vector{Counters: []protocol.Counter{{ID: dev1.Short(), Value: 3}}}
	if want := dev1.Short().String() + ":3"; floSyncVersionString(v) != want {
		t.Errorf("got %q, want %q", floSyncVersionString(v), want)
	}
	j, _ := json.Marshal(jsonVersionVector(v))
	if want := `["` + floSyncVersionString(v) + `"]`; string(j) != want {
		t.Errorf("the header %q does not match the JSON version %s", want, j)
	}
}
