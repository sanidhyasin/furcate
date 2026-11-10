// Package server is Furcate's HTTP API and the web UI on top of it. It reads
// spans and payloads from the trace store and run state from the run store and,
// when it has a Forker, runs forks in the background as jobs the UI polls.
//
// The UI is plain HTML, CSS and JS embedded in the binary: no build step, and no
// request leaves the server's own origin.
package server

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/sanidhyasin/furcate/internal/model"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/store"
)

//go:embed static
var staticFiles embed.FS

// TraceStore is the part of the ClickHouse store the server reads (*store.CH).
type TraceStore interface {
	Runs(ctx context.Context, since time.Duration, limit int) ([]store.RunSummary, error)
	Trace(ctx context.Context, traceID string, useIndex bool) ([]model.Span, error)
	Payload(ctx context.Context, hash string) ([]byte, error)
}

// Forker runs N continuations of a run from one of its steps (*replay.Engine).
type Forker interface {
	Fork(ctx context.Context, req replay.Request) (*replay.Result, error)
}

// Config is what the server serves from.
type Config struct {
	Traces TraceStore     // required
	Runs   runstore.Store // nil: no run state, so runs show as traces only
	Forker Forker         // nil: read-only, and the fork endpoint answers 501
	Log    *slog.Logger   // nil: discard
	MaxN   int            // most continuations one fork may ask for; default 100
	// MaxJobs is how many fork jobs are remembered for polling; the oldest
	// finished ones are dropped first. Default 200.
	MaxJobs int
}

// Server serves the API and the UI. Close it to cancel running forks.
type Server struct {
	traces  TraceStore
	runs    runstore.Store
	forker  Forker
	log     *slog.Logger
	maxN    int
	maxJobs int
	handler http.Handler

	ctx    context.Context // parent of every fork job; cancelled by Close
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	closed bool
	jobs   map[string]*job
	order  []string // job ids, oldest first
}

// New builds a server. It panics if cfg.Traces is nil.
func New(cfg Config) *Server {
	if cfg.Traces == nil {
		panic("server: Config.Traces is required")
	}
	s := &Server{
		traces: cfg.Traces, runs: cfg.Runs, forker: cfg.Forker, log: cfg.Log,
		maxN: cfg.MaxN, maxJobs: cfg.MaxJobs, jobs: map[string]*job{},
	}
	if s.runs == nil {
		s.runs = runstore.NewMemory() // every lookup misses: runs read as untracked traces
	}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	if s.maxN <= 0 {
		s.maxN = 100
	}
	if s.maxJobs <= 0 {
		s.maxJobs = 200
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	mux := http.NewServeMux()
	s.handle(mux, "GET /api/info", s.info)
	s.handle(mux, "GET /api/runs", s.listRuns)
	s.handle(mux, "GET /api/runs/{id}", s.getRun)
	s.handle(mux, "GET /api/runs/{id}/branches", s.branches)
	s.handle(mux, "POST /api/runs/{id}/fork", s.fork)
	s.handle(mux, "GET /api/jobs/{id}", s.getJob)
	s.handle(mux, "GET /api/payloads/{hash}", s.payload)
	s.handle(mux, "GET /api/compare", s.compare)
	s.handle(mux, "/api/", func(w http.ResponseWriter, r *http.Request) error {
		return errorf(http.StatusNotFound, "no such endpoint: %s %s", r.Method, r.URL.Path)
	})
	mux.Handle("/", staticHandler())

	// Forks cost money, so a page on another origin must not be able to start one.
	cop := http.NewCrossOriginProtection()
	cop.SetDenyHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusForbidden, errorBody{"cross-origin request rejected"})
	}))
	s.handler = cop.Handler(mux)
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	s.handler.ServeHTTP(w, r)
}

// Close cancels running forks and waits for their jobs to return. New forks are
// refused afterwards.
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.cancel()
	s.wg.Wait()
}

// staticHandler serves the embedded UI. The CSP keeps every request on this
// origin: no CDNs, no inline scripts.
func staticHandler() http.Handler {
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic(err)
	}
	files := http.FileServerFS(sub)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; "+
			"img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-cache")
		files.ServeHTTP(w, r)
	})
}

// httpError is an error with a status code; anything else is a 500 unless it
// wraps a not-found error from a store.
type httpError struct {
	status int
	msg    string
}

func (e *httpError) Error() string { return e.msg }

func errorf(status int, format string, a ...any) error {
	return &httpError{status: status, msg: fmt.Sprintf(format, a...)}
}

type errorBody struct {
	Error string `json:"error"`
}

func (s *Server) handle(mux *http.ServeMux, pattern string, h func(http.ResponseWriter, *http.Request) error) {
	mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			s.fail(w, r, err)
		}
	})
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusInternalServerError
	var he *httpError
	switch {
	case errors.As(err, &he):
		status = he.status
	case errors.Is(err, runstore.ErrNotFound), errors.Is(err, store.ErrNoTrace), errors.Is(err, store.ErrNoPayload):
		status = http.StatusNotFound
	case errors.Is(err, context.Canceled) && r.Context().Err() != nil:
		return // the client went away
	}
	if status >= 500 {
		s.log.Error("api", "method", r.Method, "path", r.URL.Path, "err", err)
	}
	writeJSON(w, status, errorBody{err.Error()})
}

// writeJSON encodes before writing, so an encoding failure is still a clean 500.
func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		status = http.StatusInternalServerError
		b, _ = json.Marshal(errorBody{"encode response: " + err.Error()})
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	w.Write(append(b, '\n'))
}

func newID(prefix string) string {
	b := make([]byte, 8)
	rand.Read(b)
	return prefix + hex.EncodeToString(b)
}
