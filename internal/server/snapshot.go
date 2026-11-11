package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

// SnapshotOptions shapes a static snapshot.
type SnapshotOptions struct {
	// Featured is the run the snapshot's banner points to; default the first listed.
	Featured string
	// Note is the banner's text, e.g. what the runs are and when they were recorded.
	Note string
	// Home is where the banner links "Furcate" (the UI itself holds no absolute URLs).
	Home string
}

// SnapshotStats is what a snapshot wrote.
type SnapshotStats struct {
	Listed, Runs, Payloads int
	Bytes                  int64
}

// Snapshot writes a static, read-only copy of the UI to dir: the embedded files,
// and every API answer the UI asks for about the given runs and all their
// branches, as files under api/. Any static host can serve it; app.js reads the
// files instead of the API when index.html carries <meta name="furcate-static">.
// The runs list shows the given runs; branches are reached from their parents.
//
// The answers come from the API's own handlers, so the snapshot shows what
// furcate serve would, with one change: a task's "files", a directory on the
// machine that ran it, is left out.
func Snapshot(ctx context.Context, cfg Config, dir string, ids []string, opt SnapshotOptions) (SnapshotStats, error) {
	var st SnapshotStats
	if cfg.Runs == nil {
		return st, errors.New("a snapshot needs the run store")
	}
	if len(ids) == 0 {
		return st, errors.New("a snapshot needs at least one run")
	}
	if ents, err := os.ReadDir(dir); err == nil && len(ents) > 0 {
		return st, fmt.Errorf("%s is not empty", dir)
	}
	if opt.Featured == "" {
		opt.Featured = ids[0]
	}
	cfg.Forker = nil
	s := New(cfg)
	defer s.Close()

	get := func(p string) ([]byte, error) {
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, p, nil)
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return nil, fmt.Errorf("GET %s: %d %s", p, rec.Code, bytes.TrimSpace(rec.Body.Bytes()))
		}
		return rec.Body.Bytes(), nil
	}
	var mu sync.Mutex
	write := func(rel string, b []byte) error {
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return err
		}
		mu.Lock()
		st.Bytes += int64(len(b))
		mu.Unlock()
		return os.WriteFile(p, b, 0o644)
	}
	writeJSON := func(rel string, v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return write(rel, b)
	}

	// The given runs and, breadth first, every run forked from them.
	all := slices.Clone(ids)
	seen := map[string]bool{}
	for _, id := range ids {
		seen[id] = true
	}
	for i := 0; i < len(all); i++ {
		// IDs become file names; the API already refuses "/".
		if strings.Contains(all[i], "..") {
			return st, fmt.Errorf("run id %q can't be a file name", all[i])
		}
		kids, err := cfg.Runs.Children(ctx, all[i])
		if err != nil {
			return st, fmt.Errorf("run %s: %w", all[i], err)
		}
		for _, k := range kids {
			if !seen[k.RunID] {
				seen[k.RunID] = true
				all = append(all, k.RunID)
			}
		}
	}
	listed := map[string]int{}
	for i, id := range ids {
		listed[id] = i
	}

	items := make([]runItem, len(ids))
	hashes := map[string]bool{}
	errs := make([]error, len(all))
	parallel(len(all), 8, func(i int) {
		id := all[i]
		errs[i] = func() error {
			b, err := get("/api/runs/" + id)
			if err != nil {
				return err
			}
			var d runDetail
			if err := json.Unmarshal(b, &d); err != nil {
				return err
			}
			if d.Run == nil {
				return fmt.Errorf("run %s is not in the run store", id)
			}
			d.Run.Task = withoutFiles(d.Run.Task)
			if err := writeJSON("api/runs/"+id+".json", d); err != nil {
				return err
			}
			if b, err = get("/api/runs/" + id + "/branches"); err != nil {
				return err
			}
			if err := write("api/runs/"+id+"/branches.json", b); err != nil {
				return err
			}
			run, err := cfg.Runs.Run(ctx, id)
			if err != nil {
				return err
			}
			if err := writeJSON("api/files/"+id+".json", run.FinalFiles); err != nil {
				return err
			}
			mu.Lock()
			if d.Trace != nil {
				var walk func([]*spanJSON)
				walk = func(ns []*spanJSON) {
					for _, n := range ns {
						for _, h := range n.Payloads {
							hashes[h] = true
						}
						walk(n.Children)
					}
				}
				walk(d.Trace.Roots)
			}
			mu.Unlock()
			if at, ok := listed[id]; ok {
				it := listItem(&d)
				if err := s.enrich(ctx, &it); err != nil {
					return err
				}
				items[at] = it
			}
			return nil
		}()
	})
	if err := errors.Join(errs...); err != nil {
		return st, err
	}

	hs := slices.Sorted(func(yield func(string) bool) {
		for h := range hashes {
			if !yield(h) {
				return
			}
		}
	})
	errs = make([]error, len(hs))
	parallel(len(hs), 8, func(i int) {
		b, err := get("/api/payloads/" + hs[i])
		if err == nil {
			err = write("api/payloads/"+hs[i]+".txt", b)
		}
		errs[i] = err
	})
	if err := errors.Join(errs...); err != nil {
		return st, err
	}

	slices.SortStableFunc(items, func(a, b runItem) int { return b.Start.Compare(a.Start) })
	if err := writeJSON("api/runs.json", map[string]any{"runs": items, "since_hours": 0, "limit": len(items)}); err != nil {
		return st, err
	}
	if err := writeJSON("api/info.json", map[string]any{"fork_enabled": false, "max_n": 0, "static": true,
		"featured": opt.Featured, "note": opt.Note, "home": opt.Home}); err != nil {
		return st, err
	}
	if err := writeUI(write); err != nil {
		return st, err
	}
	st.Listed, st.Runs, st.Payloads = len(ids), len(all), len(hs)
	return st, nil
}

// listItem is a run's row in the runs list, from its detail: what GET
// /api/runs would say about it, without the trace store's time window.
func listItem(d *runDetail) runItem {
	it := runItem{ID: d.ID, Name: d.Run.TaskID, Start: d.Run.Started}
	if d.Run.DurationMs != nil {
		it.DurationMs = *d.Run.DurationMs
	}
	if t := d.Trace; t != nil {
		it.Start, it.DurationMs, it.Spans = t.Start, t.DurationMs, uint64(t.Spans)
		it.Errors, it.Tokens, it.CostUSD = uint64(t.Totals.Errors), t.Totals.TokensIn+t.Totals.TokensOut, t.Totals.CostUSD
		if len(t.Roots) > 0 {
			it.Name, it.Service, it.SpanStatus = t.Roots[0].Name, t.Roots[0].Service, t.Roots[0].Status
		}
	}
	return it
}

// withoutFiles drops a task's "files", a path on the machine that ran it.
func withoutFiles(task json.RawMessage) json.RawMessage {
	var m map[string]json.RawMessage
	if json.Unmarshal(task, &m) != nil || m["files"] == nil {
		return task
	}
	delete(m, "files")
	b, err := json.Marshal(m)
	if err != nil {
		return task
	}
	return b
}

// writeUI copies the embedded UI, marking index.html as a snapshot.
func writeUI(write func(string, []byte) error) error {
	return fs.WalkDir(staticFiles, "static", func(p string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		b, err := staticFiles.ReadFile(p)
		if err != nil {
			return err
		}
		rel := path.Base(p)
		if rel == "index.html" {
			const charset = `<meta charset="utf-8">`
			if !bytes.Contains(b, []byte(charset)) {
				return errors.New("index.html has no charset meta to put the snapshot marker after")
			}
			b = bytes.Replace(b, []byte(charset), []byte(charset+"\n"+`<meta name="furcate-static" content="1">`), 1)
		}
		return write(rel, b)
	})
}
