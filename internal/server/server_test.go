package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/model"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/stats"
	"github.com/sanidhyasin/furcate/internal/store"
)

const (
	hashReq  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hashResp = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type fakeTraces struct {
	runs     []store.RunSummary
	spans    map[string][]model.Span
	payloads map[string]string
}

func (f *fakeTraces) Runs(_ context.Context, _ time.Duration, limit int) ([]store.RunSummary, error) {
	return f.runs[:min(limit, len(f.runs))], nil
}

func (f *fakeTraces) Trace(_ context.Context, id string, _ bool) ([]model.Span, error) {
	s, ok := f.spans[id]
	if !ok {
		return nil, store.ErrNoTrace
	}
	return s, nil
}

func (f *fakeTraces) Payload(_ context.Context, hash string) ([]byte, error) {
	p, ok := f.payloads[hash]
	if !ok {
		return nil, store.ErrNoPayload
	}
	return []byte(p), nil
}

// fakeForker blocks until released (or cancelled), then answers with res or err.
type fakeForker struct {
	release chan struct{}
	err     error
	mu      sync.Mutex
	got     []replay.Request
}

func (f *fakeForker) Fork(ctx context.Context, req replay.Request) (*replay.Result, error) {
	f.mu.Lock()
	f.got = append(f.got, req)
	f.mu.Unlock()
	select {
	case <-f.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if f.err != nil {
		return nil, f.err
	}
	res := &replay.Result{Request: req, Passes: 2, Done: 3, Usage: runstore.Usage{LLMCalls: 6, CostUSD: 0.3}, Branches: []controller.Result{
		{RunID: "b1", Status: runstore.StatusDone, Outcome: runstore.OutcomePass, Usage: runstore.Usage{CostUSD: 0.1}},
		{RunID: "b2", Status: runstore.StatusDone, Outcome: runstore.OutcomePass, Usage: runstore.Usage{CostUSD: 0.1}},
		{RunID: "b3", Status: runstore.StatusDone, Outcome: runstore.OutcomeFail, Usage: runstore.Usage{CostUSD: 0.1}},
		{RunID: "b4", Status: runstore.StatusError, Err: errors.New("sandbox died")},
	}}
	res.Lo, res.Hi = stats.Wilson(2, 3, stats.Z95)
	return res, nil
}

// fixture: run "r1" with steps 1 (llm, cp_1), 2 (tool), 3 (llm, cp_3, garbage-
// collected); four branches from step 1 (one change, one plain rerun) and one
// still running; and "lg1", a trace with no run-store row.
func fixture(t *testing.T) (*fakeTraces, *runstore.Memory) {
	t.Helper()
	ctx := context.Background()
	ms := runstore.NewMemory()
	t0 := time.Now().Add(-time.Hour).UTC().Truncate(time.Millisecond)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(ms.CreateRun(ctx, runstore.Run{RunID: "r1", TaskID: "keep-fixture", Task: json.RawMessage(`{"id":"keep-fixture","prompt":"fix it"}`),
		Driver: "testdir", StartedAt: t0}))
	must(ms.FinishRun(ctx, "r1", runstore.End{Status: runstore.StatusDone, Outcome: runstore.OutcomeFail,
		Usage:      runstore.Usage{LLMCalls: 2, InputTokens: 100, OutputTokens: 50, CostUSD: 0.02},
		FinalFiles: map[string]string{"a.txt": "h1", "b.txt": "h2", "gone.txt": "h3"}}))
	must(ms.AddCheckpoint(ctx, runstore.Checkpoint{CheckpointID: "cp_1", RunID: "r1", Step: 1, Driver: "testdir", Level: 1, DriverRef: "ref1"}))
	must(ms.AddCheckpoint(ctx, runstore.Checkpoint{CheckpointID: "cp_3", RunID: "r1", Step: 3, Driver: "testdir", Level: 1, DriverRef: "ref3"}))
	must(ms.MarkDeleted(ctx, "cp_3"))
	must(ms.AddStep(ctx, runstore.Step{RunID: "r1", Index: 1, SpanID: "s-llm1", Kind: runstore.KindLLM, CheckpointID: "cp_1"}))
	must(ms.AddStep(ctx, runstore.Step{RunID: "r1", Index: 2, SpanID: "s-tool", Kind: runstore.KindTool}))
	must(ms.AddStep(ctx, runstore.Step{RunID: "r1", Index: 3, SpanID: "s-llm3", Kind: runstore.KindLLM, CheckpointID: "cp_3"}))

	branch := func(id string, step int, change, outcome, status string, files map[string]string) {
		var ch json.RawMessage
		if change != "" {
			ch = json.RawMessage(change)
		}
		must(ms.CreateRun(ctx, runstore.Run{RunID: id, TaskID: "keep-fixture", Driver: "testdir", ParentRunID: "r1",
			ParentStep: step, Change: ch, StartedAt: t0.Add(time.Minute)}))
		if status != runstore.StatusRunning {
			must(ms.FinishRun(ctx, id, runstore.End{Status: status, Outcome: outcome, FinalFiles: files,
				Usage: runstore.Usage{LLMCalls: 1, CostUSD: 0.01}}))
		}
	}
	// Postgres hands JSONB back reformatted; both spellings must group together.
	branch("c1", 1, `{"append_system":"Never delete fixture.txt."}`, runstore.OutcomePass, runstore.StatusDone,
		map[string]string{"a.txt": "h1", "b.txt": "changed", "new.txt": "h4"})
	branch("c2", 1, `{"append_system": "Never delete fixture.txt."}`, runstore.OutcomePass, runstore.StatusDone, nil)
	branch("c3", 1, `{"append_system":"Never delete fixture.txt."}`, runstore.OutcomeFail, runstore.StatusDone, nil)
	branch("c4", 1, `{"append_system":"Never delete fixture.txt."}`, "", runstore.StatusError, nil)
	branch("c5", 1, `{}`, "", runstore.StatusRunning, nil)

	span := func(id, parent, kind, name string, off, dur time.Duration, attrs, payloads map[string]string) model.Span {
		return model.Span{TraceID: "r1", SpanID: id, ParentSpanID: parent, RunID: "r1", Kind: kind, Name: name,
			Start: t0.Add(off), End: t0.Add(off + dur), Status: model.StatusOK, Attributes: attrs, Payloads: payloads}
	}
	llm1 := span("s-llm1", "root", model.KindLLM, "chat claude-opus-5-5", 10*time.Millisecond, 100*time.Millisecond,
		map[string]string{attrStepIndex: "1", attrCheckpointID: "cp_1"},
		map[string]string{"furcate.llm.request": hashReq, "furcate.llm.response": hashResp})
	llm1.Model, llm1.TokensIn, llm1.TokensOut, llm1.CostUSD = "claude-opus-5-5", 100, 20, 0.01
	tool := span("s-tool", "root", model.KindTool, "execute_tool bash", 120*time.Millisecond, 30*time.Millisecond,
		map[string]string{attrStepIndex: "2"}, nil)
	tool.Status, tool.StatusMessage = model.StatusError, "tool call failed"
	traces := &fakeTraces{
		spans: map[string][]model.Span{
			// Children first: the store keeps arrival order, the tree sorts it out.
			"r1": {tool, llm1, span("root", "", model.KindAgent, "invoke_agent keep-fixture", 0, time.Second, nil, nil),
				span("s-llm3", "root", model.KindLLM, "chat claude-opus-5-5", 200*time.Millisecond, 100*time.Millisecond,
					map[string]string{attrStepIndex: "3", attrCheckpointID: "cp_3"}, nil)},
			"lg1": {{TraceID: "lg1", SpanID: "x", Kind: model.KindAgent, Name: "loadgen", Start: t0, End: t0.Add(time.Second), Status: model.StatusOK}},
		},
		payloads: map[string]string{hashReq: `{"model":"claude-opus-5-5","messages":[]}`, hashResp: `{"id":"msg_1"}`},
		runs: []store.RunSummary{
			{TraceID: "c1", Name: "invoke_agent keep-fixture", Start: t0.Add(time.Minute), Duration: time.Second, Status: "ok", Spans: 2},
			{TraceID: "r1", Name: "invoke_agent keep-fixture", Start: t0, Duration: time.Second, Status: "error", Spans: 4, Errors: 1, Tokens: 120, CostUSD: 0.01},
			{TraceID: "lg1", Name: "loadgen", Start: t0, Duration: time.Second, Status: "ok", Spans: 1},
		},
	}
	return traces, ms
}

func newServer(t *testing.T, forker Forker) (*Server, *httptest.Server) {
	t.Helper()
	traces, ms := fixture(t)
	s := New(Config{Traces: traces, Runs: ms, Forker: forker, MaxN: 20})
	ts := httptest.NewServer(s)
	t.Cleanup(func() {
		ts.Close()
		s.Close()
	})
	return s, ts
}

// do sends a request and decodes a JSON answer into out (if not nil).
func do(t *testing.T, ts *httptest.Server, method, path, body string, wantStatus int, out any) {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s: status %d, want %d; body %s", method, path, resp.StatusCode, wantStatus, b)
	}
	if out != nil {
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s %s: content type %q", method, path, ct)
		}
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("%s %s: %v in %s", method, path, err, b)
		}
	}
}

// wantError checks a JSON error answer and returns its message.
func wantError(t *testing.T, ts *httptest.Server, method, path, body string, status int) string {
	t.Helper()
	var e struct{ Error string }
	do(t, ts, method, path, body, status, &e)
	if e.Error == "" {
		t.Fatalf("%s %s: empty error message", method, path)
	}
	return e.Error
}

func TestStaticFilesEmbeddedAndServed(t *testing.T) {
	for _, name := range []string{"static/index.html", "static/app.js", "static/style.css"} {
		if b, err := fs.ReadFile(staticFiles, name); err != nil || len(b) == 0 {
			t.Fatalf("%s not embedded: %v", name, err)
		}
	}
	_, ts := newServer(t, nil)
	for path, want := range map[string]string{"/": "text/html", "/app.js": "javascript", "/style.css": "text/css"} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !strings.Contains(resp.Header.Get("Content-Type"), want) {
			t.Fatalf("GET %s: %d %q", path, resp.StatusCode, resp.Header.Get("Content-Type"))
		}
		if csp := resp.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") {
			t.Fatalf("GET %s: CSP %q", path, csp)
		}
		if path == "/" && !strings.Contains(string(b), `<script src="app.js">`) {
			t.Fatalf("index.html doesn't load app.js:\n%s", b)
		}
	}
	// The UI makes no requests off this origin.
	for _, name := range []string{"static/index.html", "static/app.js", "static/style.css"} {
		b, _ := fs.ReadFile(staticFiles, name)
		if strings.Contains(string(b), "http://") || strings.Contains(string(b), "https://") {
			t.Fatalf("%s references an absolute URL", name)
		}
	}
}

func TestInfo(t *testing.T) {
	_, ts := newServer(t, nil)
	var info struct {
		ForkEnabled bool `json:"fork_enabled"`
		MaxN        int  `json:"max_n"`
	}
	do(t, ts, "GET", "/api/info", "", 200, &info)
	if info.ForkEnabled || info.MaxN != 20 {
		t.Fatalf("info = %+v", info)
	}
	wantError(t, ts, "GET", "/api/nope", "", 404)
}

func TestListRuns(t *testing.T) {
	_, ts := newServer(t, nil)
	var out struct{ Runs []runItem }
	do(t, ts, "GET", "/api/runs?since=7d&limit=10", "", 200, &out)
	if len(out.Runs) != 3 {
		t.Fatalf("got %d runs", len(out.Runs))
	}
	c1, r1, lg := out.Runs[0], out.Runs[1], out.Runs[2]
	if c1.ID != "c1" || !c1.Tracked || c1.ParentRunID != "r1" || c1.ParentStep != 1 || c1.Outcome != "pass" ||
		!strings.Contains(c1.ChangeLabel, "Never delete") {
		t.Fatalf("branch row = %+v", c1)
	}
	if r1.Branches != 5 || r1.Outcome != "fail" || r1.Status != "done" || r1.Errors != 1 || r1.CostUSD != 0.01 {
		t.Fatalf("parent row = %+v", r1)
	}
	if lg.Tracked || lg.Status != "" || lg.Branches != 0 {
		t.Fatalf("loadgen row = %+v", lg)
	}
	do(t, ts, "GET", "/api/runs?limit=1", "", 200, &out)
	if len(out.Runs) != 1 {
		t.Fatalf("limit=1 gave %d runs", len(out.Runs))
	}
	wantError(t, ts, "GET", "/api/runs?since=yesterday", "", 400)
	wantError(t, ts, "GET", "/api/runs?limit=0", "", 400)
}

func TestGetRun(t *testing.T) {
	_, ts := newServer(t, nil)
	var d runDetail
	do(t, ts, "GET", "/api/runs/r1", "", 200, &d)
	if d.Run == nil || d.Run.Outcome != "fail" || d.Run.Usage.LLMCalls != 2 || d.Run.FinalFiles == nil || *d.Run.FinalFiles != 3 ||
		d.Run.DurationMs == nil || !strings.Contains(string(d.Run.Task), "keep-fixture") {
		t.Fatalf("run = %+v", d.Run)
	}
	if d.Branches != 5 || d.ForkEnabled {
		t.Fatalf("branches %d fork_enabled %v", d.Branches, d.ForkEnabled)
	}
	if len(d.Steps) != 3 || !d.Steps[0].Forkable || d.Steps[0].Checkpoint == nil || d.Steps[1].Forkable ||
		d.Steps[2].Forkable || !d.Steps[2].Checkpoint.Deleted {
		t.Fatalf("steps = %+v", d.Steps)
	}
	tr := d.Trace
	if tr == nil || tr.Spans != 4 || len(tr.Roots) != 1 || tr.DurationMs != 1000 || tr.Totals.Errors != 1 {
		t.Fatalf("trace = %+v", tr)
	}
	root := tr.Roots[0]
	if root.Kind != "agent" || len(root.Children) != 3 {
		t.Fatalf("root = %+v", root)
	}
	llm, tool := root.Children[0], root.Children[1]
	if llm.SpanID != "s-llm1" || llm.Step != 1 || llm.CheckpointID != "cp_1" || !llm.Forkable || llm.OffsetMs != 10 ||
		llm.DurationMs != 100 || llm.TokensIn != 100 || llm.Payloads["furcate.llm.request"] != hashReq {
		t.Fatalf("llm span = %+v", llm)
	}
	if tool.Step != 2 || tool.Forkable || tool.Status != "error" || tool.StatusMessage != "tool call failed" || tool.OffsetMs != 120 {
		t.Fatalf("tool span = %+v", tool)
	}
	if gc := root.Children[2]; gc.Forkable || gc.CheckpointID != "cp_3" {
		t.Fatalf("gc'd span = %+v", gc)
	}

	// A trace with no run-store row still renders, without run or steps.
	do(t, ts, "GET", "/api/runs/lg1", "", 200, &d)
	if d.Run != nil || len(d.Steps) != 0 || d.Trace == nil || d.Trace.Spans != 1 {
		t.Fatalf("trace-only run = %+v", d)
	}
	// A run whose spans haven't arrived yet renders without a trace.
	do(t, ts, "GET", "/api/runs/c2", "", 200, &d)
	if d.Run == nil || d.Trace != nil || d.Run.ParentRunID != "r1" || d.Run.ChangeLabel == "" {
		t.Fatalf("spanless run = %+v", d)
	}
	if msg := wantError(t, ts, "GET", "/api/runs/nope", "", 404); !strings.Contains(msg, "nope") {
		t.Fatalf("404 message %q", msg)
	}
	wantError(t, ts, "GET", "/api/runs/bad%20id", "", 400)
}

func TestBranches(t *testing.T) {
	_, ts := newServer(t, nil)
	var out struct {
		Total  int
		Groups []branchGroup
	}
	do(t, ts, "GET", "/api/runs/r1/branches", "", 200, &out)
	if out.Total != 5 || len(out.Groups) != 2 {
		t.Fatalf("total %d, %d groups: %+v", out.Total, len(out.Groups), out.Groups)
	}
	var change, rerun branchGroup
	for _, g := range out.Groups {
		if g.Label == "no change (rerun)" {
			rerun = g
		} else {
			change = g
		}
	}
	lo, hi := stats.Wilson(2, 3, stats.Z95)
	if change.Step != 1 || change.Passes != 2 || change.Done != 3 || change.Errors != 1 || len(change.Runs) != 4 ||
		change.Lo != lo || change.Hi != hi || change.Rate < 0.66 || change.Rate > 0.67 ||
		!strings.Contains(change.Label, `append_system="Never delete fixture.txt."`) {
		t.Fatalf("change group = %+v", change)
	}
	if rerun.Running != 1 || rerun.Done != 0 || rerun.Lo != 0 || rerun.Hi != 1 {
		t.Fatalf("rerun group = %+v", rerun)
	}
	wantError(t, ts, "GET", "/api/runs/nope/branches", "", 404)
	wantError(t, ts, "GET", "/api/runs/lg1/branches", "", 404) // a trace, but not a run-store run
}

func TestPayload(t *testing.T) {
	_, ts := newServer(t, nil)
	resp, err := http.Get(ts.URL + "/api/payloads/" + hashReq)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") ||
		string(b) != `{"model":"claude-opus-5-5","messages":[]}` {
		t.Fatalf("payload: %d %q %s", resp.StatusCode, resp.Header.Get("Content-Type"), b)
	}
	wantError(t, ts, "GET", "/api/payloads/"+strings.Repeat("c", 64), "", 404)
	wantError(t, ts, "GET", "/api/payloads/NOT-HEX", "", 400)
}

func TestCompare(t *testing.T) {
	_, ts := newServer(t, nil)
	var out struct {
		A, B  runJSON
		Files filesJSON
	}
	do(t, ts, "GET", "/api/compare?a=r1&b=c1", "", 200, &out)
	f := out.Files
	if out.A.ID != "r1" || out.B.ID != "c1" || out.B.ParentRunID != "r1" || out.A.Usage.CostUSD != 0.02 || out.B.Outcome != "pass" {
		t.Fatalf("runs = %+v / %+v", out.A, out.B)
	}
	if !f.Available || f.A != 3 || f.B != 3 || f.Same != 1 || strings.Join(f.Changed, ",") != "b.txt" ||
		strings.Join(f.OnlyA, ",") != "gone.txt" || strings.Join(f.OnlyB, ",") != "new.txt" {
		t.Fatalf("files = %+v", f)
	}
	do(t, ts, "GET", "/api/compare?a=r1&b=c2", "", 200, &out) // c2 has no manifest
	if out.Files.Available || out.Files.Changed == nil {
		t.Fatalf("files without a manifest = %+v", out.Files)
	}
	wantError(t, ts, "GET", "/api/compare?a=r1", "", 400)
	if msg := wantError(t, ts, "GET", "/api/compare?a=r1&b=nope", "", 404); !strings.Contains(msg, "nope") {
		t.Fatalf("404 message %q", msg)
	}
}

func TestForkWithoutForker(t *testing.T) {
	_, ts := newServer(t, nil)
	msg := wantError(t, ts, "POST", "/api/runs/r1/fork", `{"step":1,"n":3}`, 501)
	if !strings.Contains(msg, "without a sandbox driver") {
		t.Fatalf("501 message %q", msg)
	}
}

func TestForkValidation(t *testing.T) {
	_, ts := newServer(t, &fakeForker{release: make(chan struct{})})
	for _, c := range []struct {
		path, body string
		status     int
	}{
		{"/api/runs/r1/fork", `{"step":1,"n":3`, 400},                         // malformed
		{"/api/runs/r1/fork", `{"step":1,"n":3,"chnage":{}}`, 400},            // unknown field
		{"/api/runs/r1/fork", `{"step":1,"n":3,"change":{"modle":"x"}}`, 400}, // unknown change field
		{"/api/runs/r1/fork", `{"step":1,"n":3} {}`, 400},                     // two objects
		{"/api/runs/r1/fork", `{"step":0,"n":3}`, 400},
		{"/api/runs/r1/fork", `{"step":1,"n":0}`, 400},
		{"/api/runs/r1/fork", `{"step":1,"n":21}`, 400}, // over MaxN
		{"/api/runs/r1/fork", `{"step":2,"n":3}`, 400},  // a tool step
		{"/api/runs/r1/fork", `{"step":9,"n":3}`, 404},  // no such step
		{"/api/runs/r1/fork", `{"step":3,"n":3}`, 409},  // checkpoint garbage-collected
		{"/api/runs/nope/fork", `{"step":1,"n":3}`, 404},
	} {
		wantError(t, ts, "POST", c.path, c.body, c.status)
	}

	// Not JSON: a cross-origin form post can't start a fork.
	resp, err := http.Post(ts.URL+"/api/runs/r1/fork", "text/plain", strings.NewReader(`{"step":1,"n":3}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("text/plain fork: %d", resp.StatusCode)
	}
	req, _ := http.NewRequest("POST", ts.URL+"/api/runs/r1/fork", strings.NewReader(`{"step":1,"n":3}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-site fork: %d", resp.StatusCode)
	}
}

func pollJob(t *testing.T, ts *httptest.Server, id string) jobJSON {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var j jobJSON
		do(t, ts, "GET", "/api/jobs/"+id, "", 200, &j)
		if j.Status != jobRunning || time.Now().After(deadline) {
			return j
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestForkJob(t *testing.T) {
	ff := &fakeForker{release: make(chan struct{})}
	_, ts := newServer(t, ff)
	var j jobJSON
	do(t, ts, "POST", "/api/runs/r1/fork", `{"step":1,"n":4,"change":{"append_system":"Never delete fixture.txt.","tool_output":""}}`, 202, &j)
	if j.ID == "" || j.Status != jobRunning || j.RunID != "r1" || j.Step != 1 || j.N != 4 || j.Result != nil {
		t.Fatalf("new job = %+v", j)
	}
	if j.Change.ToolOutput == nil || *j.Change.ToolOutput != "" || j.Change.AppendSystem == "" {
		t.Fatalf("change not passed through: %+v", j.Change)
	}
	var again jobJSON
	do(t, ts, "GET", "/api/jobs/"+j.ID, "", 200, &again)
	if again.Status != jobRunning {
		t.Fatalf("job before release = %+v", again)
	}
	close(ff.release)
	done := pollJob(t, ts, j.ID)
	r := done.Result
	if done.Status != jobDone || done.Finished == nil || r == nil || r.Passes != 2 || r.Done != 3 || r.Errors != 1 ||
		len(r.Branches) != 4 || r.Branches[3].Error != "sandbox died" || r.Usage.CostUSD != 0.3 {
		t.Fatalf("finished job = %+v / %+v", done, r)
	}
	ff.mu.Lock()
	got := ff.got[0]
	ff.mu.Unlock()
	if got.RunID != "r1" || got.Step != 1 || got.N != 4 || got.Change.AppendSystem != "Never delete fixture.txt." {
		t.Fatalf("forker got %+v", got)
	}
	wantError(t, ts, "GET", "/api/jobs/job_nope", "", 404)
}

func TestForkJobError(t *testing.T) {
	ff := &fakeForker{release: make(chan struct{}), err: errors.New("no sandbox slots")}
	close(ff.release)
	_, ts := newServer(t, ff)
	var j jobJSON
	do(t, ts, "POST", "/api/runs/r1/fork", `{"step":1,"n":2}`, 202, &j)
	if done := pollJob(t, ts, j.ID); done.Status != jobError || done.Error != "no sandbox slots" || done.Result != nil {
		t.Fatalf("failed job = %+v", done)
	}
}

func TestCloseCancelsForks(t *testing.T) {
	ff := &fakeForker{release: make(chan struct{})} // never released
	s, ts := newServer(t, ff)
	var j jobJSON
	do(t, ts, "POST", "/api/runs/r1/fork", `{"step":1,"n":2}`, 202, &j)
	s.Close() // returns only once the job has seen the cancellation
	s.mu.Lock()
	status, msg := s.jobs[j.ID].status, s.jobs[j.ID].err
	s.mu.Unlock()
	if status != jobError || !strings.Contains(msg, "canceled") {
		t.Fatalf("after Close: %s %q", status, msg)
	}
	wantError(t, ts, "POST", "/api/runs/r1/fork", `{"step":1,"n":2}`, 503)
}

func TestNilRunStore(t *testing.T) {
	traces, _ := fixture(t)
	ts := httptest.NewServer(New(Config{Traces: traces}))
	defer ts.Close()
	var out struct{ Runs []runItem }
	do(t, ts, "GET", "/api/runs", "", 200, &out)
	if len(out.Runs) != 3 || out.Runs[1].Tracked {
		t.Fatalf("runs without a run store = %+v", out.Runs)
	}
	var d runDetail
	do(t, ts, "GET", "/api/runs/r1", "", 200, &d)
	if d.Run != nil || d.Trace == nil || d.Trace.Roots[0].Children[0].Forkable {
		t.Fatalf("run without a run store = %+v", d)
	}
}

func TestJobEviction(t *testing.T) {
	traces, ms := fixture(t)
	ff := &fakeForker{release: make(chan struct{})}
	close(ff.release)
	s := New(Config{Traces: traces, Runs: ms, Forker: ff, MaxJobs: 2})
	defer s.Close()
	var ids []string
	for range 4 {
		j, err := s.startJob(replay.Request{RunID: "r1", Step: 1, N: 1})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.id)
		s.wg.Wait() // let it finish, so it can be evicted
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.jobs) != 2 || s.jobs[ids[0]] != nil || s.jobs[ids[1]] != nil || s.jobs[ids[3]] == nil {
		t.Fatalf("jobs after eviction: %d (%v)", len(s.jobs), s.order)
	}
}
