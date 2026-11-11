package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sanidhyasin/furcate/internal/model"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/stats"
	"github.com/sanidhyasin/furcate/internal/store"
	"github.com/sanidhyasin/furcate/internal/tracetree"
)

// Span attributes the controller sets (internal/controller).
const (
	attrStepIndex    = "furcate.step.index"
	attrCheckpointID = "furcate.checkpoint_id"
)

// usageJSON is runstore.Usage.
type usageJSON struct {
	LLMCalls         int     `json:"llm_calls"`
	InputTokens      int64   `json:"input_tokens"`
	OutputTokens     int64   `json:"output_tokens"`
	CacheReadTokens  int64   `json:"cache_read_tokens"`
	CacheWriteTokens int64   `json:"cache_write_tokens"`
	CostUSD          float64 `json:"cost_usd"`
}

func toUsage(u runstore.Usage) usageJSON {
	return usageJSON{u.LLMCalls, u.InputTokens, u.OutputTokens, u.CacheReadTokens, u.CacheWriteTokens, u.CostUSD}
}

type evalJSON struct {
	Case    string `json:"case"`
	Variant string `json:"variant,omitempty"`
	Batch   string `json:"batch,omitempty"`
}

func toEval(e runstore.EvalTag) *evalJSON {
	if e == (runstore.EvalTag{}) {
		return nil
	}
	return &evalJSON{e.Case, e.Variant, e.Batch}
}

// runJSON is a run's row in the run store.
type runJSON struct {
	ID          string          `json:"id"`
	TaskID      string          `json:"task_id"`
	Driver      string          `json:"driver"`
	Status      string          `json:"status"`
	Outcome     string          `json:"outcome,omitempty"`
	Error       string          `json:"error,omitempty"`
	Started     time.Time       `json:"started"`
	Finished    *time.Time      `json:"finished,omitempty"`
	DurationMs  *float64        `json:"duration_ms,omitempty"`
	ParentRunID string          `json:"parent_run_id,omitempty"`
	ParentStep  int             `json:"parent_step,omitempty"`
	Change      json.RawMessage `json:"change,omitempty"`
	ChangeLabel string          `json:"change_label,omitempty"`
	Usage       usageJSON       `json:"usage"`
	FinalFiles  *int            `json:"final_files,omitempty"` // entries in the end-of-run manifest
	Eval        *evalJSON       `json:"eval,omitempty"`
	Task        json.RawMessage `json:"task,omitempty"`
}

func toRun(r runstore.Run, withTask bool) runJSON {
	out := runJSON{
		ID: r.RunID, TaskID: r.TaskID, Driver: r.Driver, Status: r.Status, Outcome: r.Outcome, Error: r.Error,
		Started: r.StartedAt, ParentRunID: r.ParentRunID, Usage: toUsage(r.Usage), Eval: toEval(r.Eval),
	}
	if !r.FinishedAt.IsZero() {
		f := r.FinishedAt
		d := ms(f.Sub(r.StartedAt))
		out.Finished, out.DurationMs = &f, &d
	}
	if r.ParentRunID != "" {
		out.ParentStep = r.ParentStep
		out.Change = rawJSON(r.Change)
		_, out.ChangeLabel = changeKey(r.Change)
	}
	if r.FinalFiles != nil {
		n := len(r.FinalFiles)
		out.FinalFiles = &n
	}
	if withTask {
		out.Task = rawJSON(r.Task)
	}
	return out
}

// rawJSON passes stored JSON through, or quotes it if it isn't valid JSON, so
// one bad row can't break a response.
func rawJSON(b []byte) json.RawMessage {
	if len(bytes.TrimSpace(b)) == 0 {
		return nil
	}
	if json.Valid(b) {
		return json.RawMessage(b)
	}
	q, _ := json.Marshal(string(b))
	return q
}

// changeKey canonicalises a stored change, so Postgres's reformatted JSONB and
// the bytes a fork wrote group together, and labels it for people.
func changeKey(raw []byte) (key, label string) {
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		raw = []byte("{}")
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw), string(raw)
	}
	canon, _ := json.Marshal(v) // map keys come out sorted
	key = string(canon)
	var c replay.Change
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&c) != nil {
		return key, key // a change this version doesn't know: show it as JSON
	}
	if c.IsZero() {
		return key, "no change (rerun)"
	}
	return key, c.String()
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func (s *Server) info(w http.ResponseWriter, r *http.Request) error {
	writeJSON(w, http.StatusOK, map[string]any{"fork_enabled": s.forker != nil, "max_n": s.maxN})
	return nil
}

// pathID reads {id} and keeps it to the characters run and job IDs use.
func pathID(r *http.Request) (string, error) {
	id := r.PathValue("id")
	if id == "" || len(id) > 128 || strings.IndexFunc(id, func(c rune) bool {
		return !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_.:", c))
	}) >= 0 {
		return "", errorf(http.StatusBadRequest, "bad id %q", id)
	}
	return id, nil
}

// runItem is one row of GET /api/runs: the trace summary from ClickHouse plus
// the run's state from the run store, if it has any (synthetic loadgen runs don't).
type runItem struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Service     string    `json:"service"`
	Start       time.Time `json:"start"`
	DurationMs  float64   `json:"duration_ms"`
	SpanStatus  string    `json:"span_status"`
	Spans       uint64    `json:"spans"`
	Errors      uint64    `json:"errors"`
	Tokens      uint64    `json:"tokens"`
	CostUSD     float64   `json:"cost_usd"`
	Tracked     bool      `json:"tracked"`
	TaskID      string    `json:"task_id,omitempty"`
	Driver      string    `json:"driver,omitempty"`
	Status      string    `json:"status,omitempty"`
	Outcome     string    `json:"outcome,omitempty"`
	ParentRunID string    `json:"parent_run_id,omitempty"`
	ParentStep  int       `json:"parent_step,omitempty"`
	ChangeLabel string    `json:"change_label,omitempty"`
	Branches    int       `json:"branches"`
	Eval        *evalJSON `json:"eval,omitempty"`
}

// parseSince reads a Go duration or a number of days ("7d").
func parseSince(v string) (time.Duration, error) {
	if days, ok := strings.CutSuffix(v, "d"); ok {
		n, err := strconv.Atoi(days)
		if err != nil || n <= 0 {
			return 0, errors.New("bad days")
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(v)
	if err == nil && d <= 0 {
		err = errors.New("not positive")
	}
	return d, err
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	since, limit := 7*24*time.Hour, 50
	if v := q.Get("since"); v != "" {
		d, err := parseSince(v)
		if err != nil {
			return errorf(http.StatusBadRequest, "bad since %q: want a duration like 24h or 7d", v)
		}
		since = d
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			return errorf(http.StatusBadRequest, "bad limit %q: want 1-1000", v)
		}
		limit = n
	}
	sums, err := s.traces.Runs(r.Context(), since, limit)
	if err != nil {
		return err
	}
	items := make([]runItem, len(sums))
	errs := make([]error, len(sums))
	parallel(len(sums), 8, func(i int) {
		sum := sums[i]
		it := runItem{
			ID: sum.TraceID, Name: sum.Name, Service: sum.Service, Start: sum.Start, DurationMs: ms(sum.Duration),
			SpanStatus: sum.Status, Spans: sum.Spans, Errors: sum.Errors, Tokens: sum.Tokens, CostUSD: sum.CostUSD,
		}
		errs[i] = s.enrich(r.Context(), &it)
		items[i] = it
	})
	if err := errors.Join(errs...); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": items, "since_hours": since.Hours(), "limit": limit})
	return nil
}

// enrich adds the run store's view of a run to its trace summary.
func (s *Server) enrich(ctx context.Context, it *runItem) error {
	run, err := s.runs.Run(ctx, it.ID)
	if errors.Is(err, runstore.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	kids, err := s.runs.Children(ctx, it.ID)
	if err != nil {
		return err
	}
	it.Tracked, it.Status, it.Outcome, it.Branches = true, run.Status, run.Outcome, len(kids)
	it.TaskID, it.Driver = run.TaskID, run.Driver
	it.ParentRunID, it.Eval = run.ParentRunID, toEval(run.Eval)
	if run.ParentRunID != "" {
		it.ParentStep = run.ParentStep
		_, it.ChangeLabel = changeKey(run.Change)
	}
	if it.CostUSD == 0 {
		it.CostUSD = run.Usage.CostUSD
	}
	return nil
}

// parallel runs f(0..n-1) on up to workers goroutines.
func parallel(n, workers int, f func(int)) {
	var wg sync.WaitGroup
	next := make(chan int)
	for range min(n, workers) {
		wg.Go(func() {
			for i := range next {
				f(i)
			}
		})
	}
	for i := range n {
		next <- i
	}
	close(next)
	wg.Wait()
}

type checkpointJSON struct {
	ID        string    `json:"id"`
	Driver    string    `json:"driver"`
	Level     int       `json:"level"`
	SizeBytes int64     `json:"size_bytes,omitempty"`
	Pinned    bool      `json:"pinned,omitempty"`
	Deleted   bool      `json:"deleted,omitempty"`
	Created   time.Time `json:"created"`
}

type stepJSON struct {
	Index        int             `json:"index"`
	Kind         string          `json:"kind"`
	SpanID       string          `json:"span_id"`
	CheckpointID string          `json:"checkpoint_id,omitempty"`
	Checkpoint   *checkpointJSON `json:"checkpoint,omitempty"`
	Forkable     bool            `json:"forkable"`
}

// spanJSON is one node of the trace tree. Offsets are from the trace's start.
type spanJSON struct {
	SpanID        string            `json:"span_id"`
	ParentSpanID  string            `json:"parent_span_id,omitempty"`
	Name          string            `json:"name"`
	Kind          string            `json:"kind"`
	Model         string            `json:"model,omitempty"`
	Service       string            `json:"service,omitempty"`
	OffsetMs      float64           `json:"offset_ms"`
	DurationMs    float64           `json:"duration_ms"`
	TokensIn      uint64            `json:"tokens_in"`
	TokensOut     uint64            `json:"tokens_out"`
	CostUSD       float64           `json:"cost_usd"`
	Status        string            `json:"status"`
	StatusMessage string            `json:"status_message,omitempty"`
	Step          int               `json:"step,omitempty"`
	CheckpointID  string            `json:"checkpoint_id,omitempty"`
	Forkable      bool              `json:"forkable,omitempty"`
	Payloads      map[string]string `json:"payloads,omitempty"` // attribute key -> sha256
	Attributes    map[string]string `json:"attributes,omitempty"`
	Orphan        bool              `json:"orphan,omitempty"`
	Children      []*spanJSON       `json:"children,omitempty"`
}

type totalsJSON struct {
	TokensIn  uint64  `json:"tokens_in"`
	TokensOut uint64  `json:"tokens_out"`
	CostUSD   float64 `json:"cost_usd"`
	Errors    int     `json:"errors"`
	LLMCalls  int     `json:"llm_calls"`
	ToolCalls int     `json:"tool_calls"`
}

type traceJSON struct {
	Start      time.Time   `json:"start"`
	DurationMs float64     `json:"duration_ms"`
	Spans      int         `json:"spans"`
	Totals     totalsJSON  `json:"totals"`
	Roots      []*spanJSON `json:"roots"`
}

type runDetail struct {
	ID          string     `json:"id"`
	Run         *runJSON   `json:"run"`   // null: the trace has no run-store row
	Steps       []stepJSON `json:"steps"` // empty without a run-store row
	Trace       *traceJSON `json:"trace"` // null: no spans in the trace store yet
	Branches    int        `json:"branches"`
	ForkEnabled bool       `json:"fork_enabled"`
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r)
	if err != nil {
		return err
	}
	ctx := r.Context()
	out := runDetail{ID: id, Steps: []stepJSON{}, ForkEnabled: s.forker != nil}

	run, err := s.runs.Run(ctx, id)
	tracked := err == nil
	if err != nil && !errors.Is(err, runstore.ErrNotFound) {
		return err
	}
	spans, err := s.traces.Trace(ctx, id, true)
	if err != nil && !errors.Is(err, store.ErrNoTrace) {
		return err
	}
	if !tracked && len(spans) == 0 {
		return errorf(http.StatusNotFound, "run %s not found", id)
	}

	bySpan := map[string]stepJSON{}
	if tracked {
		rj := toRun(run, true)
		out.Run = &rj
		if out.Steps, err = s.steps(ctx, id); err != nil {
			return err
		}
		for _, st := range out.Steps {
			bySpan[st.SpanID] = st
		}
		kids, err := s.runs.Children(ctx, id)
		if err != nil {
			return err
		}
		out.Branches = len(kids)
	}
	if len(spans) > 0 {
		out.Trace = buildTrace(spans, bySpan)
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// steps lists a run's steps with their checkpoints. A step can be forked if it
// is an LLM step whose checkpoint the driver still has.
func (s *Server) steps(ctx context.Context, runID string) ([]stepJSON, error) {
	steps, err := s.runs.Steps(ctx, runID)
	if err != nil {
		return nil, err
	}
	out := make([]stepJSON, 0, len(steps))
	for _, st := range steps {
		sj := stepJSON{Index: st.Index, Kind: st.Kind, SpanID: st.SpanID, CheckpointID: st.CheckpointID}
		if st.CheckpointID != "" {
			cp, err := s.runs.Checkpoint(ctx, st.CheckpointID)
			switch {
			case err == nil:
				sj.Checkpoint = &checkpointJSON{ID: cp.CheckpointID, Driver: cp.Driver, Level: cp.Level,
					SizeBytes: cp.SizeBytes, Pinned: cp.Pinned, Deleted: cp.Deleted, Created: cp.CreatedAt}
			case !errors.Is(err, runstore.ErrNotFound):
				return nil, err
			}
		}
		sj.Forkable = st.Kind == runstore.KindLLM && sj.Checkpoint != nil && !sj.Checkpoint.Deleted
		out = append(out, sj)
	}
	return out, nil
}

func buildTrace(spans []model.Span, bySpan map[string]stepJSON) *traceJSON {
	t := tracetree.Build(spans)
	tot := t.Totals()
	out := &traceJSON{
		Start: t.Start, DurationMs: ms(t.End.Sub(t.Start)), Spans: t.Spans,
		Totals: totalsJSON{tot.TokensIn, tot.TokensOut, tot.CostUSD, tot.Errors, tot.LLMCalls, tot.ToolCalls},
	}
	var conv func(n *tracetree.Node) *spanJSON
	conv = func(n *tracetree.Node) *spanJSON {
		sp := n.Span
		j := &spanJSON{
			SpanID: sp.SpanID, ParentSpanID: sp.ParentSpanID, Name: sp.Name, Kind: sp.Kind, Model: sp.Model,
			Service: sp.Service, OffsetMs: ms(sp.Start.Sub(t.Start)), DurationMs: ms(sp.Duration()),
			TokensIn: sp.TokensIn, TokensOut: sp.TokensOut, CostUSD: sp.CostUSD, Status: sp.Status,
			StatusMessage: sp.StatusMessage, Orphan: n.Orphan, CheckpointID: sp.Attributes[attrCheckpointID],
		}
		if len(sp.Payloads) > 0 {
			j.Payloads = sp.Payloads
		}
		if len(sp.Attributes) > 0 {
			j.Attributes = sp.Attributes
		}
		if k, err := strconv.Atoi(sp.Attributes[attrStepIndex]); err == nil {
			j.Step = k
		}
		if st, ok := bySpan[sp.SpanID]; ok {
			j.Step, j.Forkable = st.Index, st.Forkable
			if st.CheckpointID != "" {
				j.CheckpointID = st.CheckpointID
			}
		}
		for _, c := range n.Children {
			j.Children = append(j.Children, conv(c))
		}
		return j
	}
	out.Roots = make([]*spanJSON, 0, len(t.Roots))
	for _, n := range t.Roots {
		out.Roots = append(out.Roots, conv(n))
	}
	return out
}

type branchRun struct {
	ID         string    `json:"id"`
	Status     string    `json:"status"`
	Outcome    string    `json:"outcome,omitempty"`
	Error      string    `json:"error,omitempty"`
	CostUSD    float64   `json:"cost_usd"`
	DurationMs *float64  `json:"duration_ms,omitempty"`
	Started    time.Time `json:"started"`
	Eval       *evalJSON `json:"eval,omitempty"`
}

// branchGroup is every branch of a run from one step with one change, and the
// pass rate over the ones that finished, with a Wilson 95% interval.
type branchGroup struct {
	Step    int             `json:"step"`
	Change  json.RawMessage `json:"change"`
	Label   string          `json:"label"`
	Passes  int             `json:"passes"`
	Done    int             `json:"done"`
	Errors  int             `json:"errors"`
	Running int             `json:"running"`
	Rate    float64         `json:"rate"`
	Lo      float64         `json:"lo"`
	Hi      float64         `json:"hi"`
	Runs    []branchRun     `json:"runs"`
}

func (s *Server) branches(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r)
	if err != nil {
		return err
	}
	if _, err := s.runs.Run(r.Context(), id); errors.Is(err, runstore.ErrNotFound) {
		return errorf(http.StatusNotFound, "run %s not found in the run store", id)
	} else if err != nil {
		return err
	}
	kids, err := s.runs.Children(r.Context(), id)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{"run_id": id, "total": len(kids), "groups": groupBranches(kids)})
	return nil
}

func groupBranches(kids []runstore.Run) []*branchGroup {
	type key struct {
		step   int
		change string
	}
	groups := map[key]*branchGroup{}
	for _, k := range kids {
		ck, label := changeKey(k.Change)
		g := groups[key{k.ParentStep, ck}]
		if g == nil {
			g = &branchGroup{Step: k.ParentStep, Change: rawJSON([]byte(ck)), Label: label, Runs: []branchRun{}}
			groups[key{k.ParentStep, ck}] = g
		}
		br := branchRun{ID: k.RunID, Status: k.Status, Outcome: k.Outcome, Error: k.Error,
			CostUSD: k.Usage.CostUSD, Started: k.StartedAt, Eval: toEval(k.Eval)}
		if !k.FinishedAt.IsZero() {
			d := ms(k.FinishedAt.Sub(k.StartedAt))
			br.DurationMs = &d
		}
		g.Runs = append(g.Runs, br)
		switch k.Status {
		case runstore.StatusDone:
			g.Done++
			if k.Outcome == runstore.OutcomePass {
				g.Passes++
			}
		case runstore.StatusError:
			g.Errors++
		default:
			g.Running++
		}
	}
	out := slices.Collect(maps.Values(groups))
	for _, g := range out {
		if g.Done > 0 {
			g.Rate = float64(g.Passes) / float64(g.Done)
		}
		g.Lo, g.Hi = stats.Wilson(g.Passes, g.Done, stats.Z95)
	}
	slices.SortFunc(out, func(a, b *branchGroup) int {
		if a.Step != b.Step {
			return a.Step - b.Step
		}
		return strings.Compare(string(a.Change), string(b.Change))
	})
	return out
}

func (s *Server) payload(w http.ResponseWriter, r *http.Request) error {
	hash := r.PathValue("hash")
	if len(hash) == 0 || len(hash) > 128 || strings.Trim(hash, "0123456789abcdef") != "" {
		return errorf(http.StatusBadRequest, "bad payload hash %q: want lowercase hex sha256", hash)
	}
	data, err := s.traces.Payload(r.Context(), hash)
	if errors.Is(err, store.ErrNoPayload) {
		return errorf(http.StatusNotFound, "payload %s not found", hash)
	}
	if err != nil {
		return err
	}
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("Content-Length", strconv.Itoa(len(data)))
	h.Set("Cache-Control", "private, max-age=31536000, immutable") // content-addressed
	w.Write(data)
	return nil
}

// filesJSON compares two end-of-run manifests (path -> sha256).
type filesJSON struct {
	Available bool     `json:"available"` // both runs have a manifest
	A         int      `json:"a"`
	B         int      `json:"b"`
	Same      int      `json:"same"`
	Changed   []string `json:"changed"`
	OnlyA     []string `json:"only_a"`
	OnlyB     []string `json:"only_b"`
}

func diffFiles(a, b map[string]string) filesJSON {
	f := filesJSON{Available: a != nil && b != nil, A: len(a), B: len(b),
		Changed: []string{}, OnlyA: []string{}, OnlyB: []string{}}
	if !f.Available {
		return f
	}
	for _, p := range slices.Sorted(maps.Keys(a)) {
		hb, ok := b[p]
		switch {
		case !ok:
			f.OnlyA = append(f.OnlyA, p)
		case hb != a[p]:
			f.Changed = append(f.Changed, p)
		default:
			f.Same++
		}
	}
	for _, p := range slices.Sorted(maps.Keys(b)) {
		if _, ok := a[p]; !ok {
			f.OnlyB = append(f.OnlyB, p)
		}
	}
	return f
}

func (s *Server) compare(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	ids := [2]string{q.Get("a"), q.Get("b")}
	if ids[0] == "" || ids[1] == "" {
		return errorf(http.StatusBadRequest, "compare needs two runs: /api/compare?a=<run-id>&b=<run-id>")
	}
	var runs [2]runstore.Run
	for i, id := range ids {
		run, err := s.runs.Run(r.Context(), id)
		if errors.Is(err, runstore.ErrNotFound) {
			return errorf(http.StatusNotFound, "run %s not found in the run store", id)
		}
		if err != nil {
			return err
		}
		runs[i] = run
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"a": toRun(runs[0], false), "b": toRun(runs[1], false),
		"files": diffFiles(runs[0].FinalFiles, runs[1].FinalFiles),
	})
	return nil
}
