// Package evals turns checkpoints into repeatable tests. An eval case says
// "resume here; does the agent finish?": a task, a step, the request the model
// was sent at that step, a success check and the files of the sandbox at that
// moment.
//
// A case is exported as a bundle directory, so it can be committed next to the
// agent it tests and run anywhere a driver exists, without the stores it was
// recorded in:
//
//	case.json         task spec, step, check, change, where it came from
//	request.json      the recorded request at that step, byte for byte
//	workdir.tar.gz    the task's working directory at that checkpoint
//	start.tar.gz      the task's files as the run started (if it had any)
//
// Restoring a bundle creates a sandbox from the task's template, unpacks the
// starting files and reruns the task's setup on them, as the original run did,
// so whatever setup records outside the workdir (a check's hashes of the
// fixtures, say) matches the original. Then it swaps in the checkpoint's workdir
// and checkpoints the result. That carries the files (driver level 2) but not
// running processes, whichever driver runs it.
package evals

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sanidhyasin/furcate/internal/agent"
	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/driver"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/stats"
	"github.com/sanidhyasin/furcate/internal/task"
)

// Bundle is one exported eval case.
type Bundle struct {
	ID     string          `json:"id"`
	Notes  string          `json:"notes,omitempty"`
	Task   json.RawMessage `json:"task"`
	Step   int             `json:"step"`
	Check  []string        `json:"check,omitempty"` // overrides the task's check
	Change *replay.Change  `json:"change,omitempty"`
	Source Source          `json:"source"`

	Request []byte `json:"-"` // request.json, kept verbatim
	Files   []byte `json:"-"` // workdir.tar.gz
	Start   []byte `json:"-"` // start.tar.gz; nil when the task had no files
}

// Source records where a case was cut from.
type Source struct {
	RunID        string    `json:"run_id"`
	CheckpointID string    `json:"checkpoint_id"`
	Driver       string    `json:"driver"`
	Exported     time.Time `json:"exported"`
}

const (
	caseFile    = "case.json"
	requestFile = "request.json"
	filesFile   = "workdir.tar.gz"
	startFile   = "start.tar.gz"
)

// Write saves the bundle into dir.
func (b *Bundle) Write(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	meta, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	files := map[string][]byte{caseFile: append(meta, '\n'), requestFile: b.Request, filesFile: b.Files}
	if b.Start != nil {
		files[startFile] = b.Start
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// Load reads one bundle directory.
func Load(dir string) (*Bundle, error) {
	meta, err := os.ReadFile(filepath.Join(dir, caseFile))
	if err != nil {
		return nil, err
	}
	var b Bundle
	if err := json.Unmarshal(meta, &b); err != nil {
		return nil, fmt.Errorf("%s: %w", dir, err)
	}
	if b.Request, err = os.ReadFile(filepath.Join(dir, requestFile)); err != nil {
		return nil, err
	}
	if b.Files, err = os.ReadFile(filepath.Join(dir, filesFile)); err != nil {
		return nil, err
	}
	// Bundles exported before start.tar.gz existed rerun setup without files.
	if b.Start, err = os.ReadFile(filepath.Join(dir, startFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return &b, nil
}

// LoadAll reads every bundle under root (each subdirectory with a case.json),
// keeping only ids if any are given.
func LoadAll(root string, ids ...string) ([]*Bundle, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var out []*Bundle
	for _, e := range entries {
		dir := filepath.Join(root, e.Name())
		if _, err := os.Stat(filepath.Join(dir, caseFile)); err != nil {
			continue
		}
		b, err := Load(dir)
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 || slices.Contains(ids, b.ID) {
			out = append(out, b)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no eval cases under %s", root)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Add records a case from a run's step and pins its checkpoint.
func Add(ctx context.Context, store runstore.Store, c runstore.EvalCase) error {
	step, err := store.Step(ctx, c.RunID, c.Step)
	if err != nil {
		return fmt.Errorf("step %d: %w", c.Step, err)
	}
	if step.Kind != runstore.KindLLM || step.CheckpointID == "" {
		return fmt.Errorf("step %d has no checkpoint; pick an LLM step", c.Step)
	}
	c.CheckpointID = step.CheckpointID
	if err := store.AddEvalCase(ctx, c); err != nil {
		return err
	}
	return store.PinCheckpoint(ctx, c.CheckpointID)
}

// Export cuts a bundle from a recorded case: the request from the trace store
// and the files from the pinned checkpoint.
func Export(ctx context.Context, eng *replay.Engine, caseID string) (*Bundle, error) {
	st := eng.Store
	c, err := st.EvalCase(ctx, caseID)
	if err != nil {
		return nil, fmt.Errorf("eval case %s: %w", caseID, err)
	}
	run, err := st.Run(ctx, c.RunID)
	if err != nil {
		return nil, err
	}
	t, err := task.Parse(run.Task)
	if err != nil {
		return nil, err
	}
	step, err := st.Step(ctx, c.RunID, c.Step)
	if err != nil {
		return nil, err
	}
	cp, err := st.Checkpoint(ctx, c.CheckpointID)
	if err != nil {
		return nil, err
	}
	body, err := eng.Request(ctx, c.RunID, step.SpanID)
	if err != nil {
		return nil, fmt.Errorf("request at step %d: %w", c.Step, err)
	}

	sb := eng.Controller.Driver
	insts, err := sb.Fork(ctx, driver.CheckpointID(cp.DriverRef), 1, false)
	if err != nil {
		return nil, fmt.Errorf("open checkpoint: %w", err)
	}
	defer sb.Destroy(context.WithoutCancel(ctx), insts[0])
	res, err := sb.Exec(ctx, insts[0], driver.ExecRequest{Args: []string{"tar", "czf", "-", "."}, Dir: t.Workdir, Timeout: 5 * time.Minute})
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("tar the workdir: exit %d: %s", res.ExitCode, res.Stderr)
	}

	// Setup reruns on the task's starting files, so the bundle carries them, and
	// not the local directory they came from, which means nothing elsewhere.
	start, err := t.Archive()
	if err != nil {
		return nil, fmt.Errorf("the task's files: %w", err)
	}
	t.Files = ""
	spec, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	b := &Bundle{ID: c.CaseID, Notes: c.Notes, Task: spec, Step: c.Step, Check: c.Check,
		Source:  Source{RunID: c.RunID, CheckpointID: c.CheckpointID, Driver: run.Driver, Exported: time.Now().UTC()},
		Request: body, Files: res.Stdout, Start: start}
	if len(c.Change) > 0 {
		var ch replay.Change
		if err := json.Unmarshal(c.Change, &ch); err != nil {
			return nil, err
		}
		b.Change = &ch
	}
	return b, nil
}

// Restored is a bundle turned back into a checkpoint on the current driver.
type Restored struct {
	RunID        string // a stand-in parent run for the case's branches
	CheckpointID string
	Ref          driver.CheckpointID
	Task         *task.Task
}

// Restore rebuilds a bundle's checkpoint: a sandbox from the task's template,
// the task's setup, then the bundled files.
func Restore(ctx context.Context, ctl *controller.Controller, b *Bundle, batch string) (*Restored, error) {
	t, err := task.Parse(b.Task)
	if err != nil {
		return nil, err
	}
	if b.Check != nil {
		t.Check = b.Check
	}
	r := &Restored{RunID: "case-" + b.ID + "-" + batch, Task: t}
	if err := ctl.Store.CreateRun(ctx, runstore.Run{RunID: r.RunID, TaskID: t.ID, Task: t.JSON(),
		Driver: ctl.Driver.Caps().Name, Eval: runstore.EvalTag{Case: b.ID, Variant: "restore", Batch: batch}}); err != nil {
		return nil, err
	}
	fail := func(err error) (*Restored, error) {
		ctl.Store.FinishRun(context.WithoutCancel(ctx), r.RunID, runstore.End{Status: runstore.StatusError, Error: err.Error()})
		return nil, err
	}
	release, err := ctl.Slots.Acquire(ctx, 1)
	if err != nil {
		return fail(err)
	}
	defer release(1)

	sb := ctl.Driver
	inst, err := sb.Create(ctx, t.Template)
	if err != nil {
		return fail(fmt.Errorf("create sandbox: %w", err))
	}
	defer sb.Destroy(context.WithoutCancel(ctx), inst)
	exec := func(req driver.ExecRequest) error {
		res, err := sb.Exec(ctx, inst, req)
		if err == nil && res.ExitCode != 0 {
			err = fmt.Errorf("%q: exit %d: %s%s", req.Args, res.ExitCode, res.Stdout, res.Stderr)
		}
		return err
	}
	// Setup runs on the starting files, as in the original run; then the workdir
	// becomes the checkpoint's.
	if err := controller.PrepareFiles(ctx, sb, inst, t, b.Start); err != nil {
		return fail(err)
	}
	if b.Start != nil && t.Workdir != "" && t.Workdir != "/" {
		if err := exec(driver.ExecRequest{Args: []string{"find", ".", "-mindepth", "1", "-delete"}, Dir: t.Workdir, Timeout: time.Minute}); err != nil {
			return fail(fmt.Errorf("clear the starting files: %w", err))
		}
	}
	if err := exec(driver.ExecRequest{Args: []string{"tar", "xzf", "-"}, Stdin: b.Files, Dir: t.Workdir, Timeout: 5 * time.Minute}); err != nil {
		return fail(fmt.Errorf("unpack files: %w", err))
	}
	if r.Ref, err = sb.Checkpoint(ctx, inst); err != nil {
		return fail(fmt.Errorf("checkpoint: %w", err))
	}
	id := make([]byte, 8)
	rand.Read(id)
	r.CheckpointID = "cp_" + hex.EncodeToString(id)
	caps := sb.Caps()
	if err := ctl.Store.AddCheckpoint(ctx, runstore.Checkpoint{CheckpointID: r.CheckpointID, RunID: r.RunID, Step: b.Step,
		Driver: caps.Name, Level: int(caps.Level), DriverRef: string(r.Ref), Pinned: true}); err != nil {
		return fail(err)
	}
	ctl.Store.FinishRun(ctx, r.RunID, runstore.End{Status: runstore.StatusDone})
	return r, nil
}

// Variant is one way to run every case: a name and a change.
type Variant struct {
	Name   string
	Change replay.Change
}

// Cell is one case under one variant.
type Cell struct {
	Case, Variant        string
	Passes, Done, Errors int
	Lo, Hi               float64
	Usage                runstore.Usage
	Err                  error
}

// Report is one eval batch.
type Report struct {
	Batch    string
	N        int
	Variants []string
	Cases    []string
	Cells    map[[2]string]*Cell // [case, variant]
	Usage    runstore.Usage
	Duration time.Duration
}

// Total sums a variant's cells.
func (r *Report) Total(variant string) *Cell {
	t := &Cell{Variant: variant}
	for _, c := range r.Cases {
		if cell := r.Cells[[2]string{c, variant}]; cell != nil {
			t.Passes += cell.Passes
			t.Done += cell.Done
			t.Errors += cell.Errors
			t.Usage = t.Usage.Add(cell.Usage)
		}
	}
	t.Lo, t.Hi = stats.Wilson(t.Passes, t.Done, stats.Z95)
	return t
}

// NewBatchID names an eval batch.
func NewBatchID() string {
	b := make([]byte, 4)
	rand.Read(b)
	return time.Now().UTC().Format("20060102-150405") + "-" + hex.EncodeToString(b)
}

// RunSuite runs every bundle under every variant, n continuations each. Cases
// run one after another; a case's variants run at once, bounded by the
// controller's sandbox slots.
func RunSuite(ctx context.Context, eng *replay.Engine, bundles []*Bundle, variants []Variant, n int, batch string,
	logf func(string, ...any)) (*Report, error) {

	if len(variants) == 0 {
		return nil, errors.New("no variants")
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	start := time.Now()
	rep := &Report{Batch: batch, N: n, Cells: map[[2]string]*Cell{}}
	for _, v := range variants {
		rep.Variants = append(rep.Variants, v.Name)
	}
	for _, b := range bundles {
		rep.Cases = append(rep.Cases, b.ID)
		logf("case %s: restoring step %d", b.ID, b.Step)
		r, err := Restore(ctx, eng.Controller, b, batch)
		if err != nil {
			for _, v := range variants {
				rep.Cells[[2]string{b.ID, v.Name}] = &Cell{Case: b.ID, Variant: v.Name, Errors: n, Err: err}
			}
			logf("case %s: %v", b.ID, err)
			continue
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		for _, v := range variants {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cell := runCell(ctx, eng, b, r, v, n, batch)
				mu.Lock()
				rep.Cells[[2]string{b.ID, v.Name}] = cell
				rep.Usage = rep.Usage.Add(cell.Usage)
				mu.Unlock()
				logf("case %s, %s: %d/%d pass (%.2f-%.2f)", b.ID, v.Name, cell.Passes, cell.Done, cell.Lo, cell.Hi)
			}()
		}
		wg.Wait()
		eng.Controller.Driver.DeleteCheckpoint(context.WithoutCancel(ctx), r.Ref)
	}
	rep.Duration = time.Since(start)
	return rep, nil
}

func runCell(ctx context.Context, eng *replay.Engine, b *Bundle, r *Restored, v Variant, n int, batch string) *Cell {
	cell := &Cell{Case: b.ID, Variant: v.Name}
	conv, err := agent.ParseConversation(b.Request)
	if err == nil && b.Change != nil {
		err = b.Change.Apply(conv)
	}
	if err == nil {
		err = v.Change.Apply(conv)
	}
	if err != nil {
		cell.Err, cell.Errors = err, n
		return cell
	}
	change, _ := json.Marshal(v.Change)
	res, err := eng.Run(ctx, replay.Spec{Task: r.Task, Checkpoint: r.Ref, Conversation: conv, N: n,
		Branch: controller.Branch{ParentRunID: r.RunID, ParentStep: b.Step, Change: change, CheckpointID: r.CheckpointID,
			Eval: runstore.EvalTag{Case: b.ID, Variant: v.Name, Batch: batch}}})
	if err != nil {
		cell.Err, cell.Errors = err, n
		return cell
	}
	cell.Passes, cell.Done, cell.Lo, cell.Hi, cell.Usage = res.Passes, res.Done, res.Lo, res.Hi, res.Usage
	cell.Errors = len(res.Branches) - res.Done
	for _, br := range res.Branches {
		if br.Err != nil && cell.Err == nil {
			cell.Err = br.Err
		}
	}
	return cell
}

// Difference is variant b's pass rate minus variant a's, with Newcombe's 95%
// interval (built from the two Wilson intervals). Cases are pooled.
func Difference(a, b *Cell) (d, lo, hi float64) {
	if a.Done == 0 || b.Done == 0 {
		return 0, -1, 1
	}
	pa, pb := float64(a.Passes)/float64(a.Done), float64(b.Passes)/float64(b.Done)
	d = pb - pa
	lo = d - math.Sqrt(math.Pow(pb-b.Lo, 2)+math.Pow(a.Hi-pa, 2))
	hi = d + math.Sqrt(math.Pow(b.Hi-pb, 2)+math.Pow(pa-a.Lo, 2))
	return d, lo, hi
}

// Verdict reads a difference interval.
func Verdict(lo, hi float64) string {
	switch {
	case lo > 0:
		return "better"
	case hi < 0:
		return "worse"
	}
	return "no clear change"
}

func fmtCell(c *Cell) string {
	if c == nil {
		return "-"
	}
	if c.Done == 0 {
		return fmt.Sprintf("error (%d)", c.Errors)
	}
	s := fmt.Sprintf("%d/%d (%.2f-%.2f)", c.Passes, c.Done, c.Lo, c.Hi)
	if c.Errors > 0 {
		s += fmt.Sprintf(", %d errors", c.Errors)
	}
	return s
}

// Markdown renders the report for a pull-request comment. The first variant is
// the baseline.
func (r *Report) Markdown() string {
	var b strings.Builder
	base := r.Variants[0]
	fmt.Fprintf(&b, "### Furcate eval\n\n%d cases, %d continuations per case and variant, batch `%s`.\n\n",
		len(r.Cases), r.N, r.Batch)
	b.WriteString("| case |")
	for _, v := range r.Variants {
		fmt.Fprintf(&b, " %s |", v)
	}
	b.WriteString("\n|---|")
	for range r.Variants {
		b.WriteString("---|")
	}
	b.WriteString("\n")
	for _, c := range r.Cases {
		fmt.Fprintf(&b, "| `%s` |", c)
		for _, v := range r.Variants {
			fmt.Fprintf(&b, " %s |", fmtCell(r.Cells[[2]string{c, v}]))
		}
		b.WriteString("\n")
	}
	b.WriteString("| **all** |")
	for _, v := range r.Variants {
		fmt.Fprintf(&b, " **%s** |", fmtCell(r.Total(v)))
	}
	b.WriteString("\n\n")
	for _, v := range r.Variants[1:] {
		d, lo, hi := Difference(r.Total(base), r.Total(v))
		fmt.Fprintf(&b, "**%s vs %s: %+.0f points** (95%% CI %+.0f to %+.0f): %s.\n\n", v, base, d*100, lo*100, hi*100, Verdict(lo, hi))
	}
	fmt.Fprintf(&b, "Pass rates are Wilson 95%% intervals. Cost $%.2f, %d LLM calls, %s.\n",
		r.Usage.CostUSD, r.Usage.LLMCalls, r.Duration.Round(time.Second))
	return b.String()
}

// Text renders the report for a terminal.
func (r *Report) Text() string {
	var b bytes.Buffer
	md := r.Markdown()
	md = strings.ReplaceAll(md, "**", "")
	md = strings.ReplaceAll(md, "`", "")
	b.WriteString(strings.TrimPrefix(md, "### "))
	return b.String()
}
