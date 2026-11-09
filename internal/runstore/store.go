// Package runstore keeps the state that changes: runs, their steps, and the
// checkpoints a driver took. Spans stay in ClickHouse; this is the index that
// maps a run's step numbers to its spans and checkpoints.
//
// A branch is a run with a parent: it points at the run and step it was forked
// from, and at the change it applied.
package runstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

// Run statuses and outcomes.
const (
	StatusRunning = "running"
	StatusDone    = "done"  // the agent finished and the check ran
	StatusError   = "error" // the run itself broke: driver, model or check failure

	OutcomePass = "pass"
	OutcomeFail = "fail"
)

// Step kinds.
const (
	KindLLM  = "llm"
	KindTool = "tool"
)

// Run is one agent run, which is also one trace: RunID is the trace ID.
type Run struct {
	RunID       string
	TaskID      string
	Task        json.RawMessage // the task spec the run used, frozen
	Driver      string
	ParentRunID string // "" for an original run
	ParentStep  int
	Change      json.RawMessage // what a branch changed; nil for an original run
	Status      string
	Outcome     string // "" until the check has run
	Error       string
	StartedAt   time.Time
	FinishedAt  time.Time
	Usage       Usage
	FinalFiles  map[string]string // path -> sha256 of the workdir when the run ended
	Eval        EvalTag           // set on runs an eval started
}

// EvalTag marks a run as one continuation of an eval case under one variant.
type EvalTag struct {
	Case, Variant, Batch string
}

// EvalCase is a checkpoint saved as a test: "resume here; does the agent finish?"
type EvalCase struct {
	CaseID       string
	RunID        string
	Step         int
	CheckpointID string
	Check        []string        // overrides the task's check; nil = the task's
	Change       json.RawMessage // applied under every variant; nil = none
	Notes        string
	CreatedAt    time.Time
}

// Usage is what a run's model calls consumed.
type Usage struct {
	LLMCalls         int
	InputTokens      int64 // uncached input
	OutputTokens     int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	CostUSD          float64
}

// Add sums two usages.
func (u Usage) Add(o Usage) Usage {
	return Usage{
		LLMCalls: u.LLMCalls + o.LLMCalls, InputTokens: u.InputTokens + o.InputTokens,
		OutputTokens: u.OutputTokens + o.OutputTokens, CacheReadTokens: u.CacheReadTokens + o.CacheReadTokens,
		CacheWriteTokens: u.CacheWriteTokens + o.CacheWriteTokens, CostUSD: u.CostUSD + o.CostUSD,
	}
}

// End is how a run finished.
type End struct {
	Status, Outcome, Error string
	Usage                  Usage
	FinalFiles             map[string]string
}

// Step is one LLM call or tool call. LLM steps carry the checkpoint taken just
// before the call: forking step k restores it and resends that call's request.
type Step struct {
	RunID        string
	Index        int
	SpanID       string
	Kind         string
	CheckpointID string // "" for tool steps
}

// Checkpoint is a saved sandbox state. The driver keeps the bytes; DriverRef is
// the driver's own ID for them.
type Checkpoint struct {
	CheckpointID string
	RunID        string
	Step         int
	Driver       string
	Level        int
	DriverRef    string
	SizeBytes    int64
	Pinned       bool // an eval case uses it; never expires
	ExpiresAt    time.Time
	CreatedAt    time.Time
}

// ErrNotFound is returned for an unknown run, step or checkpoint.
var ErrNotFound = errors.New("runstore: not found")

// Store is the run state.
type Store interface {
	CreateRun(ctx context.Context, r Run) error
	FinishRun(ctx context.Context, runID string, end End) error
	AddCheckpoint(ctx context.Context, c Checkpoint) error
	AddStep(ctx context.Context, s Step) error

	Run(ctx context.Context, runID string) (Run, error)
	Step(ctx context.Context, runID string, index int) (Step, error)
	Steps(ctx context.Context, runID string) ([]Step, error)
	Checkpoint(ctx context.Context, checkpointID string) (Checkpoint, error)
	Children(ctx context.Context, runID string) ([]Run, error)

	// PinCheckpoint marks a checkpoint as used by an eval case: never expired.
	PinCheckpoint(ctx context.Context, checkpointID string) error
	AddEvalCase(ctx context.Context, c EvalCase) error
	EvalCase(ctx context.Context, caseID string) (EvalCase, error)
	EvalCases(ctx context.Context) ([]EvalCase, error)
}

// Memory is an in-process Store, for tests and for evals run in CI.
type Memory struct {
	mu    sync.Mutex
	runs  map[string]Run
	steps map[string][]Step
	cps   map[string]Checkpoint
	cases map[string]EvalCase
}

// NewMemory returns an empty store.
func NewMemory() *Memory {
	return &Memory{runs: map[string]Run{}, steps: map[string][]Step{}, cps: map[string]Checkpoint{}, cases: map[string]EvalCase{}}
}

func (m *Memory) PinCheckpoint(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.cps[id]
	if !ok {
		return ErrNotFound
	}
	c.Pinned = true
	m.cps[id] = c
	return nil
}

func (m *Memory) AddEvalCase(_ context.Context, c EvalCase) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dup := m.cases[c.CaseID]; dup {
		return fmt.Errorf("eval case %s already exists", c.CaseID)
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now()
	}
	m.cases[c.CaseID] = c
	return nil
}

func (m *Memory) EvalCase(_ context.Context, id string) (EvalCase, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.cases[id]
	if !ok {
		return EvalCase{}, ErrNotFound
	}
	return c, nil
}

func (m *Memory) EvalCases(_ context.Context) ([]EvalCase, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []EvalCase
	for _, c := range m.cases {
		out = append(out, c)
	}
	slices.SortFunc(out, func(a, b EvalCase) int { return strings.Compare(a.CaseID, b.CaseID) })
	return out, nil
}

func (m *Memory) CreateRun(_ context.Context, r Run) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.StartedAt.IsZero() {
		r.StartedAt = time.Now()
	}
	if r.Status == "" {
		r.Status = StatusRunning
	}
	m.runs[r.RunID] = r
	return nil
}

func (m *Memory) FinishRun(_ context.Context, runID string, end End) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runs[runID]
	if !ok {
		return ErrNotFound
	}
	r.Status, r.Outcome, r.Error, r.FinishedAt = end.Status, end.Outcome, end.Error, time.Now()
	r.Usage, r.FinalFiles = end.Usage, end.FinalFiles
	m.runs[runID] = r
	return nil
}

func (m *Memory) AddCheckpoint(_ context.Context, c Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now()
	}
	m.cps[c.CheckpointID] = c
	return nil
}

func (m *Memory) AddStep(_ context.Context, s Step) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.steps[s.RunID] = append(m.steps[s.RunID], s)
	return nil
}

func (m *Memory) Run(_ context.Context, runID string) (Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runs[runID]
	if !ok {
		return Run{}, ErrNotFound
	}
	return r, nil
}

func (m *Memory) Step(_ context.Context, runID string, index int) (Step, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.steps[runID] {
		if s.Index == index {
			return s, nil
		}
	}
	return Step{}, ErrNotFound
}

func (m *Memory) Steps(_ context.Context, runID string) ([]Step, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := slices.Clone(m.steps[runID])
	slices.SortFunc(out, func(a, b Step) int { return a.Index - b.Index })
	return out, nil
}

func (m *Memory) Checkpoint(_ context.Context, id string) (Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.cps[id]
	if !ok {
		return Checkpoint{}, ErrNotFound
	}
	return c, nil
}

func (m *Memory) Children(_ context.Context, runID string) ([]Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Run
	for _, r := range m.runs {
		if r.ParentRunID == runID {
			out = append(out, r)
		}
	}
	slices.SortFunc(out, func(a, b Run) int { return a.StartedAt.Compare(b.StartedAt) })
	return out, nil
}
