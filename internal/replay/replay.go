// Package replay forks a recorded run. Given a run, a step k, a change and N, it
// restores step k's checkpoint into N sandboxes, rebuilds the conversation from
// the request recorded on step k's span, applies the change, and runs the N
// continuations in parallel. Each continuation is a new run linked to its parent.
package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/anthropics/anthropic-sdk-go/v2"

	"github.com/sanidhyasin/furcate/internal/agent"
	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/driver"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/stats"
	"github.com/sanidhyasin/furcate/internal/task"
)

// Conversations returns the request body recorded on an LLM step's span.
type Conversations interface {
	LLMRequest(ctx context.Context, runID, spanID string) ([]byte, error)
}

// Change is what a branch does differently from its parent at step k. Every
// change but System is append-only or touches only the newest message, so the
// history the model already reasoned over stays byte-identical and its earlier
// thinking blocks remain valid.
type Change struct {
	// Model replaces the model for the rest of the branch. Thinking blocks from
	// another model are dropped by the API, unbilled.
	Model string `json:"model,omitempty"`
	// AppendSystem adds an operator instruction as a mid-conversation system
	// message after the latest user turn, instead of editing the top-level system
	// prompt (which would invalidate every earlier thinking block).
	AppendSystem string `json:"append_system,omitempty"`
	// ToolOutput replaces the output of the last tool call before step k.
	ToolOutput *string `json:"tool_output,omitempty"`
	// System replaces the top-level system prompt, as a PR that edits an agent's
	// prompt does. Unlike the other changes this edits history, so the branch
	// drops the earlier thinking blocks (the API would reject them) and starts
	// with a cold prompt cache.
	System *string `json:"system,omitempty"`
}

// IsZero reports a plain rerun from step k.
func (c Change) IsZero() bool {
	return c.Model == "" && c.AppendSystem == "" && c.ToolOutput == nil && c.System == nil
}

func (c Change) String() string {
	var parts []string
	if c.Model != "" {
		parts = append(parts, "model="+c.Model)
	}
	if c.AppendSystem != "" {
		parts = append(parts, fmt.Sprintf("append_system=%q", c.AppendSystem))
	}
	if c.ToolOutput != nil {
		parts = append(parts, fmt.Sprintf("tool_output=%q", *c.ToolOutput))
	}
	if c.System != nil {
		s := *c.System
		if len(s) > 40 {
			s = s[:40] + "..."
		}
		parts = append(parts, fmt.Sprintf("system=%q", s))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " ")
}

// Apply edits the request for step k. Only the newest message is touched or
// added; the history before it keeps its bytes.
func (c Change) Apply(conv *agent.Conversation) error {
	if c.System != nil {
		conv.SetSystem(*c.System)
		if err := conv.StripThinking(); err != nil {
			return err
		}
	}
	if c.ToolOutput != nil {
		if err := conv.ReplaceLastToolResult(*c.ToolOutput); err != nil {
			return err
		}
	}
	if c.AppendSystem != "" {
		if err := conv.Append(anthropic.MessageParam{OfSystem: &anthropic.SystemMessageParam{
			Content: []anthropic.TextBlockParam{{Text: c.AppendSystem}},
		}}); err != nil {
			return err
		}
	}
	if c.Model != "" {
		conv.SetModel(c.Model)
	}
	return nil
}

// Engine forks runs.
type Engine struct {
	Controller    *controller.Controller
	Store         runstore.Store
	Conversations Conversations
	// RequestWait is how long to wait for step k's span to reach the trace store,
	// for a fork started right after its parent finished. Default 30s.
	RequestWait time.Duration
}

// Request is one fork.
type Request struct {
	RunID  string
	Step   int
	N      int
	Change Change
}

// Result summarises the branches.
type Result struct {
	Request
	Branches []controller.Result
	Passes   int
	Done     int // branches that finished without error
	Lo, Hi   float64
	Usage    runstore.Usage // all branches together
}

// Fork runs N continuations of RunID from Step and waits for all of them.
func (e *Engine) Fork(ctx context.Context, req Request) (*Result, error) {
	if req.N <= 0 {
		return nil, errors.New("n must be at least 1")
	}
	run, err := e.Store.Run(ctx, req.RunID)
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", req.RunID, err)
	}
	t, err := task.Parse(run.Task)
	if err != nil {
		return nil, err
	}
	step, err := e.Store.Step(ctx, req.RunID, req.Step)
	if err != nil {
		return nil, fmt.Errorf("step %d: %w", req.Step, err)
	}
	if step.Kind != runstore.KindLLM || step.CheckpointID == "" {
		return nil, fmt.Errorf("step %d is a %s step without a checkpoint; fork from an LLM step (furcate steps %s)",
			req.Step, step.Kind, req.RunID)
	}
	cp, err := e.Store.Checkpoint(ctx, step.CheckpointID)
	if err != nil {
		return nil, fmt.Errorf("checkpoint %s: %w", step.CheckpointID, err)
	}
	if cp.Deleted {
		return nil, fmt.Errorf("step %d's checkpoint %s was garbage-collected; pin checkpoints you'll fork later with furcate eval add",
			req.Step, step.CheckpointID)
	}
	body, err := e.request(ctx, req.RunID, step.SpanID)
	if err != nil {
		return nil, fmt.Errorf("conversation at step %d: %w", req.Step, err)
	}
	// Apply the change once, before starting any sandbox, so a bad one fails early.
	conv, err := agent.ParseConversation(body)
	if err != nil {
		return nil, fmt.Errorf("decode recorded request: %w", err)
	}
	if err := req.Change.Apply(conv); err != nil {
		return nil, err
	}
	changeJSON, _ := json.Marshal(req.Change)
	res, err := e.Run(ctx, Spec{
		Task: t, Checkpoint: driver.CheckpointID(cp.DriverRef), Conversation: conv, N: req.N,
		Branch: controller.Branch{ParentRunID: req.RunID, ParentStep: req.Step, Change: changeJSON, CheckpointID: step.CheckpointID},
	})
	if err != nil {
		return nil, err
	}
	res.Request = req
	return res, nil
}

// Spec is one fork without the run store's history: what evals use when a case
// comes from a bundle rather than from a recorded run.
type Spec struct {
	Task         *task.Task
	Checkpoint   driver.CheckpointID // the driver's ref
	Conversation *agent.Conversation // the request for the first step, change applied
	N            int
	Branch       controller.Branch // parent, step, change, eval tags; copied to every branch
}

// Run forks spec.Checkpoint into N sandboxes and runs the continuations,
// admitting branches as sandbox slots free up, a chunk at a time, each chunk
// forked in one driver call.
func (e *Engine) Run(ctx context.Context, spec Spec) (*Result, error) {
	sb := e.Controller.Driver
	offline := sb.Caps().OfflineForks
	res := &Result{Branches: make([]controller.Result, spec.N)}
	var wg sync.WaitGroup
	for next := 0; next < spec.N; {
		k, release, err := e.Controller.Slots.AcquireUpTo(ctx, spec.N-next)
		if err != nil {
			wg.Wait()
			return nil, err
		}
		insts, err := sb.Fork(ctx, spec.Checkpoint, k, offline)
		if err != nil {
			for _, inst := range insts {
				sb.Destroy(context.WithoutCancel(ctx), inst)
			}
			release(k)
			if next == 0 {
				return nil, fmt.Errorf("fork checkpoint: %w", err)
			}
			// Keep the branches already running; report the rest as failed.
			for i := next; i < spec.N; i++ {
				res.Branches[i] = controller.Result{Status: runstore.StatusError, Err: fmt.Errorf("fork checkpoint: %w", err)}
			}
			break
		}
		release(k - len(insts))
		for _, inst := range insts {
			i := next
			next++
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer release(1)
				// Each branch gets its own copy: branches append to their history.
				res.Branches[i] = e.Controller.Continue(ctx, spec.Task, inst, spec.Conversation.Clone(), spec.Branch)
			}()
		}
	}
	wg.Wait()

	for _, b := range res.Branches {
		res.Usage = res.Usage.Add(b.Usage)
		if b.Status == runstore.StatusDone {
			res.Done++
			if b.Outcome == runstore.OutcomePass {
				res.Passes++
			}
		}
	}
	res.Lo, res.Hi = stats.Wilson(res.Passes, res.Done, stats.Z95)
	return res, nil
}

// Request returns the request body recorded on an LLM step's span, waiting for
// it to reach the trace store.
func (e *Engine) Request(ctx context.Context, runID, spanID string) ([]byte, error) {
	return e.request(ctx, runID, spanID)
}

// request waits for the recorded request to reach the trace store.
func (e *Engine) request(ctx context.Context, runID, spanID string) ([]byte, error) {
	wait := e.RequestWait
	if wait <= 0 {
		wait = 30 * time.Second
	}
	deadline := time.Now().Add(wait)
	for {
		body, err := e.Conversations.LLMRequest(ctx, runID, spanID)
		if err == nil {
			return body, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
