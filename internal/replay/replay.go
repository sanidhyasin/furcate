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
// change is append-only or touches only the newest message, so the history the
// model already reasoned over stays byte-identical and its earlier thinking
// blocks remain valid.
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
}

// IsZero reports a plain rerun from step k.
func (c Change) IsZero() bool { return c.Model == "" && c.AppendSystem == "" && c.ToolOutput == nil }

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
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " ")
}

// Apply edits the request for step k. Only the newest message is touched or
// added; the history before it keeps its bytes.
func (c Change) Apply(conv *agent.Conversation) error {
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

	sb := e.Controller.Driver
	insts, err := sb.Fork(ctx, driver.CheckpointID(cp.DriverRef), req.N, sb.Caps().OfflineForks)
	if err != nil {
		for _, inst := range insts {
			sb.Destroy(context.WithoutCancel(ctx), inst)
		}
		return nil, fmt.Errorf("fork checkpoint: %w", err)
	}

	res := &Result{Request: req, Branches: make([]controller.Result, len(insts))}
	var wg sync.WaitGroup
	for i, inst := range insts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each branch gets its own copy: branches append to their history.
			res.Branches[i] = e.Controller.Continue(ctx, t, inst, conv.Clone(), controller.Branch{
				ParentRunID: req.RunID, ParentStep: req.Step, Change: changeJSON, CheckpointID: step.CheckpointID,
			})
		}()
	}
	wg.Wait()

	for _, b := range res.Branches {
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
