// Package controller runs agent runs. It drives the agent loop on the host,
// executes tools in a sandbox through a driver, takes a checkpoint before every
// LLM call, and records each step as a span (to the collector) and a row (in the
// run store).
//
// A step is one LLM call or one tool call, numbered from 1 across the run. LLM
// steps carry the checkpoint taken just before the call, and their span carries
// the call's full request body, so forking step k means: restore that checkpoint,
// resend that request.
package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/anthropics/anthropic-sdk-go/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanidhyasin/furcate/internal/agent"
	"github.com/sanidhyasin/furcate/internal/driver"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/task"
)

// Span attributes the collector and the replay engine read.
const (
	AttrRunID        = "furcate.run_id"
	AttrParentRunID  = "furcate.parent_run_id"
	AttrParentStep   = "furcate.parent_step"
	AttrChange       = "furcate.change"
	AttrTaskID       = "furcate.task_id"
	AttrOutcome      = "furcate.outcome"
	AttrStepIndex    = "furcate.step.index"
	AttrStepKind     = "furcate.step.kind"
	AttrCheckpointID = "furcate.checkpoint_id"
	AttrCheckpointMs = "furcate.checkpoint_ms"
	AttrLLMRequest   = "furcate.llm.request"
	AttrLLMResponse  = "furcate.llm.response"
	AttrDriver       = "furcate.driver"
)

// MaxTokens per model call. Requests are not streamed, so this stays at a size
// that finishes well inside the SDK's HTTP timeout.
const MaxTokens = 16000

// Controller runs agents. It holds no per-run state, so one Controller can run
// many branches at once.
type Controller struct {
	Driver driver.Sandbox
	Model  agent.Model
	Store  runstore.Store
	Tracer trace.Tracer
	Log    *slog.Logger
	Keep   bool // leave instances running after a run, for inspection
}

// Result is one finished run.
type Result struct {
	RunID   string
	Status  string // runstore.StatusDone or StatusError
	Outcome string // pass, fail, or "" on error
	Steps   int    // last step index
	Err     error
}

// Branch says where a continuation came from.
type Branch struct {
	ParentRunID  string
	ParentStep   int
	Change       json.RawMessage
	CheckpointID string // the parent's checkpoint for ParentStep, reused as this run's first
}

// InitialParams is the first request of a task's run.
func InitialParams(t *task.Task) anthropic.MessageNewParams {
	p := anthropic.MessageNewParams{
		Model:        anthropic.Model(t.Model),
		MaxTokens:    MaxTokens,
		Messages:     []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock(t.Prompt))},
		Tools:        agent.Tools(),
		OutputConfig: anthropic.OutputConfigParam{Effort: anthropic.OutputConfigEffort(t.Effort)},
		// Caches the growing conversation, so each step re-reads its prefix.
		CacheControl: anthropic.NewCacheControlEphemeralParam(),
	}
	if t.System != "" {
		p.System = []anthropic.TextBlockParam{{Text: t.System}}
	}
	return p
}

// Start runs a task from a fresh sandbox.
func (c *Controller) Start(ctx context.Context, t *task.Task) Result {
	ctx, root := c.Tracer.Start(ctx, "invoke_agent "+t.ID, trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "invoke_agent"),
		attribute.String("gen_ai.agent.name", t.ID),
		attribute.String(AttrTaskID, t.ID),
		attribute.String(AttrDriver, c.Driver.Caps().Name),
	))
	runID := root.SpanContext().TraceID().String()
	root.SetAttributes(attribute.String(AttrRunID, runID))

	if err := c.Store.CreateRun(ctx, runstore.Run{
		RunID: runID, TaskID: t.ID, Task: t.JSON(), Driver: c.Driver.Caps().Name,
	}); err != nil {
		root.End()
		return Result{RunID: runID, Status: runstore.StatusError, Err: fmt.Errorf("record run: %w", err)}
	}

	inst, err := c.Driver.Create(ctx, t.Template)
	if err != nil {
		return c.finish(ctx, root, runID, 0, "", fmt.Errorf("create sandbox: %w", err))
	}
	defer c.destroy(inst)

	for _, cmd := range t.Setup {
		res, err := c.Driver.Exec(ctx, inst, driver.ExecRequest{Args: cmd, Dir: t.Workdir, Timeout: t.ToolTimeout.Duration})
		if err == nil && res.ExitCode != 0 {
			err = fmt.Errorf("exit %d: %s%s", res.ExitCode, res.Stdout, res.Stderr)
		}
		if err != nil {
			return c.finish(ctx, root, runID, 0, "", fmt.Errorf("setup %q: %w", cmd, err))
		}
	}
	conv, err := agent.NewConversation(InitialParams(t))
	if err != nil {
		return c.finish(ctx, root, runID, 0, "", err)
	}
	return c.drive(ctx, root, runID, t, inst, conv, 1, "")
}

// Continue runs a branch on an instance already restored from the parent's
// checkpoint, starting with the LLM call at br.ParentStep. conv is the request
// for that call, with the branch's change applied. The branch numbers its steps
// on from the parent's, so step k means the same moment in both.
func (c *Controller) Continue(ctx context.Context, t *task.Task, inst driver.Instance, conv *agent.Conversation, br Branch) Result {
	ctx, root := c.Tracer.Start(ctx, "invoke_agent "+t.ID, trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "invoke_agent"),
		attribute.String("gen_ai.agent.name", t.ID),
		attribute.String(AttrTaskID, t.ID),
		attribute.String(AttrDriver, c.Driver.Caps().Name),
		attribute.String(AttrParentRunID, br.ParentRunID),
		attribute.Int(AttrParentStep, br.ParentStep),
		attribute.String(AttrChange, string(br.Change)),
	))
	runID := root.SpanContext().TraceID().String()
	root.SetAttributes(attribute.String(AttrRunID, runID))
	defer c.destroy(inst)

	if err := c.Store.CreateRun(ctx, runstore.Run{
		RunID: runID, TaskID: t.ID, Task: t.JSON(), Driver: c.Driver.Caps().Name,
		ParentRunID: br.ParentRunID, ParentStep: br.ParentStep, Change: br.Change,
	}); err != nil {
		root.End()
		return Result{RunID: runID, Status: runstore.StatusError, Err: fmt.Errorf("record run: %w", err)}
	}
	return c.drive(ctx, root, runID, t, inst, conv, br.ParentStep, br.CheckpointID)
}

// drive is the agent loop. firstCP, if set, is an existing checkpoint of the
// instance's current state, used for the first LLM step instead of a new one.
func (c *Controller) drive(ctx context.Context, root trace.Span, runID string, t *task.Task,
	inst driver.Instance, conv *agent.Conversation, step int, firstCP string) Result {

	llmCalls := 0
	for {
		if llmCalls >= t.MaxSteps {
			c.Log.Info("max steps reached", "run", runID, "steps", llmCalls)
			break
		}
		cpID, cpDur := firstCP, time.Duration(0)
		if cpID == "" {
			var err error
			if cpID, cpDur, err = c.checkpoint(ctx, runID, step, inst); err != nil {
				return c.finish(ctx, root, runID, step, "", err)
			}
		}
		firstCP = ""

		msg, err := c.callModel(ctx, runID, step, cpID, cpDur, conv)
		if err != nil {
			return c.finish(ctx, root, runID, step, "", err)
		}
		llmCalls++
		step++
		if err := conv.Append(msg.ToParam()); err != nil {
			return c.finish(ctx, root, runID, step, "", err)
		}
		if msg.StopReason != anthropic.StopReasonToolUse {
			if msg.StopReason == anthropic.StopReasonRefusal {
				c.Log.Warn("model refused", "run", runID, "category", msg.StopDetails.Category)
			}
			break
		}

		var results []anthropic.ContentBlockParamUnion
		for _, block := range msg.Content {
			tu, ok := block.AsAny().(anthropic.ToolUseBlock)
			if !ok {
				continue
			}
			res, err := c.runTool(ctx, runID, step, t, inst, agent.ToolCall{ID: tu.ID, Name: tu.Name, Input: json.RawMessage(tu.JSON.Input.Raw())})
			if err != nil {
				return c.finish(ctx, root, runID, step, "", err)
			}
			results = append(results, anthropic.NewToolResultBlock(tu.ID, res.Output, res.IsError))
			step++
		}
		if err := conv.Append(anthropic.NewUserMessage(results...)); err != nil {
			return c.finish(ctx, root, runID, step, "", err)
		}
	}

	res, err := c.Driver.Exec(ctx, inst, driver.ExecRequest{Args: t.Check, Dir: t.Workdir, Timeout: 10 * time.Minute})
	if err != nil {
		return c.finish(ctx, root, runID, step-1, "", fmt.Errorf("check: %w", err))
	}
	outcome := runstore.OutcomeFail
	if res.ExitCode == 0 {
		outcome = runstore.OutcomePass
	}
	return c.finish(ctx, root, runID, step-1, outcome, nil)
}

func (c *Controller) checkpoint(ctx context.Context, runID string, step int, inst driver.Instance) (string, time.Duration, error) {
	start := time.Now()
	ref, err := c.Driver.Checkpoint(ctx, inst)
	if err != nil {
		return "", 0, fmt.Errorf("checkpoint step %d: %w", step, err)
	}
	took := time.Since(start)
	b := make([]byte, 8)
	rand.Read(b)
	caps := c.Driver.Caps()
	cp := runstore.Checkpoint{
		CheckpointID: "cp_" + hex.EncodeToString(b), RunID: runID, Step: step,
		Driver: caps.Name, Level: int(caps.Level), DriverRef: string(ref),
	}
	if err := c.Store.AddCheckpoint(ctx, cp); err != nil {
		return "", 0, fmt.Errorf("record checkpoint: %w", err)
	}
	return cp.CheckpointID, took, nil
}

func (c *Controller) callModel(ctx context.Context, runID string, step int, cpID string, cpDur time.Duration,
	conv *agent.Conversation) (*anthropic.Message, error) {

	req, err := conv.Body()
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	model := conv.Model()
	ctx, span := c.Tracer.Start(ctx, "chat "+model, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("gen_ai.operation.name", "chat"),
			attribute.String("gen_ai.provider.name", "anthropic"),
			attribute.String("gen_ai.request.model", model),
			attribute.String(AttrRunID, runID),
			attribute.Int(AttrStepIndex, step),
			attribute.String(AttrStepKind, runstore.KindLLM),
			attribute.String(AttrCheckpointID, cpID),
			attribute.Float64(AttrCheckpointMs, float64(cpDur.Microseconds())/1000),
			attribute.String(AttrLLMRequest, string(req)),
		))
	defer span.End()

	msg, err := c.Model.Send(ctx, req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("model call at step %d: %w", step, err)
	}
	span.SetAttributes(
		attribute.String("gen_ai.response.model", string(msg.Model)),
		attribute.String("gen_ai.response.id", msg.ID),
		attribute.StringSlice("gen_ai.response.finish_reasons", []string{string(msg.StopReason)}),
		attribute.Int64("gen_ai.usage.input_tokens", msg.Usage.InputTokens+msg.Usage.CacheReadInputTokens+msg.Usage.CacheCreationInputTokens),
		attribute.Int64("gen_ai.usage.output_tokens", msg.Usage.OutputTokens),
		attribute.Int64("gen_ai.usage.cache_read.input_tokens", msg.Usage.CacheReadInputTokens),
		attribute.String(AttrLLMResponse, msg.RawJSON()),
	)
	if err := c.Store.AddStep(ctx, runstore.Step{
		RunID: runID, Index: step, SpanID: span.SpanContext().SpanID().String(),
		Kind: runstore.KindLLM, CheckpointID: cpID,
	}); err != nil {
		return nil, fmt.Errorf("record step: %w", err)
	}
	return msg, nil
}

func (c *Controller) runTool(ctx context.Context, runID string, step int, t *task.Task, inst driver.Instance,
	call agent.ToolCall) (agent.ToolResult, error) {

	ctx, span := c.Tracer.Start(ctx, "execute_tool "+call.Name, trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "execute_tool"),
		attribute.String("gen_ai.tool.name", call.Name),
		attribute.String("gen_ai.tool.call.id", call.ID),
		attribute.String("gen_ai.tool.call.arguments", string(call.Input)),
		attribute.String(AttrRunID, runID),
		attribute.Int(AttrStepIndex, step),
		attribute.String(AttrStepKind, runstore.KindTool),
	))
	defer span.End()

	res, err := agent.RunTool(ctx, c.Driver, inst, t.Workdir, t.ToolTimeout.Duration, call)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return res, fmt.Errorf("tool %s at step %d: %w", call.Name, step, err)
	}
	span.SetAttributes(
		attribute.String("gen_ai.tool.call.result", res.Output),
		attribute.Int("furcate.tool.exit_code", res.Exit),
	)
	if res.IsError {
		span.SetStatus(codes.Error, "tool call failed")
	}
	if err := c.Store.AddStep(ctx, runstore.Step{
		RunID: runID, Index: step, SpanID: span.SpanContext().SpanID().String(), Kind: runstore.KindTool,
	}); err != nil {
		return res, fmt.Errorf("record step: %w", err)
	}
	return res, nil
}

// finish records the end of a run on its root span and in the store.
func (c *Controller) finish(ctx context.Context, root trace.Span, runID string, lastStep int, outcome string, runErr error) Result {
	status := runstore.StatusDone
	errMsg := ""
	if runErr != nil {
		status, errMsg = runstore.StatusError, runErr.Error()
		root.RecordError(runErr)
		root.SetStatus(codes.Error, errMsg)
	} else {
		root.SetAttributes(attribute.String(AttrOutcome, outcome))
		if outcome == runstore.OutcomeFail {
			root.SetStatus(codes.Error, "check failed")
		} else {
			root.SetStatus(codes.Ok, "")
		}
	}
	root.End()
	if err := c.Store.FinishRun(context.WithoutCancel(ctx), runID, status, outcome, errMsg); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("record finish: %w", err))
	}
	return Result{RunID: runID, Status: status, Outcome: outcome, Steps: lastStep, Err: runErr}
}

func (c *Controller) destroy(inst driver.Instance) {
	if c.Keep {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Driver.Destroy(ctx, inst); err != nil {
		c.Log.Warn("destroy sandbox", "instance", inst, "err", err)
	}
}
