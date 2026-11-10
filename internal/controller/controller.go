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
	"sort"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanidhyasin/furcate/internal/agent"
	"github.com/sanidhyasin/furcate/internal/driver"
	"github.com/sanidhyasin/furcate/internal/pricing"
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
	AttrCostUSD      = "furcate.cost_usd"
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
	Keep   bool          // leave instances running after a run, for inspection
	Prices pricing.Table // nil: pricing.Defaults()
	// Slots, if set, bounds how many sandboxes this process runs at once. A run
	// takes one slot for its sandbox; forks take one per branch.
	Slots *Slots
}

// Result is one finished run.
type Result struct {
	RunID    string
	Status   string // runstore.StatusDone or StatusError
	Outcome  string // pass, fail, or "" on error
	Steps    int    // last step index
	Usage    runstore.Usage
	Duration time.Duration
	// Checkpoints taken and the time they took: the per-step overhead.
	Checkpoints    int
	CheckpointTime time.Duration
	Err            error
}

// runState is one run in progress.
type runState struct {
	id     string
	start  time.Time
	usage  runstore.Usage
	files  map[string]string
	cps    int
	cpTime time.Duration
}

// Branch says where a continuation came from.
type Branch struct {
	ParentRunID  string
	ParentStep   int
	Change       json.RawMessage
	CheckpointID string // the parent's checkpoint for ParentStep, reused as this run's first
	Eval         runstore.EvalTag
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
	rs := &runState{id: root.SpanContext().TraceID().String(), start: time.Now()}
	root.SetAttributes(attribute.String(AttrRunID, rs.id))

	if err := c.Store.CreateRun(ctx, runstore.Run{
		RunID: rs.id, TaskID: t.ID, Task: t.JSON(), Driver: c.Driver.Caps().Name,
	}); err != nil {
		root.End()
		return Result{RunID: rs.id, Status: runstore.StatusError, Err: fmt.Errorf("record run: %w", err)}
	}

	release, err := c.Slots.Acquire(ctx, 1)
	if err != nil {
		return c.finish(ctx, root, rs, 0, "", err)
	}
	defer release(1)
	inst, err := c.Driver.Create(ctx, t.Template)
	if err != nil {
		return c.finish(ctx, root, rs, 0, "", fmt.Errorf("create sandbox: %w", err))
	}
	defer c.destroy(inst)

	if err := Prepare(ctx, c.Driver, inst, t); err != nil {
		return c.finish(ctx, root, rs, 0, "", err)
	}
	conv, err := agent.NewConversation(InitialParams(t))
	if err != nil {
		return c.finish(ctx, root, rs, 0, "", err)
	}
	return c.drive(ctx, root, rs, t, inst, conv, 1, "")
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
		attribute.String("furcate.eval.case", br.Eval.Case),
		attribute.String("furcate.eval.variant", br.Eval.Variant),
	))
	rs := &runState{id: root.SpanContext().TraceID().String(), start: time.Now()}
	root.SetAttributes(attribute.String(AttrRunID, rs.id))
	defer c.destroy(inst)

	if err := c.Store.CreateRun(ctx, runstore.Run{
		RunID: rs.id, TaskID: t.ID, Task: t.JSON(), Driver: c.Driver.Caps().Name,
		ParentRunID: br.ParentRunID, ParentStep: br.ParentStep, Change: br.Change, Eval: br.Eval,
	}); err != nil {
		root.End()
		return Result{RunID: rs.id, Status: runstore.StatusError, Err: fmt.Errorf("record run: %w", err)}
	}
	return c.drive(ctx, root, rs, t, inst, conv, br.ParentStep, br.CheckpointID)
}

// drive is the agent loop. firstCP, if set, is an existing checkpoint of the
// instance's current state, used for the first LLM step instead of a new one.
func (c *Controller) drive(ctx context.Context, root trace.Span, rs *runState, t *task.Task,
	inst driver.Instance, conv *agent.Conversation, step int, firstCP string) Result {

	llmCalls := 0
	for {
		if llmCalls >= t.MaxSteps {
			c.Log.Info("max steps reached", "run", rs.id, "steps", llmCalls)
			break
		}
		cpID, cpDur := firstCP, time.Duration(0)
		if cpID == "" {
			var err error
			if cpID, cpDur, err = c.checkpoint(ctx, rs.id, step, inst); err != nil {
				return c.finish(ctx, root, rs, step, "", err)
			}
			rs.cps++
			rs.cpTime += cpDur
		}
		firstCP = ""

		msg, err := c.callModel(ctx, rs, step, cpID, cpDur, conv)
		if err != nil {
			return c.finish(ctx, root, rs, step, "", err)
		}
		llmCalls++
		step++
		if err := conv.Append(msg.ToParam()); err != nil {
			return c.finish(ctx, root, rs, step, "", err)
		}
		if msg.StopReason != anthropic.StopReasonToolUse {
			if msg.StopReason == anthropic.StopReasonRefusal {
				c.Log.Warn("model refused", "run", rs.id, "category", msg.StopDetails.Category)
			}
			break
		}

		var results []anthropic.ContentBlockParamUnion
		for _, block := range msg.Content {
			tu, ok := block.AsAny().(anthropic.ToolUseBlock)
			if !ok {
				continue
			}
			res, err := c.runTool(ctx, rs.id, step, t, inst, agent.ToolCall{ID: tu.ID, Name: tu.Name, Input: json.RawMessage(tu.JSON.Input.Raw())})
			if err != nil {
				return c.finish(ctx, root, rs, step, "", err)
			}
			results = append(results, anthropic.NewToolResultBlock(tu.ID, res.Output, res.IsError))
			step++
		}
		if err := conv.Append(anthropic.NewUserMessage(results...)); err != nil {
			return c.finish(ctx, root, rs, step, "", err)
		}
	}

	rs.files = Manifest(ctx, c.Driver, inst, t.Workdir)
	res, err := c.Driver.Exec(ctx, inst, driver.ExecRequest{Args: t.Check, Dir: t.Workdir, Timeout: 10 * time.Minute})
	if err != nil {
		return c.finish(ctx, root, rs, step-1, "", fmt.Errorf("check: %w", err))
	}
	outcome := runstore.OutcomeFail
	if res.ExitCode == 0 {
		outcome = runstore.OutcomePass
	}
	return c.finish(ctx, root, rs, step-1, outcome, nil)
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

func (c *Controller) callModel(ctx context.Context, rs *runState, step int, cpID string, cpDur time.Duration,
	conv *agent.Conversation) (*anthropic.Message, error) {
	runID := rs.id

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
	u := msg.Usage
	prices := c.Prices
	if prices == nil {
		prices = pricing.Defaults()
	}
	cost := prices.CostWithCache(string(msg.Model), uint64(u.InputTokens), uint64(u.OutputTokens),
		uint64(u.CacheReadInputTokens), uint64(u.CacheCreationInputTokens))
	rs.usage = rs.usage.Add(runstore.Usage{LLMCalls: 1, InputTokens: u.InputTokens, OutputTokens: u.OutputTokens,
		CacheReadTokens: u.CacheReadInputTokens, CacheWriteTokens: u.CacheCreationInputTokens, CostUSD: cost})
	span.SetAttributes(
		attribute.Float64(AttrCostUSD, cost),
		attribute.Int64("gen_ai.usage.cache_creation.input_tokens", u.CacheCreationInputTokens),
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
func (c *Controller) finish(ctx context.Context, root trace.Span, rs *runState, lastStep int, outcome string, runErr error) Result {
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
	if err := c.Store.FinishRun(context.WithoutCancel(ctx), rs.id, runstore.End{
		Status: status, Outcome: outcome, Error: errMsg, Usage: rs.usage, FinalFiles: rs.files,
	}); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("record finish: %w", err))
	}
	return Result{RunID: rs.id, Status: status, Outcome: outcome, Steps: lastStep, Usage: rs.usage,
		Duration: time.Since(rs.start), Checkpoints: rs.cps, CheckpointTime: rs.cpTime, Err: runErr}
}

// Prepare readies a fresh sandbox for a task: the workdir, the task's local
// files, then its setup commands.
func Prepare(ctx context.Context, sb driver.Sandbox, inst driver.Instance, t *task.Task) error {
	tgz, err := t.Archive()
	if err != nil {
		return err
	}
	return PrepareFiles(ctx, sb, inst, t, tgz)
}

// PrepareFiles is Prepare with the task's files given as a gzipped tarball (nil
// for none) instead of read from t.Files: an eval bundle's starting files.
func PrepareFiles(ctx context.Context, sb driver.Sandbox, inst driver.Instance, t *task.Task, tgz []byte) error {
	if t.Workdir != "" {
		if res, err := sb.Exec(ctx, inst, driver.ExecRequest{Args: []string{"mkdir", "-p", t.Workdir}}); err != nil || res.ExitCode != 0 {
			return fmt.Errorf("create workdir: %v %s", err, res.Stderr)
		}
	}
	if tgz != nil {
		res, err := sb.Exec(ctx, inst, driver.ExecRequest{Args: []string{"tar", "xzf", "-"}, Stdin: tgz, Dir: t.Workdir, Timeout: 5 * time.Minute})
		if err == nil && res.ExitCode != 0 {
			err = fmt.Errorf("exit %d: %s", res.ExitCode, res.Stderr)
		}
		if err != nil {
			return fmt.Errorf("copy task files: %w", err)
		}
	}
	for _, cmd := range t.Setup {
		res, err := sb.Exec(ctx, inst, driver.ExecRequest{Args: cmd, Dir: t.Workdir, Timeout: t.ToolTimeout.Duration})
		if err == nil && res.ExitCode != 0 {
			err = fmt.Errorf("exit %d: %s%s", res.ExitCode, res.Stdout, res.Stderr)
		}
		if err != nil {
			return fmt.Errorf("setup %q: %w", cmd, err)
		}
	}
	return nil
}

// MaxManifestFiles bounds the final-files manifest.
const MaxManifestFiles = 5000

// Manifest hashes every file under the workdir (skipping .git) when a run
// ends, so two branches' final states can be compared. Best effort: a sandbox
// without sha256sum or shasum just yields no manifest.
func Manifest(ctx context.Context, sb driver.Sandbox, inst driver.Instance, workdir string) map[string]string {
	res, err := sb.Exec(ctx, inst, driver.ExecRequest{
		Args: []string{"sh", "-c", `h=sha256sum; command -v sha256sum >/dev/null || h="shasum -a 256"; ` +
			`find . -type f -not -path './.git/*' -not -path '*/__pycache__/*' -exec $h {} + 2>/dev/null | head -n ` +
			fmt.Sprint(MaxManifestFiles)},
		Dir: workdir, Timeout: time.Minute,
	})
	if err != nil || res.ExitCode != 0 {
		return nil
	}
	files := map[string]string{}
	for _, line := range strings.Split(string(res.Stdout), "\n") {
		hash, path, ok := strings.Cut(line, "  ")
		if ok && len(hash) == 64 {
			files[path] = hash
		}
	}
	return files
}

// SortedPaths lists a manifest's paths in order.
func SortedPaths(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for p := range m {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
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
