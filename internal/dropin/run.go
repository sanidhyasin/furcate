package dropin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/driver"
	"github.com/sanidhyasin/furcate/internal/pricing"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/stats"
	"github.com/sanidhyasin/furcate/internal/task"
)

// DefaultInstruction is what a resumed agent is told when a fork changes nothing.
const DefaultInstruction = "Continue where you left off."

// Engine runs and forks drop-in agents.
type Engine struct {
	Driver   driver.Sandbox
	Store    runstore.Store
	Tracer   trace.Tracer
	Prices   pricing.Table
	Slots    *controller.Slots
	Log      *slog.Logger
	Upstream string // the Claude API; default https://api.anthropic.com
	APIKey   string // the host's key; the sandbox never sees it
	// Listen is where each run's proxy listens, and SandboxHost how the sandbox
	// reaches it. Defaults suit Docker Desktop: 127.0.0.1:0 and
	// host.docker.internal. On Linux Docker, listen on the docker0 address.
	Listen      string
	SandboxHost string
	Client      *http.Client // for the upstream; default http.DefaultClient
}

// Start runs a task's agent from a fresh sandbox.
func (e *Engine) Start(ctx context.Context, t *task.Task) controller.Result {
	if t.DropIn == nil {
		return controller.Result{Status: runstore.StatusError, Err: errors.New("task has no dropin section")}
	}
	release, err := e.Slots.Acquire(ctx, 1)
	if err != nil {
		return controller.Result{Status: runstore.StatusError, Err: err}
	}
	defer release(1)
	inst, err := e.Driver.Create(ctx, t.Template)
	if err != nil {
		return controller.Result{Status: runstore.StatusError, Err: fmt.Errorf("create sandbox: %w", err)}
	}
	defer e.destroy(inst)
	return e.run(ctx, t, inst, t.DropIn.Command, map[string]string{"prompt": t.Prompt}, 1, "", controller.Branch{}, true)
}

// Fork resumes the agent from a recorded request, N times. A change's model
// applies through the proxy; its operator instruction is what the resumed agent
// is told. Branches keep the network: they must reach the proxy.
func (e *Engine) Fork(ctx context.Context, req replay.Request) (*replay.Result, error) {
	run, err := e.Store.Run(ctx, req.RunID)
	if err != nil {
		return nil, err
	}
	t, err := task.Parse(run.Task)
	if err != nil {
		return nil, err
	}
	if t.DropIn == nil || len(t.DropIn.Resume) == 0 {
		return nil, errors.New("the task has no dropin resume command")
	}
	if req.Change.ToolOutput != nil || req.Change.System != nil {
		return nil, errors.New("drop-in forks support a model change and an instruction (append_system) only")
	}
	step, err := e.Store.Step(ctx, req.RunID, req.Step)
	if err != nil {
		return nil, fmt.Errorf("step %d: %w", req.Step, err)
	}
	cp, err := e.Store.Checkpoint(ctx, step.CheckpointID)
	if err != nil {
		return nil, fmt.Errorf("checkpoint of step %d: %w", req.Step, err)
	}
	instruction := req.Change.AppendSystem
	if instruction == "" {
		instruction = DefaultInstruction
	}
	change, _ := json.Marshal(req.Change)
	br := controller.Branch{ParentRunID: req.RunID, ParentStep: req.Step, Change: change, CheckpointID: step.CheckpointID}

	res := &replay.Result{Request: req, Branches: make([]controller.Result, req.N)}
	var wg sync.WaitGroup
	for next := 0; next < req.N; {
		k, release, err := e.Slots.AcquireUpTo(ctx, req.N-next)
		if err != nil {
			wg.Wait()
			return nil, err
		}
		insts, err := e.Driver.Fork(ctx, driver.CheckpointID(cp.DriverRef), k, false)
		if err != nil {
			for _, inst := range insts {
				e.destroy(inst)
			}
			release(k)
			if next == 0 {
				return nil, fmt.Errorf("fork checkpoint: %w", err)
			}
			for i := next; i < req.N; i++ {
				res.Branches[i] = controller.Result{Status: runstore.StatusError, Err: err}
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
				defer e.destroy(inst)
				b := br
				res.Branches[i] = e.runBranch(ctx, t, inst, instruction, req.Change.Model, req.Step, b)
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

func (e *Engine) runBranch(ctx context.Context, t *task.Task, inst driver.Instance, instruction, model string, step int, br controller.Branch) controller.Result {
	vars := map[string]string{"instruction": instruction, "prompt": t.Prompt}
	return e.runWithModel(ctx, t, inst, t.DropIn.Resume, vars, step, br.CheckpointID, br, model)
}

func (e *Engine) run(ctx context.Context, t *task.Task, inst driver.Instance, cmd []string, vars map[string]string,
	firstStep int, firstCP string, br controller.Branch, prepare bool) controller.Result {
	if prepare {
		if err := controller.Prepare(ctx, e.Driver, inst, t); err != nil {
			return controller.Result{Status: runstore.StatusError, Err: err}
		}
	}
	return e.runWithModel(ctx, t, inst, cmd, vars, firstStep, firstCP, br, "")
}

// runWithModel records one run: a root span, a proxy for its model traffic, the
// agent's command, then the task's check.
func (e *Engine) runWithModel(ctx context.Context, t *task.Task, inst driver.Instance, cmd []string, vars map[string]string,
	firstStep int, firstCP string, br controller.Branch, model string) controller.Result {

	start := time.Now()
	ctx, root := e.Tracer.Start(ctx, "invoke_agent "+t.ID, trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "invoke_agent"),
		attribute.String("gen_ai.agent.name", t.ID),
		attribute.String(controller.AttrTaskID, t.ID),
		attribute.String(controller.AttrDriver, e.Driver.Caps().Name),
		attribute.String("furcate.mode", "drop-in"),
		attribute.String(controller.AttrParentRunID, br.ParentRunID),
		attribute.Int(controller.AttrParentStep, br.ParentStep),
		attribute.String(controller.AttrChange, string(br.Change)),
	))
	runID := root.SpanContext().TraceID().String()
	root.SetAttributes(attribute.String(controller.AttrRunID, runID))
	fail := func(err error) controller.Result {
		root.RecordError(err)
		root.SetStatus(codes.Error, err.Error())
		root.End()
		e.Store.FinishRun(context.WithoutCancel(ctx), runID, runstore.End{Status: runstore.StatusError, Error: err.Error()})
		return controller.Result{RunID: runID, Status: runstore.StatusError, Err: err, Duration: time.Since(start)}
	}
	if err := e.Store.CreateRun(ctx, runstore.Run{RunID: runID, TaskID: t.ID, Task: t.JSON(), Driver: e.Driver.Caps().Name,
		ParentRunID: br.ParentRunID, ParentStep: br.ParentStep, Change: br.Change}); err != nil {
		root.End()
		return controller.Result{RunID: runID, Status: runstore.StatusError, Err: err}
	}

	proxy := &Proxy{Driver: e.Driver, Store: e.Store, Tracer: e.Tracer, Prices: e.Prices, Upstream: e.Upstream,
		APIKey: e.APIKey, Model: model, Client: e.Client, Ctx: ctx, RunID: runID}
	proxy.Attach(inst, firstStep, firstCP)
	listen := e.Listen
	if listen == "" {
		listen = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return fail(fmt.Errorf("proxy: %w", err))
	}
	srv := &http.Server{Handler: proxy, ReadHeaderTimeout: 30 * time.Second}
	go srv.Serve(ln)
	defer srv.Close()
	host := e.SandboxHost
	if host == "" {
		host = "host.docker.internal"
	}
	_, port, _ := net.SplitHostPort(ln.Addr().String())

	env := []string{"ANTHROPIC_BASE_URL=http://" + net.JoinHostPort(host, port), "ANTHROPIC_API_KEY=" + Placeholder}
	for k, v := range t.DropIn.Env {
		env = append(env, k+"="+v)
	}
	timeout := t.DropIn.Timeout.Duration
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	out, err := e.Driver.Exec(ctx, inst, driver.ExecRequest{Args: task.Expand(cmd, vars), Dir: t.Workdir, Env: env, Timeout: timeout})
	if err != nil {
		return fail(fmt.Errorf("agent: %w", err))
	}
	tail := func(b []byte) string {
		if len(b) > 2000 {
			b = b[len(b)-2000:]
		}
		return string(b)
	}
	root.SetAttributes(attribute.Int("furcate.agent.exit_code", out.ExitCode),
		attribute.String("furcate.agent.stdout_tail", tail(out.Stdout)), attribute.String("furcate.agent.stderr_tail", tail(out.Stderr)))

	files := controller.Manifest(ctx, e.Driver, inst, t.Workdir)
	check, err := e.Driver.Exec(ctx, inst, driver.ExecRequest{Args: t.Check, Dir: t.Workdir, Timeout: 10 * time.Minute})
	if err != nil {
		return fail(fmt.Errorf("check: %w", err))
	}
	outcome := runstore.OutcomeFail
	if check.ExitCode == 0 {
		outcome = runstore.OutcomePass
		root.SetStatus(codes.Ok, "")
	} else {
		root.SetStatus(codes.Error, "check failed")
	}
	root.SetAttributes(attribute.String(controller.AttrOutcome, outcome))
	root.End()

	usage, cps, cpTime, next := proxy.Usage()
	if err := e.Store.FinishRun(context.WithoutCancel(ctx), runID, runstore.End{Status: runstore.StatusDone, Outcome: outcome,
		Usage: usage, FinalFiles: files}); err != nil {
		return controller.Result{RunID: runID, Status: runstore.StatusError, Err: err}
	}
	return controller.Result{RunID: runID, Status: runstore.StatusDone, Outcome: outcome, Steps: next - 1, Usage: usage,
		Duration: time.Since(start), Checkpoints: cps, CheckpointTime: cpTime}
}

func (e *Engine) destroy(inst driver.Instance) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e.Driver.Destroy(ctx, inst)
}
