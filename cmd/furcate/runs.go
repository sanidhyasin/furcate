package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/sanidhyasin/furcate/internal/agent"
	"github.com/sanidhyasin/furcate/internal/bisect"
	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/driver"
	"github.com/sanidhyasin/furcate/internal/driver/docker"
	"github.com/sanidhyasin/furcate/internal/driver/tine"
	"github.com/sanidhyasin/furcate/internal/dropin"
	"github.com/sanidhyasin/furcate/internal/pricing"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/stats"
	"github.com/sanidhyasin/furcate/internal/store"
	"github.com/sanidhyasin/furcate/internal/task"
)

// drivers lists the sandbox drivers the CLI can use. The test-only directory
// driver is deliberately absent: it has no isolation.
var drivers = map[string]func() (driver.Sandbox, error){
	"docker": func() (driver.Sandbox, error) {
		return docker.New(docker.Options{Memory: envOr("FURCATE_DOCKER_MEMORY", "2g")})
	},
	// FURCATE_TINE_HOST is unix:///run/tine/tine.sock (default) or ssh://<host>;
	// FURCATE_TINE_SSH is the ssh command, e.g. "ssh -F ../tine/.tine-ssh-config".
	"tine": func() (driver.Sandbox, error) {
		vcpus, _ := strconv.Atoi(os.Getenv("FURCATE_TINE_VCPUS"))
		mem, _ := strconv.Atoi(os.Getenv("FURCATE_TINE_MEMORY_MIB"))
		return tine.New(tine.Options{Host: os.Getenv("FURCATE_TINE_HOST"), SSH: os.Getenv("FURCATE_TINE_SSH"),
			VCPUs: vcpus, MemoryMiB: mem, Strategy: os.Getenv("FURCATE_TINE_STRATEGY")})
	},
}

func newDriver(name string) (driver.Sandbox, error) {
	mk, ok := drivers[name]
	if !ok {
		var names []string
		for n := range drivers {
			names = append(names, n)
		}
		slices.Sort(names)
		if len(names) == 0 {
			return nil, errors.New("no sandbox drivers")
		}
		return nil, fmt.Errorf("unknown driver %q (have: %s)", name, strings.Join(names, ", "))
	}
	return mk()
}

func pgFlag(fs *flag.FlagSet) *string {
	return fs.String("pg", envOr("FURCATE_PG", runstore.DefaultDSN), "Postgres DSN for run state (env FURCATE_PG)")
}

// env is what run and fork share: stores, tracing, model and driver.
type env struct {
	ch      *store.CH
	pg      *runstore.Postgres
	tp      *sdktrace.TracerProvider
	ctl     *controller.Controller
	cleanup func()
}

func openEnv(ctx context.Context, f runFlags) (*env, error) {
	sb, err := newDriver(*f.driver)
	if err != nil {
		return nil, err
	}
	prices, err := pricing.Load(*f.prices)
	if err != nil {
		return nil, fmt.Errorf("prices: %w", err)
	}
	dsn, pgDSN, otlp := *f.dsn, *f.pg, *f.otlp
	ch, err := store.Open(ctx, dsn)
	if err != nil {
		return nil, err
	}
	pg, err := runstore.OpenPostgres(ctx, pgDSN)
	if err != nil {
		ch.Close()
		return nil, err
	}
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(strings.TrimRight(otlp, "/")+"/v1/traces"))
	if err != nil {
		ch.Close()
		pg.Close()
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName("furcate-agent"))))
	e := &env{ch: ch, pg: pg, tp: tp, ctl: &controller.Controller{
		Driver: sb, Model: agent.NewAnthropic(!*f.noFallbacks), Store: pg, Tracer: tp.Tracer("furcate"),
		Log: slog.New(slog.NewTextHandler(os.Stderr, nil)), Keep: *f.keep, Prices: prices,
		Slots: controller.NewSlots(*f.maxSandboxes),
	}}
	e.cleanup = func() {
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tp.Shutdown(sctx) // flush spans to the collector
		pg.Close()
		ch.Close()
	}
	return e, nil
}

type runFlags struct {
	dsn, pg, otlp, driver, prices *string
	noFallbacks, keep             *bool
	maxSandboxes                  *int
}

func addRunFlags(fs *flag.FlagSet) runFlags {
	return runFlags{
		dsn: dsnFlag(fs), pg: pgFlag(fs),
		otlp:         fs.String("otlp", envOr("FURCATE_OTLP", "http://localhost:4318"), "collector OTLP/HTTP base URL (env FURCATE_OTLP)"),
		driver:       fs.String("driver", envOr("FURCATE_DRIVER", "docker"), "sandbox driver: docker or tine (env FURCATE_DRIVER)"),
		noFallbacks:  fs.Bool("no-fallbacks", false, "don't let the API re-serve a refused request with a fallback model"),
		keep:         fs.Bool("keep", false, "leave sandboxes running after the run, for inspection"),
		prices:       fs.String("prices", "", "JSON price table on top of the built-in Claude prices"),
		maxSandboxes: fs.Int("max-sandboxes", 16, "most sandboxes this process runs at once; forks queue for a slot"),
	}
}

func runRun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	f := addRunFlags(fs)
	taskPath := fs.String("task", "", "task spec (JSON)")
	fs.Parse(args)
	if *taskPath == "" {
		return errors.New("usage: furcate run -task task.json [flags]")
	}
	t, err := task.Load(*taskPath)
	if err != nil {
		return err
	}
	e, err := openEnv(ctx, f)
	if err != nil {
		return err
	}
	defer e.cleanup()
	res := e.ctl.Start(ctx, t)
	fmt.Printf("run %s  %s  outcome=%s  steps=%d  %d LLM calls  $%.4f  %s\n", res.RunID, res.Status, orDash(res.Outcome),
		res.Steps, res.Usage.LLMCalls, res.Usage.CostUSD, res.Duration.Round(time.Millisecond))
	if res.Checkpoints > 0 {
		fmt.Printf("checkpoints: %d in %s, of which the run waited %s\n", res.Checkpoints,
			res.CheckpointTime.Round(time.Millisecond), res.CheckpointWait.Round(time.Millisecond))
	}
	if res.Err != nil {
		return res.Err
	}
	fmt.Printf("next: furcate steps %s\n", res.RunID)
	return nil
}

func runFork(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fork", flag.ExitOnError)
	f := addRunFlags(fs)
	step := fs.Int("step", 0, "LLM step to fork from (see furcate steps)")
	n := fs.Int("n", 10, "continuations to run")
	model := fs.String("model", "", "change: use this model from step k on")
	appendSystem := fs.String("append-system", "", "change: add this operator instruction at step k")
	toolOutput := fs.String("tool-output", "", "change: replace the last tool output before step k")
	fs.Parse(args)
	if fs.NArg() != 1 || *step <= 0 {
		return errors.New("usage: furcate fork -step k [-n 10] [change flags] <run-id>")
	}
	ch := replay.Change{Model: *model, AppendSystem: *appendSystem}
	fs.Visit(func(fl *flag.Flag) {
		if fl.Name == "tool-output" {
			ch.ToolOutput = toolOutput
		}
	})
	e, err := openEnv(ctx, f)
	if err != nil {
		return err
	}
	defer e.cleanup()
	eng := &replay.Engine{Controller: e.ctl, Store: e.pg, Conversations: e.ch}
	start := time.Now()
	res, err := eng.Fork(ctx, replay.Request{RunID: fs.Arg(0), Step: *step, N: *n, Change: ch})
	if err != nil {
		return err
	}
	fmt.Printf("fork of %s at step %d, change: %s\n", res.RunID, res.Step, res.Change)
	fmt.Printf("passed %d of %d  (95%% CI %.2f-%.2f)  in %s  $%.4f\n", res.Passes, res.Done, res.Lo, res.Hi,
		time.Since(start).Round(time.Millisecond), res.Usage.CostUSD)
	for _, b := range res.Branches {
		line := fmt.Sprintf("  %s  %s  %s", b.RunID, b.Status, orDash(b.Outcome))
		if b.Err != nil {
			line += "  " + b.Err.Error()
		}
		fmt.Println(line)
	}
	return nil
}

func runSteps(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("steps", flag.ExitOnError)
	dsn, pgDSN := dsnFlag(fs), pgFlag(fs)
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: furcate steps <run-id>")
	}
	runID := fs.Arg(0)
	pg, err := runstore.OpenPostgres(ctx, *pgDSN)
	if err != nil {
		return err
	}
	defer pg.Close()
	run, err := pg.Run(ctx, runID)
	if err != nil {
		return err
	}
	steps, err := pg.Steps(ctx, runID)
	if err != nil {
		return err
	}
	names := map[string]string{}
	if ch, err := store.Open(ctx, *dsn); err == nil {
		if spans, err := ch.Trace(ctx, runID, true); err == nil {
			for _, s := range spans {
				names[s.SpanID] = s.Name
			}
		}
		ch.Close()
	}
	fmt.Printf("run %s  task=%s  driver=%s  %s  outcome=%s", run.RunID, run.TaskID, run.Driver, run.Status, orDash(run.Outcome))
	if run.ParentRunID != "" {
		fmt.Printf("  branch of %s at step %d, change %s", run.ParentRunID, run.ParentStep, run.Change)
	}
	fmt.Println()
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "STEP\tKIND\tSPAN\tCHECKPOINT")
	for _, s := range steps {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\n", s.Index, s.Kind, orDash(names[s.SpanID]), orDash(s.CheckpointID))
	}
	return tw.Flush()
}

func runBranches(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("branches", flag.ExitOnError)
	pgDSN := pgFlag(fs)
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: furcate branches <run-id>")
	}
	pg, err := runstore.OpenPostgres(ctx, *pgDSN)
	if err != nil {
		return err
	}
	defer pg.Close()
	kids, err := pg.Children(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	if len(kids) == 0 {
		fmt.Println("no branches yet: furcate fork -step k <run-id>")
		return nil
	}
	type key struct {
		step   int
		change string
	}
	type agg struct{ pass, done, errs int }
	groups := map[key]*agg{}
	for _, k := range kids {
		g := groups[key{k.ParentStep, string(k.Change)}]
		if g == nil {
			g = &agg{}
			groups[key{k.ParentStep, string(k.Change)}] = g
		}
		switch {
		case k.Status == runstore.StatusDone:
			g.done++
			if k.Outcome == runstore.OutcomePass {
				g.pass++
			}
		case k.Status == runstore.StatusError:
			g.errs++
		}
	}
	keys := make([]key, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].step != keys[j].step {
			return keys[i].step < keys[j].step
		}
		return keys[i].change < keys[j].change
	})
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "STEP\tCHANGE\tPASSED\tRATE\t95% CI\tAGREE\tERRORS")
	for _, k := range keys {
		g := groups[k]
		lo, hi := stats.Wilson(g.pass, g.done, stats.Z95)
		rate := 0.0
		if g.done > 0 {
			rate = float64(g.pass) / float64(g.done)
		}
		fmt.Fprintf(tw, "%d\t%s\t%d/%d\t%.0f%%\t%.2f-%.2f\t%.2f\t%d\n", k.step, k.change, g.pass, g.done, rate*100, lo, hi,
			stats.Agreement(g.pass, g.done), g.errs)
	}
	return tw.Flush()
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func runBisect(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("bisect", flag.ExitOnError)
	f := addRunFlags(fs)
	n := fs.Int("n", 10, "continuations per probe")
	maxRuns := fs.Int("max-runs", 100, "budget: most continuations in total")
	maxUSD := fs.Float64("max-usd", 0, "budget: most dollars in total (0 = no cap)")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: furcate bisect [-n 10] [-max-runs 100] [-max-usd 20] <run-id>")
	}
	e, err := openEnv(ctx, f)
	if err != nil {
		return err
	}
	defer e.cleanup()
	eng := &replay.Engine{Controller: e.ctl, Store: e.pg, Conversations: e.ch}
	res, err := bisect.Run(ctx, eng, bisect.Config{RunID: fs.Arg(0), N: *n, MaxRuns: *maxRuns, MaxUSD: *maxUSD,
		Log: func(format string, a ...any) { fmt.Fprintf(os.Stderr, format+"\n", a...) }})
	if err != nil {
		return err
	}
	fmt.Printf("\nbisect of %s (checkpointed steps %v)\n", res.RunID, res.Steps)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "STEP\tPASSED\tRATE\t95% CI\tERRORS\tCOST")
	probes := slices.Clone(res.Probes)
	slices.SortFunc(probes, func(a, b *bisect.Probe) int { return a.Step - b.Step })
	for _, p := range probes {
		fmt.Fprintf(tw, "%d\t%d/%d\t%.0f%%\t%.2f-%.2f\t%d\t$%.4f\n", p.Step, p.Passes, p.Done, p.Rate()*100, p.Lo, p.Hi, p.Errors, p.Usage.CostUSD)
	}
	tw.Flush()
	fmt.Printf("\n%s\n", res.Verdict)
	if res.Faulty > 0 {
		describeStep(ctx, e, res.RunID, res.Faulty, res.FirstBad.Step)
	}
	ratio := ""
	if res.Original.CostUSD > 0 {
		ratio = fmt.Sprintf(" = %.1f times the original run", res.Usage.CostUSD/res.Original.CostUSD)
	}
	fmt.Printf("\ncost: %d continuations, %d LLM calls, %d output tokens, $%.4f%s, %s\n", res.Runs, res.Usage.LLMCalls,
		res.Usage.OutputTokens, res.Usage.CostUSD, ratio, res.Duration.Round(time.Millisecond))
	if !res.Confident {
		fmt.Println("low confidence: some probes never separated; rerun with a larger -n")
	}
	return nil
}

// describeStep prints what the run did between an LLM step and the next one:
// the tool calls that followed the decision.
func describeStep(ctx context.Context, e *env, runID string, from, to int) {
	steps, err := e.pg.Steps(ctx, runID)
	if err != nil {
		return
	}
	spans, err := e.ch.Trace(ctx, runID, true)
	if err != nil {
		return
	}
	byID := map[string]string{}
	for _, s := range spans {
		desc := s.Name
		if args := s.Attributes["gen_ai.tool.call.arguments"]; args != "" {
			desc += " " + args
		}
		byID[s.SpanID] = desc
	}
	fmt.Printf("what step %d did:\n", from)
	for _, s := range steps {
		if s.Index >= from && s.Index < to {
			fmt.Printf("  %d %s %s\n", s.Index, s.Kind, byID[s.SpanID])
		}
	}
}

func runCompare(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	pgDSN := pgFlag(fs)
	fs.Parse(args)
	if fs.NArg() != 2 {
		return errors.New("usage: furcate compare <run-a> <run-b>")
	}
	pg, err := runstore.OpenPostgres(ctx, *pgDSN)
	if err != nil {
		return err
	}
	defer pg.Close()
	var runs [2]runstore.Run
	for i := range runs {
		if runs[i], err = pg.Run(ctx, fs.Arg(i)); err != nil {
			return fmt.Errorf("%s: %w", fs.Arg(i), err)
		}
	}
	a, b := runs[0], runs[1]
	dur := func(r runstore.Run) string {
		if r.FinishedAt.IsZero() {
			return "-"
		}
		return r.FinishedAt.Sub(r.StartedAt).Round(time.Millisecond).String()
	}
	origin := func(r runstore.Run) string {
		if r.ParentRunID == "" {
			return "original"
		}
		return fmt.Sprintf("branch of %s at step %d %s", short(r.ParentRunID), r.ParentStep, r.Change)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "\tA %s\tB %s\n", short(a.RunID), short(b.RunID))
	rows := [][3]string{
		{"origin", origin(a), origin(b)},
		{"outcome", orDash(a.Outcome), orDash(b.Outcome)},
		{"status", a.Status, b.Status},
		{"LLM calls", fmt.Sprint(a.Usage.LLMCalls), fmt.Sprint(b.Usage.LLMCalls)},
		{"input tokens (uncached)", fmt.Sprint(a.Usage.InputTokens), fmt.Sprint(b.Usage.InputTokens)},
		{"cache read tokens", fmt.Sprint(a.Usage.CacheReadTokens), fmt.Sprint(b.Usage.CacheReadTokens)},
		{"output tokens", fmt.Sprint(a.Usage.OutputTokens), fmt.Sprint(b.Usage.OutputTokens)},
		{"cost", fmt.Sprintf("$%.4f", a.Usage.CostUSD), fmt.Sprintf("$%.4f", b.Usage.CostUSD)},
		{"duration", dur(a), dur(b)},
	}
	for _, r := range rows {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", r[0], r[1], r[2])
	}
	tw.Flush()

	if a.FinalFiles == nil || b.FinalFiles == nil {
		fmt.Println("\nfiles: no final manifest for one of the runs")
		return nil
	}
	var onlyA, onlyB, changed []string
	for _, p := range controller.SortedPaths(a.FinalFiles) {
		hb, ok := b.FinalFiles[p]
		switch {
		case !ok:
			onlyA = append(onlyA, p)
		case hb != a.FinalFiles[p]:
			changed = append(changed, p)
		}
	}
	for _, p := range controller.SortedPaths(b.FinalFiles) {
		if _, ok := a.FinalFiles[p]; !ok {
			onlyB = append(onlyB, p)
		}
	}
	fmt.Printf("\nfiles at the end: %d in A, %d in B; %d differ, %d only in A, %d only in B\n",
		len(a.FinalFiles), len(b.FinalFiles), len(changed), len(onlyA), len(onlyB))
	for _, p := range changed {
		fmt.Println("  ~ " + p)
	}
	for _, p := range onlyA {
		fmt.Println("  - " + p + "  (only in A)")
	}
	for _, p := range onlyB {
		fmt.Println("  + " + p + "  (only in B)")
	}
	return nil
}

func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func runGC(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("gc", flag.ExitOnError)
	pgDSN := pgFlag(fs)
	drv := fs.String("driver", envOr("FURCATE_DRIVER", "docker"), "the driver whose checkpoints to delete")
	olderThan := fs.Duration("older-than", 24*time.Hour, "delete unpinned checkpoints older than this")
	dry := fs.Bool("dry-run", false, "list what would be deleted")
	fs.Parse(args)
	// Named runs lose every unpinned checkpoint, whatever its age: the runs of a
	// sweep you don't need to fork.
	runs := map[string]bool{}
	for _, id := range fs.Args() {
		runs[id] = true
	}
	cutoff, what := time.Now().Add(-*olderThan), "older than "+olderThan.String()
	if len(runs) > 0 {
		cutoff, what = time.Now(), fmt.Sprintf("of %d runs", len(runs))
	}
	pg, err := runstore.OpenPostgres(ctx, *pgDSN)
	if err != nil {
		return err
	}
	defer pg.Close()
	cps, err := pg.Expirable(ctx, cutoff)
	if err != nil {
		return err
	}
	var mine []runstore.Checkpoint
	for _, c := range cps {
		if c.Driver == *drv && (len(runs) == 0 || runs[c.RunID]) {
			mine = append(mine, c)
		}
	}
	if *dry || len(mine) == 0 {
		fmt.Printf("%d unpinned %s checkpoints %s\n", len(mine), *drv, what)
		return nil
	}
	sb, err := newDriver(*drv)
	if err != nil {
		return err
	}
	deleted, kept := 0, 0
	for _, c := range mine {
		if err := sb.DeleteCheckpoint(ctx, driver.CheckpointID(c.DriverRef)); err != nil {
			kept++ // in use by a running sandbox, or the driver is unreachable: next time
			continue
		}
		if err := pg.MarkDeleted(ctx, c.CheckpointID); err != nil {
			return err
		}
		deleted++
	}
	fmt.Printf("deleted %d %s checkpoints; %d still in use, kept for next time\n", deleted, *drv, kept)
	return nil
}

func runDropIn(ctx context.Context, args []string) error {
	if len(args) == 0 || (args[0] != "run" && args[0] != "fork") {
		return errors.New("usage: furcate dropin run -task t.json | furcate dropin fork -step k [-n 5] [-append-system s] [-model m] <run-id>")
	}
	fs := flag.NewFlagSet("dropin "+args[0], flag.ExitOnError)
	f := addRunFlags(fs)
	taskPath := fs.String("task", "", "task spec with a dropin section (run)")
	step := fs.Int("step", 0, "LLM request to fork from (fork)")
	n := fs.Int("n", 5, "continuations (fork)")
	model := fs.String("model", "", "change: rewrite every request to this model (fork)")
	instruction := fs.String("append-system", "", "change: what the resumed agent is told (fork)")
	listen := fs.String("listen", envOr("FURCATE_DROPIN_LISTEN", "127.0.0.1:0"), "where each run's recording proxy listens")
	sandboxHost := fs.String("sandbox-host", envOr("FURCATE_DROPIN_HOST", "host.docker.internal"), "how the sandbox reaches the proxy")
	fs.Parse(args[1:])
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		return errors.New("drop-in needs ANTHROPIC_API_KEY on the host: the proxy adds it to the agent's requests")
	}
	e, err := openEnv(ctx, f)
	if err != nil {
		return err
	}
	defer e.cleanup()
	eng := &dropin.Engine{Driver: e.ctl.Driver, Store: e.pg, Tracer: e.ctl.Tracer, Prices: e.ctl.Prices, Slots: e.ctl.Slots,
		Log: e.ctl.Log, APIKey: key, Listen: *listen, SandboxHost: *sandboxHost}
	if args[0] == "run" {
		if *taskPath == "" {
			return errors.New("dropin run needs -task")
		}
		t, err := task.Load(*taskPath)
		if err != nil {
			return err
		}
		res := eng.Start(ctx, t)
		fmt.Printf("run %s  %s  outcome=%s  %d requests  $%.4f  %s\n", res.RunID, res.Status, orDash(res.Outcome),
			res.Usage.LLMCalls, res.Usage.CostUSD, res.Duration.Round(time.Millisecond))
		return res.Err
	}
	if fs.NArg() != 1 || *step <= 0 {
		return errors.New("usage: furcate dropin fork -step k [-n 5] [-append-system s] [-model m] <run-id>")
	}
	res, err := eng.Fork(ctx, replay.Request{RunID: fs.Arg(0), Step: *step, N: *n,
		Change: replay.Change{Model: *model, AppendSystem: *instruction}})
	if err != nil {
		return err
	}
	fmt.Printf("drop-in fork of %s at step %d: passed %d of %d (95%% CI %.2f-%.2f)  $%.4f\n",
		res.RunID, res.Step, res.Passes, res.Done, res.Lo, res.Hi, res.Usage.CostUSD)
	for _, b := range res.Branches {
		line := fmt.Sprintf("  %s  %s  %s", b.RunID, b.Status, orDash(b.Outcome))
		if b.Err != nil {
			line += "  " + b.Err.Error()
		}
		fmt.Println(line)
	}
	return nil
}
