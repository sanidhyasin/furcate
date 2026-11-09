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
	"strings"
	"text/tabwriter"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/sanidhyasin/furcate/internal/agent"
	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/driver"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/stats"
	"github.com/sanidhyasin/furcate/internal/store"
	"github.com/sanidhyasin/furcate/internal/task"
)

// drivers lists the sandbox drivers the CLI can use. The test-only directory
// driver is deliberately absent: it has no isolation.
var drivers = map[string]func() (driver.Sandbox, error){}

func newDriver(name string) (driver.Sandbox, error) {
	mk, ok := drivers[name]
	if !ok {
		var names []string
		for n := range drivers {
			names = append(names, n)
		}
		slices.Sort(names)
		if len(names) == 0 {
			return nil, errors.New("no sandbox driver is built in yet: the tine driver lands once tine can " +
				"restore VMs from saved snapshots (see README, Phase 2)")
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

func openEnv(ctx context.Context, dsn, pgDSN, otlp, driverName string, fallbacks, keep bool) (*env, error) {
	sb, err := newDriver(driverName)
	if err != nil {
		return nil, err
	}
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
		Driver: sb, Model: agent.NewAnthropic(fallbacks), Store: pg, Tracer: tp.Tracer("furcate"),
		Log: slog.New(slog.NewTextHandler(os.Stderr, nil)), Keep: keep,
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
	dsn, pg, otlp, driver *string
	noFallbacks, keep     *bool
}

func addRunFlags(fs *flag.FlagSet) runFlags {
	return runFlags{
		dsn: dsnFlag(fs), pg: pgFlag(fs),
		otlp:        fs.String("otlp", envOr("FURCATE_OTLP", "http://localhost:4318"), "collector OTLP/HTTP base URL (env FURCATE_OTLP)"),
		driver:      fs.String("driver", envOr("FURCATE_DRIVER", "tine"), "sandbox driver (env FURCATE_DRIVER)"),
		noFallbacks: fs.Bool("no-fallbacks", false, "don't let the API re-serve a refused request with a fallback model"),
		keep:        fs.Bool("keep", false, "leave sandboxes running after the run, for inspection"),
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
	e, err := openEnv(ctx, *f.dsn, *f.pg, *f.otlp, *f.driver, !*f.noFallbacks, *f.keep)
	if err != nil {
		return err
	}
	defer e.cleanup()
	res := e.ctl.Start(ctx, t)
	fmt.Printf("run %s  %s  outcome=%s  steps=%d\n", res.RunID, res.Status, orDash(res.Outcome), res.Steps)
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
	e, err := openEnv(ctx, *f.dsn, *f.pg, *f.otlp, *f.driver, !*f.noFallbacks, *f.keep)
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
	fmt.Printf("passed %d of %d  (95%% CI %.2f-%.2f)  in %s\n", res.Passes, res.Done, res.Lo, res.Hi,
		time.Since(start).Round(time.Millisecond))
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
	fmt.Fprintln(tw, "STEP\tCHANGE\tPASSED\tRATE\t95% CI\tERRORS")
	for _, k := range keys {
		g := groups[k]
		lo, hi := stats.Wilson(g.pass, g.done, stats.Z95)
		rate := 0.0
		if g.done > 0 {
			rate = float64(g.pass) / float64(g.done)
		}
		fmt.Fprintf(tw, "%d\t%s\t%d/%d\t%.0f%%\t%.2f-%.2f\t%d\n", k.step, k.change, g.pass, g.done, rate*100, lo, hi, g.errs)
	}
	return tw.Flush()
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
