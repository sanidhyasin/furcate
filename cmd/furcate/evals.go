package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"go.opentelemetry.io/otel/trace/noop"

	"github.com/sanidhyasin/furcate/internal/agent"
	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/evals"
	"github.com/sanidhyasin/furcate/internal/pricing"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
)

const evalUsage = `furcate eval: checkpoints as repeatable tests.

  furcate eval add -run <id> -step k -name <case>   save a checkpoint as a case (pins it)
  furcate eval list                                 cases in the run store
  furcate eval export -o evals/cases <case>...      write portable case bundles
  furcate eval run -cases evals/cases \
      -variant name=main -variant name=pr,system-file=task.json [-n 5]
`

func runEval(ctx context.Context, args []string) error {
	if len(args) == 0 {
		fmt.Print(evalUsage)
		return nil
	}
	switch args[0] {
	case "add":
		return evalAdd(ctx, args[1:])
	case "list":
		return evalList(ctx, args[1:])
	case "export":
		return evalExport(ctx, args[1:])
	case "run":
		return evalRun(ctx, args[1:])
	}
	fmt.Print(evalUsage)
	return fmt.Errorf("unknown eval command %q", args[0])
}

func evalAdd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("eval add", flag.ExitOnError)
	pgDSN := pgFlag(fs)
	run := fs.String("run", "", "run to cut the case from")
	step := fs.Int("step", 0, "LLM step to resume at")
	name := fs.String("name", "", "case id, e.g. fixture-delete-01")
	notes := fs.String("notes", "", "what this case tests")
	fs.Parse(args)
	if *run == "" || *step <= 0 || *name == "" {
		return errors.New("usage: furcate eval add -run <id> -step k -name <case> [-notes ...]")
	}
	pg, err := runstore.OpenPostgres(ctx, *pgDSN)
	if err != nil {
		return err
	}
	defer pg.Close()
	if err := evals.Add(ctx, pg, runstore.EvalCase{CaseID: *name, RunID: *run, Step: *step, Notes: *notes}); err != nil {
		return err
	}
	fmt.Printf("case %s: resume run %s at step %d; its checkpoint is pinned\n", *name, *run, *step)
	return nil
}

func evalList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("eval list", flag.ExitOnError)
	pgDSN := pgFlag(fs)
	fs.Parse(args)
	pg, err := runstore.OpenPostgres(ctx, *pgDSN)
	if err != nil {
		return err
	}
	defer pg.Close()
	cases, err := pg.EvalCases(ctx)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "CASE\tRUN\tSTEP\tCHECKPOINT\tNOTES")
	for _, c := range cases {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\n", c.CaseID, c.RunID, c.Step, c.CheckpointID, c.Notes)
	}
	return tw.Flush()
}

func evalExport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("eval export", flag.ExitOnError)
	f := addRunFlags(fs)
	out := fs.String("o", "evals/cases", "directory for the bundles")
	fs.Parse(args)
	if fs.NArg() == 0 {
		return errors.New("usage: furcate eval export [-o dir] <case>...")
	}
	e, err := openEnv(ctx, f)
	if err != nil {
		return err
	}
	defer e.cleanup()
	eng := &replay.Engine{Controller: e.ctl, Store: e.pg, Conversations: e.ch}
	for _, id := range fs.Args() {
		b, err := evals.Export(ctx, eng, id)
		if err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
		dir := filepath.Join(*out, id)
		if err := b.Write(dir); err != nil {
			return err
		}
		fmt.Printf("%s: %s (request %d bytes, files %d bytes)\n", id, dir, len(b.Request), len(b.Files))
	}
	return nil
}

// variantFlag collects -variant name=x,model=...,append-system=...,system=...,system-file=...
type variantFlag []evals.Variant

func (v *variantFlag) String() string { return fmt.Sprint(len(*v)) }

func (v *variantFlag) Set(s string) error {
	var out evals.Variant
	for _, part := range strings.Split(s, ",") {
		k, val, ok := strings.Cut(part, "=")
		if !ok {
			return fmt.Errorf("variant %q: want key=value pairs", s)
		}
		switch k {
		case "name":
			out.Name = val
		case "model":
			out.Change.Model = val
		case "append-system":
			out.Change.AppendSystem = val
		case "system":
			out.Change.System = &val
		case "system-file":
			sys, err := systemFromFile(val)
			if err != nil {
				return err
			}
			out.Change.System = &sys
		default:
			return fmt.Errorf("variant %q: unknown key %q", s, k)
		}
	}
	if out.Name == "" {
		return fmt.Errorf("variant %q needs a name", s)
	}
	*v = append(*v, out)
	return nil
}

// systemFromFile reads a system prompt: the "system" field of a task spec, or
// the whole file.
func systemFromFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var spec struct {
		System *string `json:"system"`
	}
	if json.Unmarshal(b, &spec) == nil && spec.System != nil {
		return *spec.System, nil
	}
	return string(b), nil
}

func evalRun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("eval run", flag.ExitOnError)
	f := addRunFlags(fs)
	cases := fs.String("cases", "evals/cases", "directory of case bundles")
	only := fs.String("case", "", "comma-separated case ids to run (default all)")
	n := fs.Int("n", 5, "continuations per case and variant")
	storeKind := fs.String("store", "postgres", `run store: "postgres" (with tracing to the collector) or "memory" (no stores, for CI)`)
	mdOut := fs.String("markdown", "", "also write the report as Markdown to this file (for a PR comment)")
	var variants variantFlag
	fs.Var(&variants, "variant", "name=x[,model=m][,append-system=s][,system=s][,system-file=path]; repeat; the first is the baseline")
	fs.Parse(args)
	if len(variants) == 0 {
		variants = variantFlag{{Name: "base"}}
	}
	var ids []string
	if *only != "" {
		ids = strings.Split(*only, ",")
	}
	bundles, err := evals.LoadAll(*cases, ids...)
	if err != nil {
		return err
	}

	var eng *replay.Engine
	switch *storeKind {
	case "postgres":
		e, err := openEnv(ctx, f)
		if err != nil {
			return err
		}
		defer e.cleanup()
		eng = &replay.Engine{Controller: e.ctl, Store: e.pg, Conversations: e.ch}
	case "memory":
		sb, err := newDriver(*f.driver)
		if err != nil {
			return err
		}
		prices, err := pricing.Load(*f.prices)
		if err != nil {
			return err
		}
		store := runstore.NewMemory()
		ctl := &controller.Controller{Driver: sb, Model: agent.NewAnthropic(!*f.noFallbacks), Store: store,
			Tracer: noop.NewTracerProvider().Tracer("furcate"), Log: slog.New(slog.NewTextHandler(os.Stderr, nil)),
			Prices: prices, Slots: controller.NewSlots(*f.maxSandboxes)}
		eng = &replay.Engine{Controller: ctl, Store: store}
	default:
		return fmt.Errorf("unknown -store %q", *storeKind)
	}

	rep, err := evals.RunSuite(ctx, eng, bundles, variants, *n, evals.NewBatchID(),
		func(format string, a ...any) { fmt.Fprintf(os.Stderr, format+"\n", a...) })
	if err != nil {
		return err
	}
	fmt.Println(rep.Text())
	if *mdOut != "" {
		if err := os.WriteFile(*mdOut, []byte(rep.Markdown()), 0o644); err != nil {
			return err
		}
	}
	for _, c := range rep.Cells {
		if c.Err != nil && c.Done == 0 {
			return fmt.Errorf("case %s, variant %s: %w", c.Case, c.Variant, c.Err)
		}
	}
	return nil
}
