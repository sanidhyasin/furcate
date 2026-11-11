// Command furcate is the collector, the load generator and the CLI in one binary.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/sanidhyasin/furcate/internal/collector"
	"github.com/sanidhyasin/furcate/internal/loadgen"
	"github.com/sanidhyasin/furcate/internal/pricing"
	"github.com/sanidhyasin/furcate/internal/store"
	"github.com/sanidhyasin/furcate/internal/tracetree"
)

const usage = `furcate: agent traces you can fork from any step.

Usage:
  furcate collector [flags]        run the OTLP collector (HTTP :4318, gRPC :4317)
  furcate migrate [flags]          create the ClickHouse tables
  furcate runs [flags]             list recent runs
  furcate trace [flags] <trace-id> print one run as a tree
  furcate stats [flags]            LLM calls by model: count, latency, tokens, cost
  furcate blob [flags] <hash>      print one stored payload
  furcate loadgen [flags]          send synthetic agent runs to a collector
  furcate bench [flags]            time trace loads and stats queries

  furcate run -task t.json         run an agent, checkpointing every step
  furcate steps <run-id>           a run's steps and their checkpoints
  furcate fork -step k <run-id>    run N continuations from step k, with a change
  furcate branches <run-id>        pass rates of a run's branches
  furcate bisect <run-id>          find the step where a run went wrong
  furcate compare <run-a> <run-b>  outcome, cost and final files of two runs
  furcate eval ...                 checkpoints as test cases: add, list, export, run
  furcate gc [run-id...]           delete unpinned checkpoints: old ones, or these runs
  furcate dropin run|fork ...      record and fork an unmodified agent (Claude Code)
  furcate serve [-driver docker]   web UI and HTTP API on 127.0.0.1:7070
  furcate snapshot -o dir <run-id>...  the web UI and these runs as static files
  furcate version                  print the version

Run "furcate <command> -h" for a command's flags.
`

// version is set in release builds (-ldflags "-X main.version=v0.1.0"); a
// go install build reports its module version instead.
var version = ""

func versionString() string {
	if version != "" {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return "dev"
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "collector":
		err = runCollector(ctx, args)
	case "migrate":
		err = runMigrate(ctx, args)
	case "runs":
		err = runRuns(ctx, args)
	case "trace":
		err = runTrace(ctx, args)
	case "stats":
		err = runStats(ctx, args)
	case "blob":
		err = runBlob(ctx, args)
	case "loadgen":
		err = runLoadgen(ctx, args)
	case "bench":
		err = runBench(ctx, args)
	case "run":
		err = runRun(ctx, args)
	case "steps":
		err = runSteps(ctx, args)
	case "fork":
		err = runFork(ctx, args)
	case "branches":
		err = runBranches(ctx, args)
	case "bisect":
		err = runBisect(ctx, args)
	case "compare":
		err = runCompare(ctx, args)
	case "eval":
		err = runEval(ctx, args)
	case "gc":
		err = runGC(ctx, args)
	case "dropin":
		err = runDropIn(ctx, args)
	case "serve":
		err = runServe(ctx, args)
	case "snapshot":
		err = runSnapshot(ctx, args)
	case "version", "-version", "--version":
		fmt.Println("furcate", versionString())
	case "-h", "--help", "help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "furcate:", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func dsnFlag(fs *flag.FlagSet) *string {
	return fs.String("clickhouse", envOr("FURCATE_CLICKHOUSE", store.DefaultDSN), "ClickHouse DSN (env FURCATE_CLICKHOUSE)")
}

func runCollector(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("collector", flag.ExitOnError)
	dsn := dsnFlag(fs)
	var cfg collector.Config
	fs.StringVar(&cfg.HTTPAddr, "http", "127.0.0.1:4318", "OTLP/HTTP listen address (\":4318\" for every interface)")
	fs.StringVar(&cfg.GRPCAddr, "grpc", "127.0.0.1:4317", `OTLP/gRPC listen address ("off" to disable)`)
	fs.StringVar(&cfg.WALDir, "wal-dir", envOr("FURCATE_WAL_DIR", "data/wal"), "write-ahead log directory")
	fs.IntVar(&cfg.BatchSize, "batch-size", 10_000, "insert into ClickHouse at this many spans")
	fs.IntVar(&cfg.Inserters, "inserters", 4, "ClickHouse inserts running at once")
	fs.DurationVar(&cfg.FlushInterval, "flush-interval", time.Second, "or after this long")
	fs.IntVar(&cfg.QueueSize, "queue", 1024, "export requests queued before returning 503")
	fs.Float64Var(&cfg.SampleRatio, "sample", 1, "fraction of ordinary runs to keep (errored and checkpointed runs are always kept)")
	fs.DurationVar(&cfg.QuietPeriod, "quiet", 30*time.Second, "a run is complete once its root ended and it was quiet this long")
	fs.DurationVar(&cfg.MaxTraceAge, "max-trace-age", 30*time.Minute, "decide on a run after this long regardless")
	fs.IntVar(&cfg.PayloadThreshold, "payload-threshold", 2048, "string attributes longer than this become payloads")
	prices := fs.String("prices", "", "JSON price table for cost (see internal/pricing)")
	migrate := fs.Bool("migrate", true, "create tables on start")
	fs.Parse(args)

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	table, err := pricing.Load(*prices)
	if err != nil {
		return fmt.Errorf("prices: %w", err)
	}
	cfg.Prices = table
	if *migrate {
		if err := store.Migrate(ctx, *dsn); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	ch, err := store.Open(ctx, *dsn)
	if err != nil {
		return err
	}
	defer ch.Close()
	c, err := collector.New(cfg, ch, log)
	if err != nil {
		return err
	}
	return c.Run(ctx)
}

func runMigrate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	dsn := dsnFlag(fs)
	fs.Parse(args)
	if err := store.Migrate(ctx, *dsn); err != nil {
		return err
	}
	fmt.Println("schema is up to date")
	return nil
}

func runRuns(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("runs", flag.ExitOnError)
	dsn := dsnFlag(fs)
	since := fs.Duration("since", 24*time.Hour, "look back this far")
	limit := fs.Int("limit", 20, "max runs")
	fs.Parse(args)
	ch, err := store.Open(ctx, *dsn)
	if err != nil {
		return err
	}
	defer ch.Close()
	runs, err := ch.Runs(ctx, *since, *limit)
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		fmt.Println("no finished runs in that window")
		return nil
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TRACE\tNAME\tSTARTED\tDURATION\tSPANS\tTOKENS\tCOST\tSTATUS")
	for _, r := range runs {
		status := r.Status
		if r.Errors > 0 {
			status = fmt.Sprintf("%s (%d err)", status, r.Errors)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d\t$%.4f\t%s\n", r.TraceID, r.Name, r.Start.Local().Format("Jan 02 15:04:05"),
			r.Duration.Round(time.Millisecond), r.Spans, r.Tokens, r.CostUSD, status)
	}
	return tw.Flush()
}

func runTrace(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("trace", flag.ExitOnError)
	dsn := dsnFlag(fs)
	noIndex := fs.Bool("no-index", false, "scan spans without the trace_index time range")
	payloads := fs.Bool("payloads", false, "list each span's payloads with a preview")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: furcate trace [flags] <trace-id>")
	}
	id := strings.ToLower(fs.Arg(0))
	ch, err := store.Open(ctx, *dsn)
	if err != nil {
		return err
	}
	defer ch.Close()
	start := time.Now()
	spans, err := ch.Trace(ctx, id, !*noIndex)
	if err != nil {
		return err
	}
	took := time.Since(start)
	tree := tracetree.Build(spans)
	tracetree.Render(os.Stdout, id, tree)
	fmt.Fprintf(os.Stderr, "loaded %d spans in %s (index: %v)\n", len(spans), took.Round(time.Microsecond), !*noIndex)

	if *payloads {
		fmt.Println("\npayloads:")
		sort.Slice(spans, func(i, j int) bool { return spans[i].Start.Before(spans[j].Start) })
		for _, s := range spans {
			keys := make([]string, 0, len(s.Payloads))
			for k := range s.Payloads {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, k := range keys {
				h := s.Payloads[k]
				data, err := ch.Payload(ctx, h)
				preview := ""
				if err != nil {
					preview = "(" + err.Error() + ")"
				} else {
					preview = strings.Join(strings.Fields(string(data)), " ")
					if len(preview) > 100 {
						preview = preview[:100] + "…"
					}
					preview = fmt.Sprintf("%d B  %s", len(data), preview)
				}
				fmt.Printf("  %s  %-28s %s  %s\n", s.SpanID, k, h[:12], preview)
			}
		}
	}
	return nil
}

func runStats(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	dsn := dsnFlag(fs)
	since := fs.Duration("since", 24*time.Hour, "look back this far")
	raw := fs.Bool("raw", false, "scan raw spans instead of the per-minute rollups")
	fs.Parse(args)
	ch, err := store.Open(ctx, *dsn)
	if err != nil {
		return err
	}
	defer ch.Close()
	start := time.Now()
	rows, err := ch.Stats(ctx, *since, !*raw)
	if err != nil {
		return err
	}
	took := time.Since(start)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "MODEL\tCALLS\tERRORS\tP50\tP95\tP99\tTOKENS IN\tTOKENS OUT\tCOST")
	for _, r := range rows {
		model := r.Model
		if model == "" {
			model = "(none)"
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\t%s\t%d\t%d\t$%.4f\n", model, r.Spans, r.Errors,
			r.P50.Round(time.Millisecond), r.P95.Round(time.Millisecond), r.P99.Round(time.Millisecond),
			r.TokensIn, r.TokensOut, r.CostUSD)
	}
	tw.Flush()
	src := "rollups"
	if *raw {
		src = "raw spans"
	}
	fmt.Fprintf(os.Stderr, "query took %s (%s)\n", took.Round(time.Microsecond), src)
	return nil
}

func runBlob(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("blob", flag.ExitOnError)
	dsn := dsnFlag(fs)
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: furcate blob [flags] <sha256>")
	}
	ch, err := store.Open(ctx, *dsn)
	if err != nil {
		return err
	}
	defer ch.Close()
	data, err := ch.Payload(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(data)
	return err
}

func runLoadgen(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("loadgen", flag.ExitOnError)
	var cfg loadgen.Config
	fs.StringVar(&cfg.Endpoint, "endpoint", "http://localhost:4318", "collector OTLP/HTTP base URL")
	fs.DurationVar(&cfg.Duration, "duration", 0, "send for this long")
	fs.IntVar(&cfg.Runs, "runs", 0, "send this many runs (default 100 if -duration is unset)")
	fs.IntVar(&cfg.Concurrency, "concurrency", 8, "parallel senders")
	fs.IntVar(&cfg.Steps, "steps", 10, "LLM steps per run (each followed by a tool step)")
	fs.Float64Var(&cfg.PayloadScale, "payload-scale", 1, "multiply every payload size")
	fs.Float64Var(&cfg.ErrorRate, "error-rate", 0.05, "fraction of runs that fail")
	fs.IntVar(&cfg.BatchSpans, "batch-spans", 512, "max spans per export request")
	fs.BoolVar(&cfg.Gzip, "gzip", false, "gzip request bodies")
	fs.BoolVar(&cfg.Checkpoints, "checkpoints", false, "tag each step with a checkpoint id")
	fs.Parse(args)
	res, err := loadgen.Run(ctx, cfg)
	fmt.Println(res)
	return err
}

func runBench(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	dsn := dsnFlag(fs)
	n := fs.Int("n", 50, "trace loads per mode")
	since := fs.Duration("since", 24*time.Hour, "pick traces and stats from this window")
	fs.Parse(args)
	ch, err := store.Open(ctx, *dsn)
	if err != nil {
		return err
	}
	defer ch.Close()

	total, err := ch.Count(ctx)
	if err != nil {
		return err
	}
	runs, err := ch.Runs(ctx, *since, 1000)
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		return errors.New("no runs to benchmark; send some with furcate loadgen")
	}
	fmt.Printf("spans table: %d rows; sampling from %d recent runs\n\n", total, len(runs))

	time1 := func(f func() error) (time.Duration, error) {
		s := time.Now()
		err := f()
		return time.Since(s), err
	}
	report := func(label string, ds []time.Duration) {
		slices.Sort(ds)
		q := func(p float64) time.Duration { return ds[min(len(ds)-1, int(p*float64(len(ds))))] }
		fmt.Printf("%-28s n=%-4d p50 %-10s p95 %-10s p99 %s\n", label, len(ds),
			q(0.5).Round(time.Microsecond), q(0.95).Round(time.Microsecond), q(0.99).Round(time.Microsecond))
	}

	if err := printStorage(ctx, ch); err != nil {
		return err
	}

	for _, useIndex := range []bool{true, false} {
		var ds []time.Duration
		for i := 0; i < *n; i++ {
			id := runs[rand.IntN(len(runs))].TraceID
			d, err := time1(func() error { _, err := ch.Trace(ctx, id, useIndex); return err })
			if err != nil {
				return err
			}
			ds = append(ds, d)
		}
		label := "trace load, with index"
		if !useIndex {
			label = "trace load, full scan"
		}
		report(label, ds)
	}
	for _, rollups := range []bool{true, false} {
		var ds []time.Duration
		for i := 0; i < max(*n/5, 5); i++ {
			d, err := time1(func() error { _, err := ch.Stats(ctx, *since, rollups); return err })
			if err != nil {
				return err
			}
			ds = append(ds, d)
		}
		label := "stats by model, rollups"
		if !rollups {
			label = "stats by model, raw spans"
		}
		report(label, ds)
	}
	return nil
}

func printStorage(ctx context.Context, ch *store.CH) error {
	sizes, err := ch.Storage(ctx)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TABLE\tROWS\tON DISK\tUNCOMPRESSED\tRATIO\tBYTES/ROW")
	var spanRows, spanDisk, payloadDisk uint64
	for _, t := range sizes {
		ratio := 0.0
		if t.Compressed > 0 {
			ratio = float64(t.Uncompressed) / float64(t.Compressed)
		}
		perRow := 0.0
		if t.Rows > 0 {
			perRow = float64(t.Compressed) / float64(t.Rows)
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%.1fx\t%.0f\n", t.Table, t.Rows, mib(t.Compressed), mib(t.Uncompressed), ratio, perRow)
		switch t.Table {
		case "spans":
			spanRows, spanDisk = t.Rows, t.Compressed
		case "payloads":
			payloadDisk = t.Compressed
		}
	}
	tw.Flush()
	refs, unique, refBytes, uniqueBytes, err := ch.PayloadDedupe(ctx)
	if err != nil {
		return err
	}
	if unique > 0 && spanRows > 0 {
		fmt.Printf("\npayload dedupe: %d references -> %d stored (%.1fx), %s -> %s\n",
			refs, unique, float64(refs)/float64(unique), mib(refBytes), mib(uniqueBytes))
		fmt.Printf("on disk per span, payloads included: %.0f B\n\n", float64(spanDisk+payloadDisk)/float64(spanRows))
	}
	return nil
}

func mib(b uint64) string { return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20)) }
