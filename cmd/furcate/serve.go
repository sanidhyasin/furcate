package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/sanidhyasin/furcate/internal/dropin"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/server"
	"github.com/sanidhyasin/furcate/internal/store"
	"github.com/sanidhyasin/furcate/internal/task"
)

// runServe serves the web UI and its JSON API. Without -driver it only reads the
// stores; with one, the UI can fork runs, which needs what `furcate fork` needs:
// the driver, ANTHROPIC_API_KEY and a collector for the branches' spans.
func runServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:7070", "address for the UI and API")
	f := runFlags{dsn: dsnFlag(fs), pg: pgFlag(fs)}
	f.driver = fs.String("driver", "", "sandbox driver for forks from the UI (docker, tine); empty: read-only")
	f.otlp = fs.String("otlp", envOr("FURCATE_OTLP", "http://localhost:4318"), "collector OTLP/HTTP base URL, for fork spans (env FURCATE_OTLP)")
	f.prices = fs.String("prices", "", "JSON price table on top of the built-in Claude prices")
	f.noFallbacks = fs.Bool("no-fallbacks", false, "don't let the API re-serve a refused request with a fallback model")
	f.maxSandboxes = fs.Int("max-sandboxes", 16, "most sandboxes this process runs at once; forks queue for a slot")
	f.keep = new(bool)
	maxN := fs.Int("max-n", 50, "most continuations one fork from the UI may ask for")
	fs.Parse(args)
	if fs.NArg() != 0 {
		return errors.New("usage: furcate serve [-listen 127.0.0.1:7070] [-driver docker] [flags]")
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg := server.Config{Log: log, MaxN: *maxN}

	mode := "read-only"
	if *f.driver != "" {
		e, err := openEnv(ctx, f)
		if err != nil {
			return err
		}
		defer e.cleanup()
		cfg.Traces, cfg.Runs = e.ch, e.pg
		cfg.Forker = &serveForker{store: e.pg, env: e,
			replay: &replay.Engine{Controller: e.ctl, Store: e.pg, Conversations: e.ch}}
		mode = "forks on " + *f.driver
	} else {
		ch, err := store.Open(ctx, *f.dsn)
		if err != nil {
			return err
		}
		defer ch.Close()
		cfg.Traces = ch
		if *f.pg != "" {
			pg, err := runstore.OpenPostgres(ctx, *f.pg)
			if err != nil {
				return fmt.Errorf("%w (-pg '' serves traces without run state)", err)
			}
			defer pg.Close()
			cfg.Runs = pg
		} else {
			mode = "read-only, traces only"
		}
	}

	srv := server.New(cfg)
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		srv.Close()
		return err
	}
	hs := &http.Server{Handler: srv, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: time.Minute}
	fmt.Printf("furcate UI on http://%s  (%s)\n", ln.Addr(), mode)
	errc := make(chan error, 1)
	go func() { errc <- hs.Serve(ln) }()
	select {
	case err := <-errc:
		srv.Close()
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hs.Shutdown(sctx)
	srv.Close() // cancels forks still running; their branches end as errors
	return nil
}

// runSnapshot writes the web UI and the given runs, with all their branches, as
// static files any web host can serve: a read-only demo with no server behind it.
func runSnapshot(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	dsn, pg := dsnFlag(fs), pgFlag(fs)
	out := fs.String("o", "", "directory to write (must be empty or missing)")
	featured := fs.String("featured", "", "run the banner points to (default: the first given)")
	note := fs.String("note", "", "banner text: what these runs are")
	home := fs.String("home", "https://github.com/sanidhyasin/furcate", "where the banner links Furcate's name")
	fs.Parse(args)
	if *out == "" || fs.NArg() == 0 {
		return errors.New("usage: furcate snapshot -o <dir> [-note text] [-featured run-id] [-home url] <run-id>...")
	}
	ch, err := store.Open(ctx, *dsn)
	if err != nil {
		return err
	}
	defer ch.Close()
	rs, err := runstore.OpenPostgres(ctx, *pg)
	if err != nil {
		return err
	}
	defer rs.Close()
	start := time.Now()
	st, err := server.Snapshot(ctx, server.Config{Traces: ch, Runs: rs}, *out, fs.Args(),
		server.SnapshotOptions{Featured: *featured, Note: *note, Home: *home})
	if err != nil {
		return err
	}
	fmt.Printf("wrote %s: %d runs listed, %d in all with their branches, %d payloads, %.1f MB, in %s\n",
		*out, st.Listed, st.Runs, st.Payloads, float64(st.Bytes)/(1<<20), time.Since(start).Round(time.Millisecond))
	return nil
}

// serveForker sends a fork to the engine that can resume the run: the drop-in
// engine for tasks with a dropin section, the replay engine for the rest.
type serveForker struct {
	store  runstore.Store
	env    *env
	replay *replay.Engine
}

func (f *serveForker) Fork(ctx context.Context, req replay.Request) (*replay.Result, error) {
	run, err := f.store.Run(ctx, req.RunID)
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", req.RunID, err)
	}
	t, err := task.Parse(run.Task)
	if err != nil {
		return nil, err
	}
	if t.DropIn == nil {
		return f.replay.Fork(ctx, req)
	}
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		return nil, errors.New("drop-in forks need ANTHROPIC_API_KEY on the host: the proxy adds it to the agent's requests")
	}
	ctl := f.env.ctl
	eng := &dropin.Engine{Driver: ctl.Driver, Store: f.store, Tracer: ctl.Tracer, Prices: ctl.Prices, Slots: ctl.Slots,
		Log: ctl.Log, APIKey: key, Listen: envOr("FURCATE_DROPIN_LISTEN", "127.0.0.1:0"),
		SandboxHost: envOr("FURCATE_DROPIN_HOST", "host.docker.internal")}
	return eng.Fork(ctx, req)
}
