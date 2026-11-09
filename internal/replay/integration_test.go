package replay

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/driver/testdir"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/store"
	"github.com/sanidhyasin/furcate/internal/task"
)

// The whole chain on real stores: spans over OTLP to a running collector, run
// state in Postgres, and the fork reading step k's request back out of
// ClickHouse. Needs `docker compose up` and `furcate collector`; enable with
// FURCATE_TEST_E2E=1.
func TestForkThroughCollectorAndStores(t *testing.T) {
	if os.Getenv("FURCATE_TEST_E2E") == "" {
		t.Skip("set FURCATE_TEST_E2E=1 with the collector and docker compose running")
	}
	ctx := context.Background()
	ch, err := store.Open(ctx, store.DefaultDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	pg, err := runstore.OpenPostgres(ctx, runstore.DefaultDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL("http://localhost:4318/v1/traces"))
	if err != nil {
		t.Fatal(err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(100*time.Millisecond)),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName("furcate-agent-test"))))
	defer tp.Shutdown(ctx)

	drv, err := testdir.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctl := &controller.Controller{
		Driver: drv, Model: &scriptModel{}, Store: pg, Tracer: tp.Tracer("e2e"),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	tk, _ := task.Parse([]byte(`{"id":"keep-fixture","prompt":"Tidy the repo.","check":["sh","-c","test -f fixture.txt"]}`))

	orig := ctl.Start(ctx, tk)
	if orig.Err != nil || orig.Outcome != runstore.OutcomeFail {
		t.Fatalf("original: %+v", orig)
	}
	tp.ForceFlush(ctx)

	eng := &Engine{Controller: ctl, Store: pg, Conversations: ch, RequestWait: 20 * time.Second}
	start := time.Now()
	res, err := eng.Fork(ctx, Request{RunID: orig.RunID, Step: 5, N: 4, Change: Change{AppendSystem: "Never delete fixture.txt."}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("fork waited for the trace store and ran 4 branches in %s", time.Since(start).Round(time.Millisecond))
	if res.Passes != 4 || res.Done != 4 {
		t.Fatalf("passed %d/%d, want 4/4", res.Passes, res.Done)
	}
	tp.ForceFlush(ctx)

	// Each branch is its own trace in ClickHouse, numbered on from step 5.
	b := res.Branches[0].RunID
	deadline := time.Now().Add(20 * time.Second)
	for {
		spans, err := ch.Trace(ctx, b, true)
		if err == nil {
			for _, s := range spans {
				if s.IsRoot() && s.Attributes["furcate.parent_run_id"] == orig.RunID && s.Attributes["furcate.parent_step"] == "5" {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("branch %s never showed up linked to its parent: %v", b, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
