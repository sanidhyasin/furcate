package runstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
)

// Runs only with FURCATE_TEST_PG set to a DSN, e.g. the docker-compose one.
func TestPostgresStore(t *testing.T) {
	dsn := os.Getenv("FURCATE_TEST_PG")
	if dsn == "" {
		t.Skip("set FURCATE_TEST_PG to run against Postgres")
	}
	ctx := context.Background()
	pg, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	b := make([]byte, 8)
	rand.Read(b)
	id := "test" + hex.EncodeToString(b)
	task := []byte(`{"id":"t"}`)

	if err := pg.CreateRun(ctx, Run{RunID: id, TaskID: "t", Task: task, Driver: "testdir"}); err != nil {
		t.Fatal(err)
	}
	cp := Checkpoint{CheckpointID: "cp_" + id, RunID: id, Step: 1, Driver: "testdir", Level: 2, DriverRef: "ref1"}
	if err := pg.AddCheckpoint(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err := pg.AddStep(ctx, Step{RunID: id, Index: 1, SpanID: "s1", Kind: KindLLM, CheckpointID: cp.CheckpointID}); err != nil {
		t.Fatal(err)
	}
	if err := pg.AddStep(ctx, Step{RunID: id, Index: 2, SpanID: "s2", Kind: KindTool}); err != nil {
		t.Fatal(err)
	}
	if err := pg.FinishRun(ctx, id, End{Status: StatusDone, Outcome: OutcomeFail,
		Usage:      Usage{LLMCalls: 3, InputTokens: 10, OutputTokens: 5, CostUSD: 0.25},
		FinalFiles: map[string]string{"./a.txt": "abc"}}); err != nil {
		t.Fatal(err)
	}

	child := id + "-b"
	if err := pg.CreateRun(ctx, Run{RunID: child, TaskID: "t", Task: task, Driver: "testdir",
		ParentRunID: id, ParentStep: 1, Change: []byte(`{"model":"m"}`)}); err != nil {
		t.Fatal(err)
	}

	r, err := pg.Run(ctx, id)
	if err != nil || r.Status != StatusDone || r.Outcome != OutcomeFail || r.ParentRunID != "" || r.FinishedAt.IsZero() ||
		r.Usage.LLMCalls != 3 || r.Usage.CostUSD != 0.25 || r.FinalFiles["./a.txt"] != "abc" {
		t.Fatalf("run: %+v %v", r, err)
	}
	steps, _ := pg.Steps(ctx, id)
	if len(steps) != 2 || steps[0].CheckpointID != cp.CheckpointID || steps[1].CheckpointID != "" {
		t.Fatalf("steps: %+v", steps)
	}
	got, err := pg.Checkpoint(ctx, cp.CheckpointID)
	if err != nil || got.DriverRef != "ref1" || got.Level != 2 {
		t.Fatalf("checkpoint: %+v %v", got, err)
	}
	kids, _ := pg.Children(ctx, id)
	if len(kids) != 1 || kids[0].ParentStep != 1 || string(kids[0].Change) != `{"model": "m"}` {
		t.Fatalf("children: %+v", kids)
	}
	if _, err := pg.Step(ctx, id, 9); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing step: %v", err)
	}
}
