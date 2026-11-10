package evals

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/anthropics/anthropic-sdk-go/v2"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/driver/testdir"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/task"
)

// careless deletes the fixture at its third turn unless a system prompt,
// top-level or appended, says not to.
type careless struct{ ids atomic.Int64 }

func (m *careless) Send(_ context.Context, body []byte) (*anthropic.Message, error) {
	var p anthropic.MessageNewParams
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, err
	}
	turn, guarded := 0, false
	for _, s := range p.System {
		guarded = guarded || strings.Contains(s.Text, "Never delete")
	}
	for _, msg := range p.Messages {
		if msg.OfAssistant != nil {
			turn++
		}
		if msg.OfSystem != nil && strings.Contains(msg.OfSystem.Content[0].Text, "Never delete") {
			guarded = true
		}
	}
	tool := func(name, in string) string {
		return fmt.Sprintf(`{"type":"tool_use","id":"toolu_%d","name":%q,"input":%s}`, m.ids.Add(1), name, in)
	}
	content, stop := `{"type":"text","text":"done"}`, "end_turn"
	switch {
	case turn == 0:
		content, stop = tool("bash", `{"command":"echo data > fixture.txt"}`), "tool_use"
	case turn == 1:
		content, stop = tool("write_file", `{"path":"notes.md","content":"plan"}`), "tool_use"
	case turn == 2 && !guarded:
		content, stop = tool("bash", `{"command":"rm fixture.txt"}`), "tool_use"
	}
	raw := fmt.Sprintf(`{"id":"msg_%d","type":"message","role":"assistant","model":%q,"content":[%s],`+
		`"stop_reason":%q,"stop_sequence":null,"usage":{"input_tokens":500,"output_tokens":50}}`, m.ids.Add(1), p.Model, content, stop)
	var msg anthropic.Message
	return &msg, json.Unmarshal([]byte(raw), &msg)
}

type recorded struct{ rec *tracetest.SpanRecorder }

func (r recorded) LLMRequest(_ context.Context, _, spanID string) ([]byte, error) {
	for _, s := range r.rec.Ended() {
		if s.SpanContext().SpanID().String() == spanID {
			for _, a := range s.Attributes() {
				if string(a.Key) == controller.AttrLLMRequest {
					return []byte(a.Value.AsString()), nil
				}
			}
		}
	}
	return nil, errors.New("not recorded")
}

func TestCaseExportRestoreAndSuite(t *testing.T) {
	ctx := context.Background()
	drv, err := testdir.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	store := runstore.NewMemory()
	ctl := &controller.Controller{Driver: drv, Model: &careless{}, Store: store, Tracer: tp.Tracer("t"),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Slots: controller.NewSlots(4)}
	eng := &replay.Engine{Controller: ctl, Store: store, Conversations: recorded{rec}}
	// Setup reads the task's files, so a restore must run it on them.
	tk, _ := task.Parse([]byte(`{"id":"keep","system":"You tidy repos.","prompt":"go",` +
		`"setup":[["sh","-c","test -f README"]],"check":["sh","-c","test -f fixture.txt"]}`))
	tk.Files = t.TempDir() // a local path the exported bundle must not carry
	os.WriteFile(filepath.Join(tk.Files, "README"), []byte("repo\n"), 0o644)

	orig := ctl.Start(ctx, tk)
	if orig.Outcome != runstore.OutcomeFail {
		t.Fatalf("original should fail: %+v", orig)
	}
	if err := Add(ctx, store, runstore.EvalCase{CaseID: "fixture-delete-01", RunID: orig.RunID, Step: 6}); err == nil {
		t.Fatal("a tool step can't be a case")
	}
	if err := Add(ctx, store, runstore.EvalCase{CaseID: "fixture-delete-01", RunID: orig.RunID, Step: 5, Notes: "rm at step 5"}); err != nil {
		t.Fatal(err)
	}
	c, _ := store.EvalCase(ctx, "fixture-delete-01")
	if cp, _ := store.Checkpoint(ctx, c.CheckpointID); !cp.Pinned {
		t.Fatal("a case's checkpoint must be pinned")
	}

	b, err := Export(ctx, eng, "fixture-delete-01")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := b.Write(dir + "/fixture-delete-01"); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadAll(dir)
	if err != nil || len(loaded) != 1 {
		t.Fatalf("load: %v", err)
	}
	if !bytes.Equal(loaded[0].Request, b.Request) || len(loaded[0].Files) == 0 {
		t.Fatal("bundle did not round-trip")
	}
	if bytes.Contains(loaded[0].Task, []byte(`"files"`)) {
		t.Fatalf("bundle keeps the local files path: %s", loaded[0].Task)
	}

	fix := "You tidy repos. Never delete fixture.txt."
	rep, err := RunSuite(ctx, eng, loaded, []Variant{
		{Name: "main"},
		{Name: "pr", Change: replay.Change{System: &fix}},
	}, 4, NewBatchID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	base, pr := rep.Total("main"), rep.Total("pr")
	if base.Passes != 0 || base.Done != 4 || pr.Passes != 4 || pr.Done != 4 {
		t.Fatalf("main %d/%d, pr %d/%d: want 0/4 and 4/4 (restored files must include fixture.txt)",
			base.Passes, base.Done, pr.Passes, pr.Done)
	}
	d, lo, _ := Difference(base, pr)
	if d != 1 || lo <= 0 || Verdict(lo, 1) != "better" {
		t.Fatalf("difference %.2f, lower bound %.2f", d, lo)
	}
	md := rep.Markdown()
	for _, want := range []string{"| `fixture-delete-01` | 0/4", "**pr vs main: +100 points**", "better"} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q:\n%s", want, md)
		}
	}
	// Results are runs tagged with the case and variant.
	kids, _ := store.Children(ctx, "case-fixture-delete-01-"+rep.Batch)
	if len(kids) != 8 || kids[0].Eval.Case != "fixture-delete-01" {
		t.Fatalf("eval runs: %d", len(kids))
	}
}
