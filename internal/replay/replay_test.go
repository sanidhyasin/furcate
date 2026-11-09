package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/anthropics/anthropic-sdk-go/v2"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/sanidhyasin/furcate/internal/agent"
	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/driver/testdir"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/stats"
	"github.com/sanidhyasin/furcate/internal/task"
)

// scriptModel plays a coding agent that, at its third turn, deletes a file the
// check needs, unless an operator message told it not to.
type scriptModel struct {
	mu    sync.Mutex
	calls [][]byte // every request, as sent
	ids   atomic.Int64
}

func (m *scriptModel) Send(_ context.Context, body []byte) (*anthropic.Message, error) {
	var p anthropic.MessageNewParams
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.calls = append(m.calls, body)
	m.mu.Unlock()

	turn, guarded := 0, false
	for _, msg := range p.Messages {
		if msg.OfAssistant != nil {
			turn++
		}
		if s := msg.OfSystem; s != nil && strings.Contains(s.Content[0].Text, "Never delete") {
			guarded = true
		}
	}
	var content string
	switch {
	case turn == 0:
		content = m.toolUse("bash", `{"command":"echo data > fixture.txt"}`)
	case turn == 1:
		content = m.toolUse("write_file", `{"path":"notes/plan.md","content":"keep the fixture"}`)
	case turn == 2 && !guarded:
		content = m.toolUse("bash", `{"command":"rm fixture.txt"}`)
	default:
		content = `{"type":"text","text":"done"}`
	}
	stop := "end_turn"
	if strings.Contains(content, "tool_use") {
		stop = "tool_use"
	}
	raw := fmt.Sprintf(`{"id":"msg_%d","type":"message","role":"assistant","model":%q,"content":[%s],`+
		`"stop_reason":%q,"stop_sequence":null,"usage":{"input_tokens":100,"output_tokens":20}}`,
		m.ids.Add(1), p.Model, content, stop)
	var msg anthropic.Message
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (m *scriptModel) toolUse(name, input string) string {
	return fmt.Sprintf(`{"type":"tool_use","id":"toolu_%d","name":%q,"input":%s}`, m.ids.Add(1), name, input)
}

// recorded serves requests from spans the test's tracer recorded.
type recorded struct{ rec *tracetest.SpanRecorder }

func (r recorded) LLMRequest(_ context.Context, _, spanID string) ([]byte, error) {
	for _, s := range r.rec.Ended() {
		if s.SpanContext().SpanID().String() != spanID {
			continue
		}
		for _, a := range s.Attributes() {
			if string(a.Key) == controller.AttrLLMRequest {
				return []byte(a.Value.AsString()), nil
			}
		}
	}
	return nil, errors.New("not recorded")
}

type harness struct {
	drv    *testdir.Driver
	root   string
	store  *runstore.Memory
	model  *scriptModel
	engine *Engine
	ctl    *controller.Controller
	task   *task.Task
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	root := t.TempDir()
	drv, err := testdir.New(root)
	if err != nil {
		t.Fatal(err)
	}
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	h := &harness{drv: drv, root: root, store: runstore.NewMemory(), model: &scriptModel{}}
	h.ctl = &controller.Controller{
		Driver: drv, Model: h.model, Store: h.store, Tracer: tp.Tracer("test"),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	h.engine = &Engine{Controller: h.ctl, Store: h.store, Conversations: recorded{rec}}
	h.task, err = task.Parse([]byte(`{"id":"keep-fixture","prompt":"Tidy the repo.","system":"You are a careful agent.",
		"check":["sh","-c","test -f fixture.txt"],"max_steps":10}`))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestForkFromStepBeforeTheMistake(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	orig := h.ctl.Start(ctx, h.task)
	if orig.Err != nil || orig.Status != runstore.StatusDone || orig.Outcome != runstore.OutcomeFail {
		t.Fatalf("original run: %+v", orig)
	}
	// llm, tool, llm, tool, llm, tool(rm), llm
	steps, _ := h.store.Steps(ctx, orig.RunID)
	var kinds []string
	for _, s := range steps {
		kinds = append(kinds, s.Kind)
		if (s.Kind == runstore.KindLLM) != (s.CheckpointID != "") {
			t.Fatalf("step %d: llm steps and only llm steps carry a checkpoint", s.Index)
		}
	}
	if got := strings.Join(kinds, ","); got != "llm,tool,llm,tool,llm,tool,llm" {
		t.Fatalf("steps = %s", got)
	}

	// Step 5's checkpoint holds the files as they were before the bad step.
	s5, _ := h.store.Step(ctx, orig.RunID, 5)
	cp, _ := h.store.Checkpoint(ctx, s5.CheckpointID)
	for _, f := range []string{"fixture.txt", "notes/plan.md"} {
		if _, err := os.Stat(filepath.Join(h.root, "cp", cp.DriverRef, f)); err != nil {
			t.Fatalf("checkpoint at step 5 is missing %s", f)
		}
	}

	// A plain rerun from step 5 makes the same mistake every time.
	same, err := h.engine.Fork(ctx, Request{RunID: orig.RunID, Step: 5, N: 3})
	if err != nil {
		t.Fatal(err)
	}
	if same.Done != 3 || same.Passes != 0 {
		t.Fatalf("plain rerun: %d/%d passed, want 0/3", same.Passes, same.Done)
	}

	// With an operator instruction appended at step 5, every branch passes.
	fixed, err := h.engine.Fork(ctx, Request{RunID: orig.RunID, Step: 5, N: 3,
		Change: Change{AppendSystem: "Never delete fixture.txt."}})
	if err != nil {
		t.Fatal(err)
	}
	if fixed.Done != 3 || fixed.Passes != 3 {
		t.Fatalf("with the fix: %d/%d passed, want 3/3", fixed.Passes, fixed.Done)
	}
	if fixed.Lo < 0.43 || fixed.Hi != 1 {
		t.Fatalf("3/3 interval = [%.2f, %.2f]", fixed.Lo, fixed.Hi)
	}

	// Branches are runs linked to their parent; their step 5 reuses the parent's checkpoint.
	kids, _ := h.store.Children(ctx, orig.RunID)
	if len(kids) != 6 {
		t.Fatalf("parent has %d branches, want 6", len(kids))
	}
	for _, b := range fixed.Branches {
		br, _ := h.store.Run(ctx, b.RunID)
		bs, _ := h.store.Step(ctx, b.RunID, 5)
		if br.ParentRunID != orig.RunID || br.ParentStep != 5 || bs.CheckpointID != s5.CheckpointID {
			t.Fatalf("branch %s not linked: %+v / %+v", b.RunID, br, bs)
		}
	}
}

func TestBranchResendsTheRecordedRequest(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	orig := h.ctl.Start(ctx, h.task)
	s5, _ := h.store.Step(ctx, orig.RunID, 5)
	recordedBody, err := h.engine.Conversations.LLMRequest(ctx, orig.RunID, s5.SpanID)
	if err != nil {
		t.Fatal(err)
	}
	before := len(h.model.calls)

	if _, err := h.engine.Fork(ctx, Request{RunID: orig.RunID, Step: 5, N: 1}); err != nil {
		t.Fatal(err)
	}
	// The branch's first call is byte-identical to the parent's call at step 5.
	if got := string(h.model.calls[before]); got != string(recordedBody) {
		t.Fatalf("branch's first request differs from the recorded one:\n got %s\nwant %s", got, recordedBody)
	}
}

func TestChangesAreAppendOnly(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	orig := h.ctl.Start(ctx, h.task)
	s5, _ := h.store.Step(ctx, orig.RunID, 5)
	body, _ := h.engine.Conversations.LLMRequest(ctx, orig.RunID, s5.SpanID)

	out := "plan.md written; do not touch the fixture"
	conv, _ := agent.ParseConversation(body)
	orig2, _ := agent.ParseConversation(body)
	n := len(conv.Messages)
	if err := (Change{Model: "claude-sonnet-5-5", AppendSystem: "Be careful.", ToolOutput: &out}).Apply(conv); err != nil {
		t.Fatal(err)
	}
	if conv.Model() != "claude-sonnet-5-5" || len(conv.Messages) != n+1 {
		t.Fatalf("change not applied: model %s, %d messages", conv.Model(), len(conv.Messages))
	}
	// Every message before the newest keeps its bytes.
	for i := 0; i < n-1; i++ {
		if string(conv.Messages[i]) != string(orig2.Messages[i]) {
			t.Fatalf("message %d changed", i)
		}
	}
	if !strings.Contains(string(conv.Messages[n-1]), out) || !strings.Contains(string(conv.Messages[n]), `"role":"system"`) {
		t.Fatalf("tool output or system message missing:\n%s\n%s", conv.Messages[n-1], conv.Messages[n])
	}

	// Step 1 has no tool result to replace.
	s1, _ := h.store.Step(ctx, orig.RunID, 1)
	b1, _ := h.engine.Conversations.LLMRequest(ctx, orig.RunID, s1.SpanID)
	first, _ := agent.ParseConversation(b1)
	if err := (Change{ToolOutput: &out}).Apply(first); err == nil {
		t.Fatal("want an error replacing a tool output at step 1")
	}
}

func TestForkRejectsToolSteps(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	orig := h.ctl.Start(ctx, h.task)
	_, err := h.engine.Fork(ctx, Request{RunID: orig.RunID, Step: 6, N: 2})
	if err == nil || !strings.Contains(err.Error(), "tool step") {
		t.Fatalf("got %v, want a tool-step error", err)
	}
}

func TestRequestRoundTripsWithThinking(t *testing.T) {
	// A recorded request, thinking blocks and signatures included, must come back
	// byte for byte, or a branch would not resend what the parent sent.
	in := `{"max_tokens":16000,"messages":[` +
		`{"content":[{"text":"go","type":"text"}],"role":"user"},` +
		`{"content":[{"signature":"c2ln","thinking":"","type":"thinking"},{"data":"cmVk","type":"redacted_thinking"},` +
		`{"id":"toolu_1","input":{"command":"ls"},"name":"bash","type":"tool_use"}],"role":"assistant"},` +
		`{"content":[{"content":[{"text":"a.txt","type":"text"}],"tool_use_id":"toolu_1","type":"tool_result"}],"role":"user"}],` +
		`"model":"claude-opus-5-5","output_config":{"effort":"high"}}`
	conv, err := agent.ParseConversation([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	out, _ := conv.Body()
	if string(out) != in {
		t.Fatalf("round trip changed the request:\n in %s\nout %s", in, out)
	}
}

func TestWilsonMatchesTheDesignDoc(t *testing.T) {
	// The doc: 4 of 5 against 0 of 5 still overlap (0.38–0.96 and 0–0.43).
	lo, hi := stats.Wilson(4, 5, stats.Z95)
	lo0, hi0 := stats.Wilson(0, 5, stats.Z95)
	round := func(x float64) float64 { return math.Round(x*100) / 100 }
	if round(lo) != 0.38 || round(hi) != 0.96 || lo0 != 0 || round(hi0) != 0.43 {
		t.Fatalf("got [%.3f, %.3f] and [%.3f, %.3f]", lo, hi, lo0, hi0)
	}
	if !stats.Overlap(lo, hi, lo0, hi0) {
		t.Fatal("4/5 and 0/5 should overlap")
	}
	lo10, _ := stats.Wilson(8, 10, stats.Z95)
	_, hi10 := stats.Wilson(0, 10, stats.Z95)
	if lo10 <= hi10 {
		t.Fatalf("8/10 and 0/10 should separate: %.2f vs %.2f", lo10, hi10)
	}
}
