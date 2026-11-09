package bisect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

// flakyModel writes a fixture, writes notes, then at its third turn deletes the
// fixture on every fifth call: from any step before that decision, about 80% of
// continuations pass; after it, none do. The original run is made to fail.
type flakyModel struct {
	ids      atomic.Int64
	decision atomic.Int64
	scripted string // "" or "always-rm" / "never-rm"
}

func (m *flakyModel) Send(_ context.Context, body []byte) (*anthropic.Message, error) {
	var p anthropic.MessageNewParams
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, err
	}
	turn := 0
	for _, msg := range p.Messages {
		if msg.OfAssistant != nil {
			turn++
		}
	}
	var content string
	switch turn {
	case 0:
		content = m.tool("bash", `{"command":"echo data > fixture.txt"}`)
	case 1:
		content = m.tool("write_file", `{"path":"notes.md","content":"plan"}`)
	case 2:
		rm := m.decision.Add(1)%5 == 0
		switch m.scripted {
		case "always-rm":
			rm = true
		case "never-rm":
			rm = false
		}
		if rm {
			content = m.tool("bash", `{"command":"rm fixture.txt"}`)
		} else {
			content = m.tool("bash", `{"command":"ls"}`)
		}
	default:
		content = `{"type":"text","text":"done"}`
	}
	stop := "end_turn"
	if strings.Contains(content, "tool_use") {
		stop = "tool_use"
	}
	raw := fmt.Sprintf(`{"id":"msg_%d","type":"message","role":"assistant","model":%q,"content":[%s],`+
		`"stop_reason":%q,"stop_sequence":null,"usage":{"input_tokens":1000,"output_tokens":100}}`,
		m.ids.Add(1), p.Model, content, stop)
	var msg anthropic.Message
	return &msg, json.Unmarshal([]byte(raw), &msg)
}

func (m *flakyModel) tool(name, input string) string {
	return fmt.Sprintf(`{"type":"tool_use","id":"toolu_%d","name":%q,"input":%s}`, m.ids.Add(1), name, input)
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

func setup(t *testing.T, m *flakyModel) (*replay.Engine, string) {
	t.Helper()
	drv, err := testdir.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	store := runstore.NewMemory()
	ctl := &controller.Controller{Driver: drv, Model: m, Store: store, Tracer: tp.Tracer("t"),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Slots: controller.NewSlots(8)}
	tk, _ := task.Parse([]byte(`{"id":"keep","prompt":"go","check":["sh","-c","test -f fixture.txt"]}`))

	// The original run makes the mistake.
	m.scripted = "always-rm"
	orig := ctl.Start(context.Background(), tk)
	m.scripted = ""
	if orig.Outcome != runstore.OutcomeFail {
		t.Fatalf("original run should fail: %+v", orig)
	}
	return &replay.Engine{Controller: ctl, Store: store, Conversations: recorded{rec}}, orig.RunID
}

func TestBisectFindsTheDecision(t *testing.T) {
	eng, run := setup(t, &flakyModel{})
	res, err := Run(context.Background(), eng, Config{RunID: run, N: 10})
	if err != nil {
		t.Fatal(err)
	}
	// Steps: 1 llm, 2 tool, 3 llm, 4 tool, 5 llm (the rm), 6 tool, 7 llm.
	if res.Faulty != 5 || !res.Confident {
		t.Fatalf("faulty step %d (confident %v): %s", res.Faulty, res.Confident, res.Verdict)
	}
	if res.LastGood.Step != 5 || res.FirstBad.Step != 7 || res.FirstBad.Passes != 0 {
		t.Fatalf("good %+v bad %+v", res.LastGood, res.FirstBad)
	}
	// First, last, then two midpoints: 4 probes, 40 continuations.
	if len(res.Probes) != 4 || res.Runs != 40 || res.Usage.CostUSD <= 0 {
		t.Fatalf("probes %d, runs %d, cost %.4f", len(res.Probes), res.Runs, res.Usage.CostUSD)
	}
}

func TestBisectStopsAtTheBudget(t *testing.T) {
	eng, run := setup(t, &flakyModel{})
	res, err := Run(context.Background(), eng, Config{RunID: run, N: 10, MaxRuns: 25})
	if err != nil {
		t.Fatal(err)
	}
	if res.Faulty != 0 || res.Runs > 25 || !strings.Contains(res.Verdict, "budget") {
		t.Fatalf("runs %d faulty %d: %s", res.Runs, res.Faulty, res.Verdict)
	}
}

func TestBisectWhenEveryStepFails(t *testing.T) {
	m := &flakyModel{}
	eng, run := setup(t, m)
	m.scripted = "always-rm"
	res, err := Run(context.Background(), eng, Config{RunID: run, N: 6})
	if err != nil {
		t.Fatal(err)
	}
	if res.Faulty != 0 || !strings.Contains(res.Verdict, "task or prompt") {
		t.Fatalf("faulty %d: %s", res.Faulty, res.Verdict)
	}
}
