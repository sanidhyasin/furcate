// Package casestudy runs the same bisect on each driver, as the design doc asks:
// the cleanest proof of what a level-3 driver is worth.
package casestudy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go/v2"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/sanidhyasin/furcate/internal/bisect"
	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/driver"
	"github.com/sanidhyasin/furcate/internal/driver/docker"
	"github.com/sanidhyasin/furcate/internal/driver/tine"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/task"
)

// devServerAgent starts a dev server, writes notes, and at its third turn kills
// the server one time in five. The check needs the server alive at the end, so
// the decision at step 5 is what breaks a run. A scripted agent, so the bisect
// measures the drivers rather than a model.
type devServerAgent struct {
	ids, decisions atomic.Int64
	alwaysKill     atomic.Bool
}

func (m *devServerAgent) Send(_ context.Context, body []byte) (*anthropic.Message, error) {
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
	tool := func(cmd string) string {
		in, _ := json.Marshal(map[string]string{"command": cmd})
		return fmt.Sprintf(`{"type":"tool_use","id":"toolu_%d","name":"bash","input":%s}`, m.ids.Add(1), in)
	}
	content, stop := `{"type":"text","text":"done"}`, "end_turn"
	switch turn {
	case 0:
		content, stop = tool(`nohup sh -c 'while :; do date +%s > /tmp/heartbeat; sleep 0.2; done' >/dev/null 2>&1 & echo $! > /tmp/server.pid; echo server started`), "tool_use"
	case 1:
		content, stop = tool(`echo "server is up" > /root/notes.md`), "tool_use"
	case 2:
		if m.alwaysKill.Load() || m.decisions.Add(1)%5 == 0 {
			content = tool(`kill $(cat /tmp/server.pid); echo stopped the server`)
		} else {
			content = tool(`cat /root/notes.md`)
		}
		stop = "tool_use"
	}
	raw := fmt.Sprintf(`{"id":"msg_%d","type":"message","role":"assistant","model":"claude-opus-5-5","content":[%s],`+
		`"stop_reason":%q,"stop_sequence":null,"usage":{"input_tokens":10,"output_tokens":5}}`, m.ids.Add(1), content, stop)
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

// FURCATE_CASESTUDY=docker or tine (with FURCATE_TEST_TINE and FURCATE_TINE_SSH).
func TestDevServerBisect(t *testing.T) {
	which := os.Getenv("FURCATE_CASESTUDY")
	var sb driver.Sandbox
	var image string
	var err error
	switch which {
	case "docker":
		sb, err = docker.New(docker.Options{Prefix: "furcatecase", Memory: "256m", CPUs: 1})
		image = "alpine:3.22"
		defer sb.(*docker.Driver).Cleanup(context.Background())
	case "tine":
		sb, err = tine.New(tine.Options{Host: os.Getenv("FURCATE_TEST_TINE"), SSH: os.Getenv("FURCATE_TINE_SSH"), TTL: 30 * time.Minute})
		image = "base"
	default:
		t.Skip("set FURCATE_CASESTUDY=docker or tine")
	}
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	store := runstore.NewMemory()
	agent := &devServerAgent{}
	ctl := &controller.Controller{Driver: sb, Model: agent, Store: store, Tracer: tp.Tracer("case"),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Slots: controller.NewSlots(10)}
	eng := &replay.Engine{Controller: ctl, Store: store, Conversations: recorded{rec}}
	tk, _ := task.Parse([]byte(fmt.Sprintf(`{"id":"dev-server","template":%q,"workdir":"/root","prompt":"go",`+
		`"check":["sh","-c","test -f /root/notes.md && sleep 2 && [ $(( $(date +%%s) - $(cat /tmp/heartbeat) )) -le 1 ]"]}`, image)))

	agent.alwaysKill.Store(true) // the original run makes the mistake
	orig := ctl.Start(ctx, tk)
	agent.alwaysKill.Store(false)
	if orig.Err != nil || orig.Outcome != runstore.OutcomeFail {
		t.Fatalf("original run: %+v", orig)
	}

	start := time.Now()
	res, err := bisect.Run(ctx, eng, bisect.Config{RunID: orig.RunID, N: 10, MaxRuns: 80})
	if err != nil {
		t.Fatal(err)
	}
	var probes []string
	for _, p := range res.Probes {
		probes = append(probes, fmt.Sprintf("step %d: %d/%d", p.Step, p.Passes, p.Done))
	}
	t.Logf("%s: %s", which, strings.Join(probes, ", "))
	t.Logf("%s: %s", which, res.Verdict)
	t.Logf("%s: %d continuations in %s (original run: %s, %d checkpoints in %s)", which, res.Runs,
		time.Since(start).Round(time.Millisecond), orig.Duration.Round(time.Millisecond), orig.Checkpoints,
		orig.CheckpointTime.Round(time.Millisecond))

	switch which {
	case "tine":
		if res.Faulty != 5 {
			t.Fatalf("tine should find step 5, got %d", res.Faulty)
		}
	case "docker":
		// Forks restore files but not processes: the server is gone in every
		// continuation, so no step can pass and the faulty one can't be found.
		if res.Faulty != 0 {
			t.Fatalf("docker can't see running state, yet bisect named step %d", res.Faulty)
		}
	}
}
