package tine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go/v2"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/driver"
	"github.com/sanidhyasin/furcate/internal/driver/tine"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/task"
)

// machineModel starts a counter process at its first turn, writes a file at its
// second, reads the counter at its third and stops. It records every request.
type machineModel struct {
	ids   atomic.Int64
	mu    sync.Mutex
	calls [][]byte
}

func (m *machineModel) Send(_ context.Context, body []byte) (*anthropic.Message, error) {
	m.mu.Lock()
	m.calls = append(m.calls, body)
	m.mu.Unlock()
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
	tool := func(name, in string) string {
		return fmt.Sprintf(`{"type":"tool_use","id":"toolu_%d","name":%q,"input":%s}`, m.ids.Add(1), name, in)
	}
	content, stop := `{"type":"text","text":"done"}`, "end_turn"
	switch turn {
	case 0:
		cmd := `nohup sh -c 'i=0; while :; do i=$((i+1)); echo $i > /tmp/count; sleep 0.2; done' >/dev/null 2>&1 & echo started`
		b, _ := json.Marshal(map[string]string{"command": cmd})
		content, stop = tool("bash", string(b)), "tool_use"
	case 1:
		content, stop = tool("write_file", `{"path":"/root/notes.md","content":"plan"}`), "tool_use"
	case 2:
		content, stop = tool("bash", `{"command":"cat /tmp/count"}`), "tool_use"
	}
	raw := fmt.Sprintf(`{"id":"msg_%d","type":"message","role":"assistant","model":%q,"content":[%s],`+
		`"stop_reason":%q,"stop_sequence":null,"usage":{"input_tokens":10,"output_tokens":5}}`, m.ids.Add(1), p.Model, content, stop)
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

// The Phase 2 gate on real tine: a fork of step k matches its files, its
// running processes and its conversation; step overhead is measured.
//
//	FURCATE_TEST_TINE=ssh://tine-dev FURCATE_TINE_SSH="ssh -F ../tine/.tine-ssh-config" \
//	  go test -v -run Gate ./internal/driver/tine/
func TestGateOnTine(t *testing.T) {
	host := os.Getenv("FURCATE_TEST_TINE")
	if host == "" {
		t.Skip("set FURCATE_TEST_TINE to a tine host, e.g. ssh://tine-dev")
	}
	image := os.Getenv("FURCATE_TINE_IMAGE")
	if image == "" {
		image = "base"
	}
	ctx := context.Background()
	d, err := tine.New(tine.Options{Host: host, SSH: os.Getenv("FURCATE_TINE_SSH"), TTL: 30 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("tine %s, features %v", d.Version, d.Features)

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	store := runstore.NewMemory()
	model := &machineModel{}
	ctl := &controller.Controller{Driver: d, Model: model, Store: store, Tracer: tp.Tracer("gate"),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Slots: controller.NewSlots(16)}
	eng := &replay.Engine{Controller: ctl, Store: store, Conversations: recorded{rec}}
	tk, _ := task.Parse([]byte(fmt.Sprintf(`{"id":"gate","template":%q,"workdir":"/root","prompt":"go",`+
		`"check":["sh","-c","test -f /root/notes.md"]}`, image)))

	start := time.Now()
	orig := ctl.Start(ctx, tk)
	if orig.Err != nil || orig.Outcome != runstore.OutcomePass {
		t.Fatalf("original run: %+v", orig)
	}
	t.Logf("run: %d steps in %s; %d checkpoints took %s (%s each)", orig.Steps, time.Since(start).Round(time.Millisecond),
		orig.Checkpoints, orig.CheckpointTime.Round(time.Millisecond), (orig.CheckpointTime / time.Duration(orig.Checkpoints)).Round(time.Millisecond))

	// Step 5 is the third LLM call: after the counter started and the file was written.
	s5, err := store.Step(ctx, orig.RunID, 5)
	if err != nil || s5.CheckpointID == "" {
		t.Fatalf("step 5: %v %+v", err, s5)
	}
	cp, _ := store.Checkpoint(ctx, s5.CheckpointID)

	// The original VM is gone; the retained snapshot must still restore.
	for _, n := range []int{1, 10} {
		fs := time.Now()
		kids, err := d.Fork(ctx, driver.CheckpointID(cp.DriverRef), n, true)
		took := time.Since(fs)
		if err != nil || len(kids) != n {
			t.Fatalf("fork %d: %v (%d)", n, err, len(kids))
		}
		t.Logf("fork of %d from a retained snapshot, offline: %s", n, took.Round(time.Millisecond))
		if n == 1 {
			checkFork(t, ctx, d, kids[0])
		}
		for _, k := range kids {
			d.Destroy(ctx, k)
		}
	}

	// Through the replay engine: the branch's first request is the recorded one.
	body, _ := eng.Request(ctx, orig.RunID, s5.SpanID)
	before := len(model.calls)
	res, err := eng.Fork(ctx, replay.Request{RunID: orig.RunID, Step: 5, N: 3})
	if err != nil || res.Done != 3 || res.Passes != 3 {
		t.Fatalf("fork through the engine: %v %+v", err, res)
	}
	model.mu.Lock()
	first := slices.IndexFunc(model.calls[before:], func(b []byte) bool { return string(b) == string(body) })
	model.mu.Unlock()
	if first < 0 {
		t.Fatal("no branch resent the recorded request byte for byte")
	}
	d.DeleteCheckpoint(ctx, driver.CheckpointID(cp.DriverRef))
}

func checkFork(t *testing.T, ctx context.Context, d *tine.Driver, inst driver.Instance) {
	t.Helper()
	run := func(cmd string) string {
		res, err := d.Exec(ctx, inst, driver.ExecRequest{Args: []string{"sh", "-c", cmd}, Timeout: 10 * time.Second})
		if err != nil {
			t.Fatalf("%s: %v", cmd, err)
		}
		return strings.TrimSpace(string(res.Stdout))
	}
	if got := run("cat /root/notes.md"); got != "plan" {
		t.Fatalf("files: notes.md = %q", got)
	}
	a, _ := strconv.Atoi(run("cat /tmp/count"))
	time.Sleep(1500 * time.Millisecond)
	b, _ := strconv.Atoi(run("cat /tmp/count"))
	if a <= 1 || b <= a {
		t.Fatalf("processes: counter went %d -> %d; want it running on from its value at the checkpoint", a, b)
	}
	// Offline: tine's own check, a connection to the internet fails at once.
	if out := run("timeout 5 bash -c 'echo > /dev/tcp/1.1.1.1/443' 2>&1; true"); !strings.Contains(out, "unreachable") {
		t.Fatalf("offline fork reached out, or failed oddly: %q", out)
	}
	t.Logf("fork fidelity: notes.md restored; counter resumed at %d and reached %d; internet: network unreachable", a, b)
}
