package dropin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/driver/docker"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/task"
)

// fakeAgent is an agent Furcate doesn't drive: it calls the Messages API itself
// and keeps its transcript on disk, so it can resume like `claude --continue`.
const fakeAgent = `import json, os, subprocess, sys, urllib.request
T = "/root/.fakeagent/transcript.json"
mode, text = sys.argv[1], sys.argv[2]
if mode == "run":
    msgs = [{"role": "user", "content": [{"type": "text", "text": text}]}]
else:
    msgs = json.load(open(T))
    if msgs[-1]["role"] == "user":
        msgs[-1]["content"].append({"type": "text", "text": text})
    else:
        msgs.append({"role": "user", "content": [{"type": "text", "text": text}]})
def save():
    os.makedirs(os.path.dirname(T), exist_ok=True)
    json.dump(msgs, open(T, "w"))
save()
while True:
    body = json.dumps({"model": "claude-opus-5-5", "max_tokens": 1024, "messages": msgs,
        "tools": [{"name": "bash", "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}}}]}).encode()
    req = urllib.request.Request(os.environ["ANTHROPIC_BASE_URL"] + "/v1/messages", data=body, headers={
        "x-api-key": os.environ["ANTHROPIC_API_KEY"], "anthropic-version": "2023-06-01", "content-type": "application/json"})
    resp = json.load(urllib.request.urlopen(req, timeout=60))
    msgs.append({"role": "assistant", "content": resp["content"]})
    if resp["stop_reason"] != "tool_use":
        save()
        break
    results = []
    for b in resp["content"]:
        if b["type"] == "tool_use":
            p = subprocess.run(["sh", "-c", b["input"]["command"]], capture_output=True, text=True)
            results.append({"type": "tool_result", "tool_use_id": b["id"], "content": p.stdout + p.stderr})
    msgs.append({"role": "user", "content": results})
    save()
`

// fakeClaude answers like the Messages API: the agent deletes the fixture at
// its third request unless a user message says not to.
func fakeClaude(t *testing.T, keys *atomic.Int64) *httptest.Server {
	var ids atomic.Int64
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "host-key" {
			w.WriteHeader(401)
			io.WriteString(w, `{"type":"error","error":{"type":"authentication_error","message":"bad key"}}`)
			return
		}
		keys.Add(1)
		var req struct {
			Messages []struct {
				Role    string            `json:"role"`
				Content []json.RawMessage `json:"content"`
			} `json:"messages"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		turn, guarded := 0, false
		for _, m := range req.Messages {
			if m.Role == "assistant" {
				turn++
			}
			for _, c := range m.Content {
				guarded = guarded || strings.Contains(string(c), "Never delete")
			}
		}
		tool := func(cmd string) string {
			in, _ := json.Marshal(map[string]string{"command": cmd})
			return fmt.Sprintf(`{"type":"tool_use","id":"toolu_%d","name":"bash","input":%s}`, ids.Add(1), in)
		}
		content, stop := `{"type":"text","text":"done"}`, "end_turn"
		switch {
		case turn == 0:
			content, stop = tool("echo data > fixture.txt"), "tool_use"
		case turn == 1:
			content, stop = tool("echo plan > notes.md"), "tool_use"
		case turn == 2 && !guarded:
			content, stop = tool("rm fixture.txt"), "tool_use"
		case turn == 2:
			content, stop = tool("ls"), "tool_use"
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"msg_%d","type":"message","role":"assistant","model":"claude-opus-5-5","content":[%s],`+
			`"stop_reason":%q,"usage":{"input_tokens":100,"output_tokens":10}}`, ids.Add(1), content, stop)
	}))
}

// Needs Docker; enable with FURCATE_TEST_DOCKER=1.
func TestDropInRunAndFork(t *testing.T) {
	if os.Getenv("FURCATE_TEST_DOCKER") == "" {
		t.Skip("set FURCATE_TEST_DOCKER=1 to run against Docker")
	}
	ctx := context.Background()
	d, err := docker.New(docker.Options{Prefix: "furcatedropin", Memory: "512m", CPUs: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Cleanup(context.Background())
	var keyed atomic.Int64
	up := fakeClaude(t, &keyed)
	defer up.Close()

	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "agent.py"), []byte(fakeAgent), 0o644)
	tk, err := task.Parse([]byte(fmt.Sprintf(`{"id":"dropin","template":"python:3.13-slim","workdir":"/work","files":%q,
		"prompt":"tidy up","check":["sh","-c","test -f fixture.txt"],
		"dropin":{"command":["python3","agent.py","run","{prompt}"],"resume":["python3","agent.py","resume","{instruction}"]}}`, dir)))
	if err != nil {
		t.Fatal(err)
	}
	rec := tracetest.NewSpanRecorder()
	store := runstore.NewMemory()
	eng := &Engine{Driver: d, Store: store, Tracer: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("t"),
		Slots: controller.NewSlots(4), Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Upstream: up.URL, APIKey: "host-key"}

	orig := eng.Start(ctx, tk)
	if orig.Err != nil || orig.Outcome != runstore.OutcomeFail || orig.Usage.LLMCalls != 4 || orig.Checkpoints != 4 {
		t.Fatalf("original: %+v", orig)
	}
	t.Logf("original: 4 requests, 4 checkpoints in %s", orig.CheckpointTime)
	steps, _ := store.Steps(ctx, orig.RunID)
	if len(steps) != 4 || steps[2].Index != 3 || steps[2].CheckpointID == "" {
		t.Fatalf("steps: %+v", steps)
	}
	// The request at step 3 was recorded with the placeholder key's traffic swapped.
	for _, s := range rec.Ended() {
		for _, a := range s.Attributes() {
			if string(a.Key) == controller.AttrLLMRequest && strings.Contains(a.Value.AsString(), Placeholder) {
				t.Fatal("the placeholder key leaked into a recorded request")
			}
		}
	}

	same, err := eng.Fork(ctx, replay.Request{RunID: orig.RunID, Step: 3, N: 2})
	if err != nil || same.Done != 2 || same.Passes != 0 {
		t.Fatalf("plain resume from step 3: %v %+v", err, same)
	}
	fixed, err := eng.Fork(ctx, replay.Request{RunID: orig.RunID, Step: 3, N: 2,
		Change: replay.Change{AppendSystem: "Never delete fixture.txt."}})
	if err != nil || fixed.Done != 2 || fixed.Passes != 2 {
		t.Fatalf("resume with the instruction: %v %+v", err, fixed)
	}
	b := fixed.Branches[0]
	bs, _ := store.Step(ctx, b.RunID, 3)
	if bs.CheckpointID != steps[2].CheckpointID {
		t.Fatal("a branch's first step should reuse the parent's checkpoint")
	}
	t.Logf("from step 3: plain resume 0/2, with the instruction 2/2; %d requests reached the upstream, all with the host's key", keyed.Load())
}

func TestParseSSEUsage(t *testing.T) {
	sse := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-opus-5-5\",\"usage\":{\"input_tokens\":12,\"cache_read_input_tokens\":300,\"output_tokens\":1}}}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":42}}\n\n"
	r := parseResponse([]byte(sse), true)
	if r.model != "claude-opus-5-5" || r.stop != "tool_use" || r.usage.InputTokens != 12 || r.usage.CacheRead != 300 || r.usage.OutputTokens != 42 {
		t.Fatalf("%+v", r)
	}
	b, _ := setModel([]byte(`{"messages":[{"content":[{"input":{"b":1,"a":2}}]}],"model":"x"}`), "y")
	if !strings.Contains(string(b), `"model":"y"`) || !strings.Contains(string(b), `{"b":1,"a":2}`) {
		t.Fatalf("setModel: %s", b)
	}
}
