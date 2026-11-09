package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go/v2"
	"github.com/anthropics/anthropic-sdk-go/v2/option"
)

// The SDK must send our bytes untouched, adding only the fallbacks field.
func TestSendKeepsTheBodyAndAddsFallbacks(t *testing.T) {
	var got []byte
	var beta string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		beta = r.Header.Get("anthropic-beta")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5-5",`+
			`"content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","stop_sequence":null,`+
			`"usage":{"input_tokens":3,"output_tokens":1}}`)
	}))
	defer srv.Close()

	body := `{"max_tokens":16000,"messages":[{"content":[{"id":"toolu_1","input":{"path":"a","content":"b"},` +
		`"name":"write_file","type":"tool_use"}],"role":"assistant"}],"model":"claude-opus-5-5"}`
	m := NewAnthropic(true, option.WithBaseURL(srv.URL), option.WithAPIKey("test"), option.WithMaxRetries(0))
	msg, err := m.Send(context.Background(), []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if msg.StopReason != anthropic.StopReasonEndTurn {
		t.Fatalf("stop reason %q", msg.StopReason)
	}
	if !strings.Contains(beta, "server-side-fallback-2026-07-01") {
		t.Fatalf("beta header %q", beta)
	}
	// The tool input keeps its key order (path before content).
	if !strings.Contains(string(got), `"input":{"path":"a","content":"b"}`) {
		t.Fatalf("body was re-encoded: %s", got)
	}
	var sent map[string]json.RawMessage
	json.Unmarshal(got, &sent)
	if string(sent["fallbacks"]) != `"default"` {
		t.Fatalf("fallbacks = %s", sent["fallbacks"])
	}
}

func TestConversationKeepsMessageBytes(t *testing.T) {
	body := `{"max_tokens":16000,"messages":[{"content":[{"id":"toolu_1","input":{"path":"a","content":"b"},` +
		`"name":"write_file","type":"tool_use"}],"role":"assistant"},` +
		`{"content":[{"tool_use_id":"toolu_1","content":[{"text":"ok","type":"text"}],"type":"tool_result"}],"role":"user"}],` +
		`"model":"claude-opus-5-5"}`
	c, err := ParseConversation([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	out, _ := c.Body()
	if string(out) != body {
		t.Fatalf("body changed:\n got %s\nwant %s", out, body)
	}
	first := string(c.Messages[0])
	if err := c.ReplaceLastToolResult("changed"); err != nil {
		t.Fatal(err)
	}
	c.SetModel("claude-sonnet-5-5")
	c.Append(anthropic.MessageParam{OfSystem: &anthropic.SystemMessageParam{Content: []anthropic.TextBlockParam{{Text: "x"}}}})
	if string(c.Messages[0]) != first || len(c.Messages) != 3 || c.Model() != "claude-sonnet-5-5" {
		t.Fatal("only the newest message may change")
	}
	if !strings.Contains(string(c.Messages[1]), `"changed"`) {
		t.Fatalf("tool output not replaced: %s", c.Messages[1])
	}
}

func TestSetSystemStripsThinkingOnly(t *testing.T) {
	body := `{"max_tokens":1,"messages":[{"content":[{"text":"go","type":"text"}],"role":"user"},` +
		`{"content":[{"signature":"c2ln","thinking":"","type":"thinking"},{"id":"toolu_1","input":{"path":"a","content":"b"},"name":"write_file","type":"tool_use"}],"role":"assistant"}],` +
		`"model":"m","system":[{"text":"old","type":"text"}]}`
	c, _ := ParseConversation([]byte(body))
	c.SetSystem("new")
	if err := c.StripThinking(); err != nil {
		t.Fatal(err)
	}
	out, _ := c.Body()
	s := string(out)
	if strings.Contains(s, "thinking") || !strings.Contains(s, `"text":"new"`) {
		t.Fatalf("got %s", s)
	}
	if !strings.Contains(s, `{"id":"toolu_1","input":{"path":"a","content":"b"},"name":"write_file","type":"tool_use"}`) {
		t.Fatalf("tool_use block changed: %s", s)
	}
}
