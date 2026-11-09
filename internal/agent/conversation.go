package agent

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go/v2"
)

// Conversation is a Messages API request kept as raw JSON. Messages are encoded
// once, when they are appended, and never decoded and re-encoded: a round trip
// through the SDK's typed params reorders the keys of earlier tool inputs, and a
// history that differs from what the model saw breaks the prompt cache and, on
// newer accounts, the API's check that replayed thinking blocks belong to the
// same conversation.
type Conversation struct {
	Head     map[string]json.RawMessage // every top-level field except messages
	Messages []json.RawMessage
}

// NewConversation encodes the first request of a run.
func NewConversation(p anthropic.MessageNewParams) (*Conversation, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	return ParseConversation(b)
}

// ParseConversation splits a recorded request body. Messages keep their bytes.
func ParseConversation(body []byte) (*Conversation, error) {
	var head map[string]json.RawMessage
	if err := json.Unmarshal(body, &head); err != nil {
		return nil, fmt.Errorf("conversation: %w", err)
	}
	c := &Conversation{Head: head}
	if raw, ok := head["messages"]; ok {
		if err := json.Unmarshal(raw, &c.Messages); err != nil {
			return nil, fmt.Errorf("conversation messages: %w", err)
		}
		delete(head, "messages")
	}
	return c, nil
}

// Body is the request to send. Top-level keys come out sorted; message bytes are
// as they were appended.
func (c *Conversation) Body() ([]byte, error) {
	out := make(map[string]json.RawMessage, len(c.Head)+1)
	for k, v := range c.Head {
		out[k] = v
	}
	msgs, err := json.Marshal(c.Messages)
	if err != nil {
		return nil, err
	}
	out["messages"] = msgs
	return json.Marshal(out)
}

// Append encodes a message onto the end.
func (c *Conversation) Append(m anthropic.MessageParam) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	c.Messages = append(c.Messages, b)
	return nil
}

// Model returns the request's model.
func (c *Conversation) Model() string {
	var m string
	json.Unmarshal(c.Head["model"], &m)
	return m
}

// SetModel replaces the model for this and later requests.
func (c *Conversation) SetModel(model string) {
	b, _ := json.Marshal(model)
	c.Head["model"] = b
}

// ReplaceLastToolResult swaps the output of the last tool_result in the newest
// message. Only that message is re-encoded; everything before it keeps its bytes.
func (c *Conversation) ReplaceLastToolResult(output string) error {
	if len(c.Messages) == 0 {
		return errors.New("tool_output: empty conversation")
	}
	var last anthropic.MessageParam
	if err := json.Unmarshal(c.Messages[len(c.Messages)-1], &last); err != nil {
		return err
	}
	if last.OfUser == nil {
		return errors.New("tool_output: the request at this step does not end with tool results")
	}
	for i := len(last.OfUser.Content) - 1; i >= 0; i-- {
		if tr := last.OfUser.Content[i].OfToolResult; tr != nil {
			tr.Content = []anthropic.ToolResultBlockParamContentUnion{{OfText: &anthropic.TextBlockParam{Text: output}}}
			b, err := json.Marshal(last)
			if err != nil {
				return err
			}
			c.Messages[len(c.Messages)-1] = b
			return nil
		}
	}
	return errors.New("tool_output: no tool result before this step (it is the first step)")
}

// Clone copies the conversation so branches can append independently.
func (c *Conversation) Clone() *Conversation {
	head := make(map[string]json.RawMessage, len(c.Head))
	for k, v := range c.Head {
		head[k] = v
	}
	return &Conversation{Head: head, Messages: append([]json.RawMessage(nil), c.Messages...)}
}

// SetSystem replaces the top-level system prompt. This edits history: thinking
// blocks produced under the old prompt no longer belong to this conversation,
// so callers strip them (StripThinking) and accept a cold prompt cache.
func (c *Conversation) SetSystem(text string) {
	if text == "" {
		delete(c.Head, "system")
		return
	}
	b, _ := json.Marshal([]anthropic.TextBlockParam{{Text: text}})
	c.Head["system"] = b
}

// StripThinking removes thinking and redacted_thinking blocks from every
// assistant message. Other blocks keep their bytes.
func (c *Conversation) StripThinking() error {
	for i, raw := range c.Messages {
		var msg map[string]json.RawMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			return err
		}
		var role string
		json.Unmarshal(msg["role"], &role)
		if role != "assistant" {
			continue
		}
		var blocks []json.RawMessage
		if err := json.Unmarshal(msg["content"], &blocks); err != nil {
			continue // string content: nothing to strip
		}
		kept := blocks[:0:0]
		for _, b := range blocks {
			var head struct {
				Type string `json:"type"`
			}
			json.Unmarshal(b, &head)
			if head.Type != "thinking" && head.Type != "redacted_thinking" {
				kept = append(kept, b)
			}
		}
		if len(kept) == len(blocks) {
			continue
		}
		content, err := json.Marshal(kept)
		if err != nil {
			return err
		}
		msg["content"] = content
		if c.Messages[i], err = json.Marshal(msg); err != nil {
			return err
		}
	}
	return nil
}
