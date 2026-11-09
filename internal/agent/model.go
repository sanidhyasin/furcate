// Package agent holds the pieces of Furcate's agent loop that are not about
// recording: the conversation, the model client and the sandbox tools. The loop
// itself runs in the run controller, on the host; only tools execute in the
// sandbox.
package agent

import (
	"context"

	"github.com/anthropics/anthropic-sdk-go/v2"
	"github.com/anthropics/anthropic-sdk-go/v2/option"
)

// Model sends one Messages API request body exactly as given.
type Model interface {
	Send(ctx context.Context, body []byte) (*anthropic.Message, error)
}

// Anthropic calls the Claude API. Credentials come from the environment, as the
// SDK resolves them (ANTHROPIC_API_KEY, or an `ant auth login` profile).
type Anthropic struct {
	client    anthropic.Client
	opts      []option.RequestOption
	fallbacks bool
}

// NewAnthropic returns a client. With fallbacks, a request the model declines on
// safety grounds is re-served by a fallback model the API picks by refusal
// category; the span records which model actually answered.
func NewAnthropic(fallbacks bool, opts ...option.RequestOption) *Anthropic {
	a := &Anthropic{client: anthropic.NewClient(opts...)}
	if fallbacks {
		a.opts = append(a.opts, option.WithHeaderAdd("anthropic-beta", "server-side-fallback-2026-07-01"))
	}
	a.fallbacks = fallbacks
	return a
}

// Send posts the body unchanged: the recorded request is what the model saw, so
// a replay must not re-encode it. Fallbacks are a request option, not part of
// the conversation, so they are added here and never recorded.
func (a *Anthropic) Send(ctx context.Context, body []byte) (*anthropic.Message, error) {
	opts := append([]option.RequestOption{option.WithRequestBody("application/json", body)}, a.opts...)
	if a.fallbacks {
		opts = append(opts, option.WithJSONSet("fallbacks", "default"))
	}
	return a.client.Messages.New(ctx, anthropic.MessageNewParams{}, opts...)
}
