// Package dropin records and forks agents Furcate doesn't drive itself: an
// unmodified agent such as Claude Code runs inside the sandbox, and its model
// traffic goes through a recording proxy on the host.
//
// Every Messages request is one step. While the agent waits for its answer, the
// proxy checkpoints the sandbox, records the request on an LLM span, forwards it
// upstream with the host's credentials (the sandbox only ever has a placeholder
// key), streams the answer back and records it.
//
// This is drop-in mode at level 2, as the design doc suggests trying first: a
// fork restores the sandbox's files, including the agent's own transcript, and
// starts the agent's resume command. The agent rebuilds its conversation from
// that transcript, so a branch's first request is the agent's, not a byte-for-
// byte replay. Level 3 (tine, with a vsock shim re-sending the pending request)
// would keep the waiting process itself.
package dropin

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanidhyasin/furcate/internal/controller"
	"github.com/sanidhyasin/furcate/internal/driver"
	"github.com/sanidhyasin/furcate/internal/pricing"
	"github.com/sanidhyasin/furcate/internal/runstore"
)

// Placeholder is the API key the sandbox gets. The proxy swaps in the real one.
const Placeholder = "furcate-proxy-placeholder"

// Proxy records one run's model traffic. Steps are numbered from FirstStep.
type Proxy struct {
	Driver   driver.Sandbox
	Store    runstore.Store
	Tracer   trace.Tracer
	Prices   pricing.Table
	Upstream string          // default https://api.anthropic.com
	APIKey   string          // the host's credential
	Model    string          // if set, replaces the model of every request (a fork's change)
	Client   *http.Client    // default http.DefaultClient
	Ctx      context.Context // the run's root span context
	RunID    string
	inst     driver.Instance
	mu       sync.Mutex // one checkpoint at a time; the agent waits anyway
	step     int
	firstCP  string // reuse this checkpoint for the first step (a fork's restored state)
	usage    runstore.Usage
	cps      int
	cpTime   time.Duration
}

// Attach sets the sandbox the proxy checkpoints and where its numbering starts.
func (p *Proxy) Attach(inst driver.Instance, firstStep int, firstCheckpoint string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inst, p.step, p.firstCP = inst, firstStep, firstCheckpoint
}

// Usage is what the run's model calls consumed so far.
func (p *Proxy) Usage() (runstore.Usage, int, time.Duration, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.usage, p.cps, p.cpTime, p.step
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodPost || r.URL.Path != "/v1/messages" {
		p.forward(w, r, body, nil) // count_tokens and the like: not a step
		return
	}
	if p.Model != "" {
		if body, err = setModel(body, p.Model); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	p.mu.Lock()
	step, cpID, cpDur, err := p.checkpointLocked(r.Context())
	p.mu.Unlock()
	if err != nil {
		http.Error(w, "furcate: checkpoint: "+err.Error(), http.StatusBadGateway)
		return
	}

	var req struct {
		Model string `json:"model"`
	}
	json.Unmarshal(body, &req)
	ctx, span := p.Tracer.Start(p.Ctx, "chat "+req.Model, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "chat"),
		attribute.String("gen_ai.provider.name", "anthropic"),
		attribute.String("gen_ai.request.model", req.Model),
		attribute.String(controller.AttrRunID, p.RunID),
		attribute.Int(controller.AttrStepIndex, step),
		attribute.String(controller.AttrStepKind, runstore.KindLLM),
		attribute.String(controller.AttrCheckpointID, cpID),
		attribute.Float64(controller.AttrCheckpointMs, float64(cpDur.Microseconds())/1000),
		attribute.String(controller.AttrLLMRequest, string(body)),
		attribute.String("furcate.mode", "drop-in"),
	))
	defer span.End()

	resp := p.forward(w, r.WithContext(ctx), body, span)
	if resp == nil {
		return
	}
	u := resp.usage
	prices := p.Prices
	if prices == nil {
		prices = pricing.Defaults()
	}
	cost := prices.CostWithCache(resp.model, u.InputTokens, u.OutputTokens, u.CacheRead, u.CacheWrite)
	span.SetAttributes(
		attribute.String("gen_ai.response.model", resp.model),
		attribute.StringSlice("gen_ai.response.finish_reasons", []string{resp.stop}),
		attribute.Int64("gen_ai.usage.input_tokens", int64(u.InputTokens+u.CacheRead+u.CacheWrite)),
		attribute.Int64("gen_ai.usage.output_tokens", int64(u.OutputTokens)),
		attribute.Float64(controller.AttrCostUSD, cost),
		attribute.String(controller.AttrLLMResponse, resp.raw),
	)
	p.mu.Lock()
	p.usage = p.usage.Add(runstore.Usage{LLMCalls: 1, InputTokens: int64(u.InputTokens), OutputTokens: int64(u.OutputTokens),
		CacheReadTokens: int64(u.CacheRead), CacheWriteTokens: int64(u.CacheWrite), CostUSD: cost})
	p.mu.Unlock()
	p.Store.AddStep(context.WithoutCancel(ctx), runstore.Step{RunID: p.RunID, Index: step,
		SpanID: span.SpanContext().SpanID().String(), Kind: runstore.KindLLM, CheckpointID: cpID})
}

// checkpointLocked takes this step's checkpoint (or reuses a fork's restored
// one) and numbers the step. p.mu is held.
func (p *Proxy) checkpointLocked(ctx context.Context) (step int, id string, took time.Duration, err error) {
	step = p.step
	p.step++
	if p.firstCP != "" {
		id, p.firstCP = p.firstCP, ""
		return step, id, 0, nil
	}
	start := time.Now()
	ref, err := p.Driver.Checkpoint(ctx, p.inst)
	if err != nil {
		return 0, "", 0, err
	}
	took = time.Since(start)
	b := make([]byte, 8)
	rand.Read(b)
	id = "cp_" + hex.EncodeToString(b)
	caps := p.Driver.Caps()
	if err := p.Store.AddCheckpoint(ctx, runstore.Checkpoint{CheckpointID: id, RunID: p.RunID, Step: step,
		Driver: caps.Name, Level: int(caps.Level), DriverRef: string(ref)}); err != nil {
		return 0, "", 0, err
	}
	p.cps++
	p.cpTime += took
	return step, id, took, nil
}

type usage struct{ InputTokens, OutputTokens, CacheRead, CacheWrite uint64 }

type recorded struct {
	model, stop, raw string
	usage            usage
}

// forward sends the request upstream with the host's key and streams the
// answer back as it arrives, keeping a copy to record.
func (p *Proxy) forward(w http.ResponseWriter, r *http.Request, body []byte, span trace.Span) *recorded {
	upstream := p.Upstream
	if upstream == "" {
		upstream = "https://api.anthropic.com"
	}
	out, err := http.NewRequestWithContext(r.Context(), r.Method, strings.TrimRight(upstream, "/")+r.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return nil
	}
	for k, vs := range r.Header {
		switch strings.ToLower(k) {
		case "x-api-key", "authorization", "host", "content-length", "accept-encoding":
			continue
		}
		for _, v := range vs {
			out.Header.Add(k, v)
		}
	}
	out.Header.Set("x-api-key", p.APIKey)
	client := p.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(out)
	if err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		http.Error(w, "furcate: upstream: "+err.Error(), http.StatusBadGateway)
		return nil
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	var copyBuf bytes.Buffer
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32<<10)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			w.Write(buf[:n])
			copyBuf.Write(buf[:n])
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr != nil {
			break
		}
	}
	if span == nil {
		return nil
	}
	if resp.StatusCode >= 300 {
		span.SetStatus(codes.Error, fmt.Sprintf("upstream answered %d", resp.StatusCode))
	}
	rec := parseResponse(copyBuf.Bytes(), strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream"))
	return rec
}

// parseResponse pulls the model, stop reason and usage out of a JSON answer or
// an SSE stream (message_start carries input usage, message_delta the rest).
func parseResponse(b []byte, sse bool) *recorded {
	rec := &recorded{raw: string(b)}
	type msg struct {
		Model      string `json:"model"`
		StopReason string `json:"stop_reason"`
		Usage      struct {
			InputTokens              uint64 `json:"input_tokens"`
			OutputTokens             uint64 `json:"output_tokens"`
			CacheReadInputTokens     uint64 `json:"cache_read_input_tokens"`
			CacheCreationInputTokens uint64 `json:"cache_creation_input_tokens"`
		} `json:"usage"`
	}
	take := func(m msg) {
		if m.Model != "" {
			rec.model = m.Model
		}
		if m.StopReason != "" {
			rec.stop = m.StopReason
		}
		u := m.Usage
		rec.usage.InputTokens = max(rec.usage.InputTokens, u.InputTokens)
		rec.usage.OutputTokens = max(rec.usage.OutputTokens, u.OutputTokens)
		rec.usage.CacheRead = max(rec.usage.CacheRead, u.CacheReadInputTokens)
		rec.usage.CacheWrite = max(rec.usage.CacheWrite, u.CacheCreationInputTokens)
	}
	if !sse {
		var m msg
		if json.Unmarshal(b, &m) == nil {
			take(m)
		}
		return rec
	}
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok {
			continue
		}
		var ev struct {
			Type    string `json:"type"`
			Message msg    `json:"message"`
			Delta   struct {
				StopReason string `json:"stop_reason"`
			} `json:"delta"`
			Usage json.RawMessage `json:"usage"`
		}
		if json.Unmarshal([]byte(data), &ev) != nil {
			continue
		}
		switch ev.Type {
		case "message_start":
			take(ev.Message)
		case "message_delta":
			var m msg
			m.StopReason = ev.Delta.StopReason
			json.Unmarshal(ev.Usage, &m.Usage)
			take(m)
		}
	}
	return rec
}

// setModel replaces the top-level model, leaving every other field's bytes alone.
func setModel(body []byte, model string) ([]byte, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, err
	}
	m, _ := json.Marshal(model)
	top["model"] = m
	return json.Marshal(top)
}
