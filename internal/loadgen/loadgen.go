// Package loadgen sends synthetic agent runs to a collector over OTLP/HTTP, shaped
// like the real thing: a root agent span, then alternating LLM and tool steps; the
// conversation grows every step; a long system prompt repeats across runs (it
// should dedupe); and the root span is sent last, as an SDK would.
package loadgen

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Config shapes the load.
type Config struct {
	Endpoint     string        // collector base URL, e.g. http://localhost:4318
	Duration     time.Duration // stop after this long (0 = until Runs are sent)
	Runs         int           // stop after this many runs (0 = until Duration)
	Concurrency  int
	Steps        int     // LLM steps per run; each is followed by a tool step
	PayloadScale float64 // multiplies every payload size
	ErrorRate    float64 // fraction of runs whose last tool call fails
	BatchSpans   int     // max spans per export request
	Gzip         bool
	Checkpoints  bool // tag each step with furcate.checkpoint_id
}

// Result is what the run achieved, as seen by the client. Spans counts only
// spans in acknowledged requests; Runs only runs whose every request was
// acknowledged.
type Result struct {
	Runs, Spans, Requests, Retries, Failures uint64
	PayloadBytes, WireBytes                  uint64
	Elapsed                                  time.Duration
}

func (r Result) String() string {
	secs := r.Elapsed.Seconds()
	return fmt.Sprintf(
		"runs %d  spans %d  requests %d  retries %d  failures %d\n"+
			"elapsed %.1fs  %.0f spans/s  %.0f runs/s  payload %.1f MiB/s  wire %.1f MiB/s",
		r.Runs, r.Spans, r.Requests, r.Retries, r.Failures,
		secs, float64(r.Spans)/secs, float64(r.Runs)/secs,
		float64(r.PayloadBytes)/secs/(1<<20), float64(r.WireBytes)/secs/(1<<20))
}

var models = []string{"claude-sonnet-5-5", "claude-haiku-5-5", "claude-opus-5-5"}
var tools = []string{"bash", "read_file", "edit_file", "run_tests", "grep"}

// Run sends load until the duration passes or the run count is reached.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 8
	}
	if cfg.Steps <= 0 {
		cfg.Steps = 10
	}
	if cfg.PayloadScale <= 0 {
		cfg.PayloadScale = 1
	}
	if cfg.BatchSpans <= 0 {
		cfg.BatchSpans = 512
	}
	if cfg.Duration <= 0 && cfg.Runs <= 0 {
		cfg.Runs = 100
	}
	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}

	corpus := newCorpus()
	system := corpus.text(int(6000 * cfg.PayloadScale)) // shared by every run
	client := &http.Client{Timeout: 30 * time.Second}
	url := strings.TrimRight(cfg.Endpoint, "/") + "/v1/traces"

	var res Result
	var started atomic.Int64
	var firstErr error
	var errOnce sync.Once
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := mrand.New(mrand.NewPCG(seed, uint64(time.Now().UnixNano())))
			g := &gen{cfg: cfg, rng: rng, corpus: corpus, system: system}
			for ctx.Err() == nil {
				if cfg.Runs > 0 && started.Add(1) > int64(cfg.Runs) {
					return
				}
				spans, payload := g.run()
				ok := true
				for i := 0; i < len(spans); i += cfg.BatchSpans {
					batch := spans[i:min(i+cfg.BatchSpans, len(spans))]
					n, retries, err := send(ctx, client, url, batch, cfg.Gzip)
					atomic.AddUint64(&res.Requests, 1)
					atomic.AddUint64(&res.Retries, retries)
					atomic.AddUint64(&res.WireBytes, uint64(n))
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						atomic.AddUint64(&res.Failures, 1)
						errOnce.Do(func() { firstErr = err })
						ok = false
						continue
					}
					atomic.AddUint64(&res.Spans, uint64(len(batch)))
				}
				if ok { // only runs the collector fully acknowledged count
					atomic.AddUint64(&res.Runs, 1)
					atomic.AddUint64(&res.PayloadBytes, uint64(payload))
				}
			}
		}(uint64(w))
	}
	wg.Wait()
	res.Elapsed = time.Since(start)
	if res.Spans == 0 && firstErr != nil {
		return res, firstErr
	}
	return res, nil
}

// send posts one export request, retrying while the collector says it is busy.
func send(ctx context.Context, client *http.Client, url string, spans []*tracepb.Span, gz bool) (wire int, retries uint64, err error) {
	req := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{str("service.name", "loadgen-agent")}},
		ScopeSpans: []*tracepb.ScopeSpans{{
			Scope: &commonpb.InstrumentationScope{Name: "furcate-loadgen"},
			Spans: spans,
		}},
	}}}
	body, err := proto.Marshal(req)
	if err != nil {
		return 0, 0, err
	}
	if gz {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		zw.Write(body)
		zw.Close()
		body = buf.Bytes()
	}
	backoff := 50 * time.Millisecond
	for {
		hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return 0, retries, err
		}
		hreq.Header.Set("Content-Type", "application/x-protobuf")
		if gz {
			hreq.Header.Set("Content-Encoding", "gzip")
		}
		resp, err := client.Do(hreq)
		if err != nil {
			return len(body), retries, err
		}
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK:
			return len(body), retries, nil
		case http.StatusServiceUnavailable, http.StatusTooManyRequests:
			retries++
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, 2*time.Second)
				continue
			case <-ctx.Done():
				return len(body), retries, ctx.Err()
			}
		default:
			return len(body), retries, fmt.Errorf("collector returned %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
		}
	}
}

// gen builds one synthetic run at a time.
type gen struct {
	cfg    Config
	rng    *mrand.Rand
	corpus *corpus
	system string
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// run returns the run's spans, root last, and the payload bytes they carry.
func (g *gen) run() ([]*tracepb.Span, int) {
	traceID := randBytes(16)
	rootID := randBytes(8)
	model := models[g.rng.IntN(len(models))]
	fail := g.rng.Float64() < g.cfg.ErrorRate
	scale := g.cfg.PayloadScale

	// Lay the steps out back to back, ending now.
	type step struct{ llm, tool time.Duration }
	steps := make([]step, g.cfg.Steps)
	var total time.Duration
	for i := range steps {
		steps[i] = step{
			llm:  time.Duration(500+g.rng.IntN(3500)) * time.Millisecond,
			tool: time.Duration(50+g.rng.IntN(1950)) * time.Millisecond,
		}
		total += steps[i].llm + steps[i].tool
	}
	end := time.Now()
	t := end.Add(-total)
	runStart := t

	var spans []*tracepb.Span
	payload := 0
	history := []message{{Role: "user", Content: g.corpus.text(int(400 * scale))}}
	for i, st := range steps {
		out := g.corpus.text(int(float64(200+g.rng.IntN(1300)) * scale))
		inTok := uint64(len(g.system)/4 + jsonLen(history)/4)
		outTok := uint64(len(out) / 4)
		hist, _ := json.Marshal(history)
		llm := &tracepb.Span{
			TraceId: traceID, SpanId: randBytes(8), ParentSpanId: rootID,
			Name: "chat " + model, Kind: tracepb.Span_SPAN_KIND_CLIENT,
			StartTimeUnixNano: uint64(t.UnixNano()), EndTimeUnixNano: uint64(t.Add(st.llm).UnixNano()),
			Attributes: []*commonpb.KeyValue{
				str("gen_ai.operation.name", "chat"),
				str("gen_ai.provider.name", "anthropic"),
				str("gen_ai.request.model", model),
				num("gen_ai.usage.input_tokens", int64(inTok)),
				num("gen_ai.usage.output_tokens", int64(outTok)),
				str("gen_ai.system_instructions", g.system),
				str("gen_ai.input.messages", string(hist)),
				str("gen_ai.output.messages", out),
			},
			Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
		}
		if g.cfg.Checkpoints {
			llm.Attributes = append(llm.Attributes, str("furcate.checkpoint_id", fmt.Sprintf("cp-%x-%d", traceID[:4], i)))
		}
		payload += len(g.system) + len(hist) + len(out)
		t = t.Add(st.llm)

		tool := tools[g.rng.IntN(len(tools))]
		args := g.corpus.text(int(float64(80+g.rng.IntN(300)) * scale))
		result := g.corpus.text(int(float64(300+g.rng.IntN(3700)) * scale))
		toolSpan := &tracepb.Span{
			TraceId: traceID, SpanId: randBytes(8), ParentSpanId: rootID,
			Name: "execute_tool " + tool, Kind: tracepb.Span_SPAN_KIND_INTERNAL,
			StartTimeUnixNano: uint64(t.UnixNano()), EndTimeUnixNano: uint64(t.Add(st.tool).UnixNano()),
			Attributes: []*commonpb.KeyValue{
				str("gen_ai.operation.name", "execute_tool"),
				str("gen_ai.tool.name", tool),
				str("gen_ai.tool.call.arguments", args),
				str("gen_ai.tool.call.result", result),
			},
			Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
		}
		if fail && i == len(steps)-1 {
			toolSpan.Status = &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "exit status 1: test still failing"}
		}
		payload += len(args) + len(result)
		t = t.Add(st.tool)

		spans = append(spans, llm, toolSpan)
		history = append(history, message{"assistant", out}, message{"tool", result})
	}

	root := &tracepb.Span{
		TraceId: traceID, SpanId: rootID,
		Name: "invoke_agent fix-failing-test", Kind: tracepb.Span_SPAN_KIND_INTERNAL,
		StartTimeUnixNano: uint64(runStart.UnixNano()), EndTimeUnixNano: uint64(end.UnixNano()),
		Attributes: []*commonpb.KeyValue{
			str("gen_ai.operation.name", "invoke_agent"),
			str("gen_ai.agent.name", "loadgen-coder"),
		},
		Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
	}
	if fail {
		root.Status = &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "task failed"}
	}
	return append(spans, root), payload
}

func jsonLen(m []message) int {
	n := 0
	for _, x := range m {
		n += len(x.Content) + 32
	}
	return n
}

func str(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func num(k string, v int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}}}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// corpus produces text that compresses like prose and code, cheaply: random
// windows over a fixed word list rather than random bytes.
type corpus struct{ words string }

func newCorpus() *corpus {
	base := strings.Fields(`the agent reads the failing test and opens the fixture file then runs the suite
		again func main error return nil if err != nil import package handler request response context
		stack trace panic runtime goroutine assert expected got diff line column module build cache
		install dependency version migration database schema table index query select insert update
		server port listen timeout retry backoff config env path file directory permission denied`)
	var b strings.Builder
	r := mrand.New(mrand.NewPCG(1, 2))
	for b.Len() < 1<<20 {
		b.WriteString(base[r.IntN(len(base))])
		if r.IntN(12) == 0 {
			b.WriteString(".\n")
		} else {
			b.WriteByte(' ')
		}
	}
	return &corpus{words: b.String()}
}

func (c *corpus) text(n int) string {
	if n <= 0 {
		return ""
	}
	n = min(n, len(c.words))
	off := mrand.IntN(len(c.words) - n + 1)
	return c.words[off : off+n]
}
