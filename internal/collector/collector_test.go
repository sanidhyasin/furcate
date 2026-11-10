package collector

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/sanidhyasin/furcate/internal/blob"
	"github.com/sanidhyasin/furcate/internal/model"
	"github.com/sanidhyasin/furcate/internal/pricing"
)

func kv(k string, v any) *commonpb.KeyValue {
	var av *commonpb.AnyValue
	switch x := v.(type) {
	case string:
		av = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: x}}
	case int:
		av = &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: int64(x)}}
	case float64:
		av = &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: x}}
	}
	return &commonpb.KeyValue{Key: k, Value: av}
}

var (
	traceID = bytes.Repeat([]byte{0xab}, 16)
	rootID  = []byte{1, 1, 1, 1, 1, 1, 1, 1}
	llmID   = []byte{2, 2, 2, 2, 2, 2, 2, 2}
	toolID  = []byte{3, 3, 3, 3, 3, 3, 3, 3}
)

func request(spans ...*tracepb.Span) *coltracepb.ExportTraceServiceRequest {
	return &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource:   &resourcepb.Resource{Attributes: []*commonpb.KeyValue{kv("service.name", "agent")}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}}
}

func llmSpan() *tracepb.Span {
	t0 := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	return &tracepb.Span{
		TraceId: traceID, SpanId: llmID, ParentSpanId: rootID, Name: "chat",
		StartTimeUnixNano: uint64(t0.UnixNano()), EndTimeUnixNano: uint64(t0.Add(1500 * time.Millisecond).UnixNano()),
		Attributes: []*commonpb.KeyValue{
			kv("gen_ai.operation.name", "chat"),
			kv("gen_ai.request.model", "model-a"),
			kv("gen_ai.response.model", "model-a-2026"),
			kv("gen_ai.usage.input_tokens", 1000),
			kv("gen_ai.usage.output_tokens", 200),
			kv("gen_ai.input.messages", `[{"role":"user","content":"hi"}]`),
			kv("short.attr", "small"),
			kv("long.attr", strings.Repeat("x", 5000)),
		},
	}
}

func newConverter(t *testing.T) *Converter {
	return &Converter{
		Seen:      blob.NewSeen(100),
		Prices:    pricing.Table{"model-a": {InputPerMTok: 3, OutputPerMTok: 15}},
		Threshold: 2048,
	}
}

func TestConvertLLMSpan(t *testing.T) {
	c := newConverter(t)
	rows, err := c.Convert(request(llmSpan()), 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows", len(rows))
	}
	r := rows[0]
	if r.Kind != model.KindLLM || r.Model != "model-a-2026" || r.Service != "agent" {
		t.Fatalf("kind %q model %q service %q", r.Kind, r.Model, r.Service)
	}
	if r.TraceID != hex.EncodeToString(traceID) || r.ParentSpanID != "0101010101010101" || r.RunID != r.TraceID {
		t.Fatalf("ids: %+v", r)
	}
	if r.TokensIn != 1000 || r.TokensOut != 200 || r.WALSeq != 7 {
		t.Fatalf("tokens %d/%d seq %d", r.TokensIn, r.TokensOut, r.WALSeq)
	}
	// Prefix match: model-a prices model-a-2026. 1000*3/1e6 + 200*15/1e6 = 0.006.
	if r.CostUSD < 0.00599 || r.CostUSD > 0.00601 {
		t.Fatalf("cost %f, want 0.006", r.CostUSD)
	}
	if r.Duration() != 1500*time.Millisecond {
		t.Fatalf("duration %s", r.Duration())
	}
	// A known payload key and an over-threshold attribute become payloads, and
	// their bytes ride along on the span.
	carried := map[string][]byte{}
	for _, p := range r.NewPayloads {
		carried[p.Hash] = p.Data
	}
	for _, k := range []string{"gen_ai.input.messages", "long.attr"} {
		h, ok := r.Payloads[k]
		if !ok {
			t.Fatalf("%s should be a payload", k)
		}
		if _, ok := r.Attributes[k]; ok {
			t.Fatalf("%s should not also be an attribute", k)
		}
		if data, ok := carried[h]; !ok || blob.Hash(data) != h {
			t.Fatalf("payload %s not carried with the span", k)
		}
	}
	if r.Attributes["short.attr"] != "small" {
		t.Fatal("short attribute should stay inline")
	}
}

func TestConvertRejectsBadIDs(t *testing.T) {
	c := newConverter(t)
	sp := llmSpan()
	sp.TraceId = []byte{1, 2, 3}
	if _, err := c.Convert(request(sp), 1); err == nil {
		t.Fatal("want an error for a short trace id")
	}
}

func TestCostAttributeWins(t *testing.T) {
	c := newConverter(t)
	sp := llmSpan()
	sp.Attributes = append(sp.Attributes, kv("furcate.cost_usd", 1.25))
	rows, _ := c.Convert(request(sp), 1)
	if rows[0].CostUSD != 1.25 {
		t.Fatalf("cost %f, want 1.25 from the attribute", rows[0].CostUSD)
	}
}

func TestPayloadDedupe(t *testing.T) {
	c := newConverter(t)
	first, _ := c.Convert(request(llmSpan()), 1)
	// Not confirmed stored yet: a second span must carry the bytes too, or a
	// dropped first span would leave the second one's payloads missing.
	second, _ := c.Convert(request(llmSpan()), 2)
	if len(first[0].NewPayloads) != 2 || len(second[0].NewPayloads) != 2 {
		t.Fatal("payloads must ride along until they are confirmed stored")
	}
	for _, p := range first[0].NewPayloads {
		c.Seen.Add(p.Hash)
	}
	third, _ := c.Convert(request(llmSpan()), 3)
	if len(third[0].NewPayloads) != 0 || len(third[0].Payloads) != 2 {
		t.Fatal("stored payloads should be referenced, not carried again")
	}
}

func TestHexIDsToBase64(t *testing.T) {
	in := []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"abababababababababababababababab","spanId":"0101010101010101","name":"x"}]}]}]}`)
	out, err := hexIDsToBase64(in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"traceId":"q6urq6urq6urq6urq6urqw=="`) {
		t.Fatalf("trace id not converted: %s", out)
	}
}

// --- sampler and tracker ---------------------------------------------------

func span(trace, id, parent string, status string, attrs map[string]string) model.Span {
	if attrs == nil {
		attrs = map[string]string{}
	}
	return model.Span{TraceID: trace, SpanID: id, ParentSpanID: parent, Status: status, Attributes: attrs, WALSeq: 1}
}

func TestSamplerHoldsUntilComplete(t *testing.T) {
	var kept, dropped []model.Span
	s := newSampler(0.0, time.Second, time.Hour,
		func(sp []model.Span) { kept = append(kept, sp...) },
		func(sp []model.Span) { dropped = append(dropped, sp...) })

	now := time.Now()
	s.Add([]model.Span{span("t1", "a", "root", model.StatusOK, nil)})
	s.Tick(now.Add(5 * time.Second))
	if len(kept)+len(dropped) != 0 {
		t.Fatal("a trace without its root must not be decided")
	}
	s.Add([]model.Span{span("t1", "root", "", model.StatusOK, nil)})
	s.Tick(time.Now().Add(2 * time.Second))
	if len(dropped) != 2 {
		t.Fatalf("ratio 0 should drop the whole trace; kept %d dropped %d", len(kept), len(dropped))
	}
	// A late span follows the decision.
	s.Add([]model.Span{span("t1", "late", "root", model.StatusOK, nil)})
	if len(dropped) != 3 {
		t.Fatal("late span should be dropped like its trace")
	}
}

func TestSamplerAlwaysKeepsErrorsAndCheckpoints(t *testing.T) {
	var kept []model.Span
	s := newSampler(0.0, time.Second, time.Hour,
		func(sp []model.Span) { kept = append(kept, sp...) }, func([]model.Span) {})
	s.Add([]model.Span{
		span("err", "a", "", model.StatusError, nil),
		span("cp", "b", "", model.StatusOK, map[string]string{attrCheckpoint: "cp-1"}),
		span("plain", "c", "", model.StatusOK, nil),
	})
	s.Tick(time.Now().Add(2 * time.Second))
	if len(kept) != 2 {
		t.Fatalf("kept %d spans, want the errored and checkpointed runs only", len(kept))
	}
}

func TestSamplerMaxAge(t *testing.T) {
	var kept []model.Span
	s := newSampler(0.5, time.Second, time.Minute,
		func(sp []model.Span) { kept = append(kept, sp...) }, func([]model.Span) {})
	s.Add([]model.Span{span("t", "child", "never-arrives", model.StatusError, nil)})
	s.Tick(time.Now().Add(2 * time.Minute))
	if len(kept) != 1 || s.Held() != 0 {
		t.Fatal("an old trace must be decided even without its root")
	}
}

func TestTrackerLowWater(t *testing.T) {
	tr := newTracker()
	tr.add(3, 2)
	tr.add(5, 1)
	if lw := tr.lowWater(9); lw != 3 {
		t.Fatalf("low water %d, want 3", lw)
	}
	tr.done([]model.Span{{WALSeq: 3}, {WALSeq: 3}})
	if lw := tr.lowWater(9); lw != 5 {
		t.Fatalf("low water %d, want 5", lw)
	}
	tr.done([]model.Span{{WALSeq: 5}})
	if lw := tr.lowWater(9); lw != 9 {
		t.Fatalf("low water %d, want 9 with nothing pending", lw)
	}
}

// --- end to end through HTTP, with a fake writer ----------------------------

type memWriter struct {
	mu    sync.Mutex
	spans []model.Span
	fail  int // fail this many inserts first
}

func (m *memWriter) Insert(_ context.Context, s []model.Span) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail > 0 {
		m.fail--
		return io.ErrUnexpectedEOF
	}
	m.spans = append(m.spans, s...)
	return nil
}

func (m *memWriter) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.spans)
}

func startCollector(t *testing.T, walDir string, w Writer) (*Collector, context.CancelFunc, chan error) {
	t.Helper()
	c, err := New(Config{
		HTTPAddr: "127.0.0.1:0", GRPCAddr: "off", WALDir: walDir,
		BatchSize: 100, FlushInterval: 20 * time.Millisecond, ShutdownTimeout: 200 * time.Millisecond,
	}, w, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	select {
	case <-c.Ready():
	case err := <-done:
		t.Fatalf("collector stopped before it was ready: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("collector not ready after 10s")
	}
	return c, cancel, done
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHTTPIngestFlushesAndRetries(t *testing.T) {
	w := &memWriter{fail: 2}
	c, cancel, done := startCollector(t, t.TempDir(), w)
	srv := httptest.NewServer(c.Handler())
	defer srv.Close()

	body, _ := proto.Marshal(request(llmSpan()))
	resp, err := http.Post(srv.URL+"/v1/traces", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	waitFor(t, func() bool { return w.count() == 1 })
	if st := c.Stats(); st.InsertFailures != 2 || st.SpansInflight != 0 {
		t.Fatalf("stats after retries: %+v", st)
	}

	// Garbage is refused before it reaches the WAL.
	resp, _ = http.Post(srv.URL+"/v1/traces", "application/x-protobuf", strings.NewReader("not a protobuf"))
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("garbage got %d, want 400", resp.StatusCode)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCrashRecoveryReplaysWAL(t *testing.T) {
	walDir := t.TempDir()
	// First collector: ClickHouse is down for the whole run.
	down := &memWriter{fail: 1 << 30}
	c, cancel, done := startCollector(t, walDir, down)
	if err := c.ingest(request(llmSpan()), nil); err != nil {
		t.Fatal(err)
	}
	cancel()
	<-done

	// Second collector: ClickHouse is back. The acknowledged span arrives.
	up := &memWriter{}
	_, cancel2, done2 := startCollector(t, walDir, up)
	waitFor(t, func() bool { return up.count() == 1 })
	cancel2()
	<-done2
}

func TestGRPCExport(t *testing.T) {
	w := &memWriter{}
	c, cancel, done := startCollector(t, t.TempDir(), w)
	g := &grpcService{c: c}
	if _, err := g.Export(context.Background(), request(llmSpan())); err != nil {
		t.Fatal(err)
	}
	bad := llmSpan()
	bad.SpanId = []byte{1}
	_, err := g.Export(context.Background(), request(bad))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad span: got %v, want InvalidArgument", err)
	}
	waitFor(t, func() bool { return w.count() == 1 })
	cancel()
	<-done
}
