// Package collector accepts OTLP traces over HTTP and gRPC and writes them to the
// trace store. The path of one export request:
//
//	parse -> convert (payloads to blob store) -> WAL append + fsync -> ack
//	      -> tail sampler -> batcher -> ClickHouse -> WAL segments released
package collector

import (
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/sanidhyasin/furcate/internal/blob"
	"github.com/sanidhyasin/furcate/internal/model"
	"github.com/sanidhyasin/furcate/internal/pricing"
	"github.com/sanidhyasin/furcate/internal/wal"
)

// Config sets up a collector. Zero values get the defaults in New.
type Config struct {
	HTTPAddr string // OTLP/HTTP, default :4318
	GRPCAddr string // OTLP/gRPC, default :4317; "off" disables it

	WALDir         string
	WALSegmentSize int64
	SeenCapacity   int // payload hashes remembered as stored

	BatchSize     int           // flush at this many spans
	FlushInterval time.Duration // or after this long
	QueueSize     int           // export requests queued before the server pushes back

	SampleRatio float64       // fraction of ordinary runs kept; >= 1 keeps all
	QuietPeriod time.Duration // a run is complete once its root ended and it has been quiet this long
	MaxTraceAge time.Duration // decide on a run after this long regardless

	PayloadThreshold int // string attributes longer than this go to the blob store
	Prices           pricing.Table

	ShutdownTimeout time.Duration // how long shutdown waits for clients and the last flush
}

func (c *Config) defaults() {
	if c.HTTPAddr == "" {
		c.HTTPAddr = ":4318"
	}
	if c.GRPCAddr == "" {
		c.GRPCAddr = ":4317"
	}
	if c.WALDir == "" {
		c.WALDir = "data/wal"
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 10_000
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 1024
	}
	if c.SampleRatio <= 0 {
		c.SampleRatio = 1
	}
	if c.QuietPeriod <= 0 {
		c.QuietPeriod = 30 * time.Second
	}
	if c.MaxTraceAge <= 0 {
		c.MaxTraceAge = 30 * time.Minute
	}
	if c.PayloadThreshold == 0 {
		c.PayloadThreshold = 2048
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 10 * time.Second
	}
}

// Collector is one running collector.
type Collector struct {
	cfg     Config
	log     *slog.Logger
	wal     *wal.WAL
	seen    *blob.Seen
	conv    *Converter
	tracker *tracker
	sampler *Sampler
	batcher *Batcher

	requests, received, rejected atomic.Uint64
	walReleased                  atomic.Uint64

	ready chan struct{} // closed once the WAL is replayed and the servers listen
}

// Ready is closed once the collector has replayed its WAL and accepts requests.
func (c *Collector) Ready() <-chan struct{} { return c.ready }

var (
	errBusy       = errors.New("collector is saturated; retry later")
	errBadRequest = errors.New("bad request")
)

// New opens the WAL. Nothing is served until Run.
func New(cfg Config, w Writer, log *slog.Logger) (*Collector, error) {
	cfg.defaults()
	l, err := wal.Open(cfg.WALDir, cfg.WALSegmentSize)
	if err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}
	seen := blob.NewSeen(cfg.SeenCapacity)
	c := &Collector{
		cfg: cfg, log: log, wal: l, seen: seen, tracker: newTracker(), ready: make(chan struct{}),
		conv: &Converter{Seen: seen, Prices: cfg.Prices, Threshold: cfg.PayloadThreshold},
	}
	c.batcher = newBatcher(w, cfg.BatchSize, cfg.FlushInterval, cfg.QueueSize, c.flushed, log)
	c.batcher.drainTimeout = cfg.ShutdownTimeout
	c.sampler = newSampler(cfg.SampleRatio, cfg.QuietPeriod, cfg.MaxTraceAge, c.batcher.Submit, c.tracker.done)
	return c, nil
}

// Run replays the WAL, serves until ctx is done, then drains and flushes.
func (c *Collector) Run(ctx context.Context) error {
	bctx, stopBatcher := context.WithCancel(context.Background())
	batcherDone := make(chan struct{})
	go func() { c.batcher.Run(bctx); close(batcherDone) }()
	defer func() {
		stopBatcher()
		<-batcherDone
		c.release()
		c.wal.Close()
	}()

	replayed := 0
	err := c.wal.Replay(func(seq uint64, payload []byte) error {
		req := &coltracepb.ExportTraceServiceRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return fmt.Errorf("record %d: %w", seq, err)
		}
		rows, err := c.conv.Convert(req, seq)
		if err != nil {
			return fmt.Errorf("record %d: %w", seq, err)
		}
		c.tracker.add(seq, len(rows))
		c.sampler.Add(rows)
		replayed++
		return nil
	})
	if err != nil {
		return fmt.Errorf("wal replay: %w", err)
	}
	if replayed > 0 {
		c.log.Info("replayed WAL", "records", replayed)
	}

	httpLn, err := net.Listen("tcp", c.cfg.HTTPAddr)
	if err != nil {
		return err
	}
	httpSrv := &http.Server{Handler: c.Handler(), ReadHeaderTimeout: 10 * time.Second}
	var grpcSrv *grpc.Server
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := httpSrv.Serve(httpLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			c.log.Error("http server", "err", err)
		}
	}()
	if c.cfg.GRPCAddr != "off" {
		grpcLn, err := net.Listen("tcp", c.cfg.GRPCAddr)
		if err != nil {
			httpSrv.Close()
			return err
		}
		grpcSrv = grpc.NewServer(grpc.MaxRecvMsgSize(wal.MaxRecord))
		coltracepb.RegisterTraceServiceServer(grpcSrv, &grpcService{c: c})
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := grpcSrv.Serve(grpcLn); err != nil {
				c.log.Error("grpc server", "err", err)
			}
		}()
	}
	close(c.ready)
	c.log.Info("collector listening", "http", httpLn.Addr().String(), "grpc", c.cfg.GRPCAddr,
		"batch", c.cfg.BatchSize, "flush", c.cfg.FlushInterval, "sample", c.cfg.SampleRatio)

	// Release every second: the watermark it saves bounds how much a crash
	// replays, and so how many duplicate rows recovery can write.
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case now := <-tick.C:
			c.sampler.Tick(now)
			c.release()
		case <-ctx.Done():
			c.log.Info("shutting down")
			sctx, cancel := context.WithTimeout(context.Background(), c.cfg.ShutdownTimeout)
			httpSrv.Shutdown(sctx)
			cancel()
			if grpcSrv != nil {
				grpcSrv.GracefulStop()
			}
			wg.Wait()
			c.sampler.Flush()
			return nil
		}
	}
}

// flushed runs after a batch reached ClickHouse: its payloads are now known to be
// stored, and its WAL records are one step closer to release.
func (c *Collector) flushed(spans []model.Span) {
	var hashes []string
	for i := range spans {
		for _, p := range spans[i].NewPayloads {
			hashes = append(hashes, p.Hash)
		}
	}
	c.seen.Add(hashes...)
	c.tracker.done(spans)
}

// release drops WAL segments whose spans have all reached ClickHouse.
func (c *Collector) release() {
	next := c.wal.NextSeq() // read first: every seq below it is registered
	n, err := c.wal.Release(c.tracker.lowWater(next))
	if err != nil {
		c.log.Warn("wal release", "err", err)
	}
	c.walReleased.Add(uint64(n))
}

// ingest is the shared path for HTTP and gRPC. raw is the request's protobuf
// encoding if the caller already has it.
func (c *Collector) ingest(req *coltracepb.ExportTraceServiceRequest, raw []byte) error {
	c.requests.Add(1)
	if c.batcher.Saturated() {
		c.rejected.Add(1)
		return errBusy
	}
	rows, err := c.conv.Convert(req, 0)
	if err != nil {
		c.rejected.Add(1)
		return fmt.Errorf("%w: %v", errBadRequest, err)
	}
	if len(rows) == 0 {
		return nil
	}
	if raw == nil {
		if raw, err = proto.Marshal(req); err != nil {
			return err
		}
	}
	seq, err := c.wal.Append(raw, func(seq uint64) { c.tracker.add(seq, len(rows)) })
	if seq == 0 {
		return fmt.Errorf("wal append: %w", err)
	}
	// The record is written (and, unless err != nil, synced). Process it either
	// way: if the fsync failed the client retries, and a duplicate beats a loss.
	for i := range rows {
		rows[i].WALSeq = seq
	}
	c.sampler.Add(rows)
	c.received.Add(uint64(len(rows)))
	if err != nil {
		return fmt.Errorf("%w: wal sync: %v", errBusy, err)
	}
	return nil
}

// Handler serves OTLP/HTTP on /v1/traces plus /healthz, /debug/stats and /debug/pprof.
func (c *Collector) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/traces", c.handleTraces)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok\n") })
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("GET /debug/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(c.Stats())
	})
	return mux
}

func (c *Collector) handleTraces(w http.ResponseWriter, r *http.Request) {
	var body io.Reader = http.MaxBytesReader(w, r.Body, wal.MaxRecord)
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(body)
		if err != nil {
			http.Error(w, "bad gzip body", http.StatusBadRequest)
			return
		}
		defer gz.Close()
		body = io.LimitReader(gz, wal.MaxRecord+1)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		http.Error(w, "reading body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(data) > wal.MaxRecord {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}

	ct, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	req := &coltracepb.ExportTraceServiceRequest{}
	var raw []byte
	switch ct {
	case "application/x-protobuf", "application/protobuf":
		if err := proto.Unmarshal(data, req); err != nil {
			http.Error(w, "bad protobuf: "+err.Error(), http.StatusBadRequest)
			return
		}
		raw = data
	case "application/json":
		fixed, err := hexIDsToBase64(data)
		if err == nil {
			err = protojson.Unmarshal(fixed, req)
		}
		if err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
	default:
		http.Error(w, "unsupported content type "+ct, http.StatusUnsupportedMediaType)
		return
	}

	if err := c.ingest(req, raw); err != nil {
		switch {
		case errors.Is(err, errBusy):
			w.Header().Set("Retry-After", "1")
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		case errors.Is(err, errBadRequest):
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if ct == "application/json" {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, "{}")
		return
	}
	out, _ := proto.Marshal(&coltracepb.ExportTraceServiceResponse{})
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(out)
}

// hexIDsToBase64 fixes the one place OTLP/JSON departs from protobuf JSON: trace
// and span IDs are hex strings, while protojson expects base64 for bytes.
func hexIDsToBase64(data []byte) ([]byte, error) {
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			for k, val := range x {
				switch k {
				case "traceId", "spanId", "parentSpanId", "trace_id", "span_id", "parent_span_id":
					if s, ok := val.(string); ok && s != "" {
						if b, err := hex.DecodeString(s); err == nil {
							x[k] = base64.StdEncoding.EncodeToString(b)
						}
					}
				default:
					walk(val)
				}
			}
		case []any:
			for _, e := range x {
				walk(e)
			}
		}
	}
	walk(doc)
	return json.Marshal(doc)
}

type grpcService struct {
	coltracepb.UnimplementedTraceServiceServer
	c *Collector
}

func (g *grpcService) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	if err := g.c.ingest(req, nil); err != nil {
		switch {
		case errors.Is(err, errBusy):
			return nil, status.Error(codes.Unavailable, err.Error())
		case errors.Is(err, errBadRequest):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// Stats is a snapshot of the collector's counters, served on /debug/stats.
type Stats struct {
	Requests       uint64  `json:"requests"`
	Rejected       uint64  `json:"rejected"`
	SpansReceived  uint64  `json:"spans_received"`
	SpansFlushed   uint64  `json:"spans_flushed"`
	SpansKept      uint64  `json:"spans_kept"`
	SpansDropped   uint64  `json:"spans_dropped"`
	SpansInflight  int     `json:"spans_inflight"`
	TracesHeld     int     `json:"traces_held"`
	Batches        uint64  `json:"batches"`
	InsertFailures uint64  `json:"insert_failures"`
	InsertP50Ms    float64 `json:"insert_p50_ms"`
	InsertP99Ms    float64 `json:"insert_p99_ms"`
	QueueLen       int     `json:"queue_len"`
	WALSegments    int     `json:"wal_segments"`
	WALReleased    uint64  `json:"wal_segments_released"`
	Payloads       uint64  `json:"payloads"`      // payload references seen
	PayloadBytes   uint64  `json:"payload_bytes"` // their bytes, before dedupe
	SeenLookups    uint64  `json:"seen_lookups"`  // stored-payload cache lookups
	SeenHits       uint64  `json:"seen_hits"`     // payloads skipped as already stored
}

// Stats returns the current counters.
func (c *Collector) Stats() Stats {
	p50, p99 := c.batcher.LatencyQuantiles()
	lookups, hits := c.seen.Stats()
	return Stats{
		Requests:       c.requests.Load(),
		Rejected:       c.rejected.Load(),
		SpansReceived:  c.received.Load(),
		SpansFlushed:   c.batcher.spans.Load(),
		SpansKept:      c.sampler.kept.Load(),
		SpansDropped:   c.sampler.dropped.Load(),
		SpansInflight:  c.tracker.inflight(),
		TracesHeld:     c.sampler.Held(),
		Batches:        c.batcher.batches.Load(),
		InsertFailures: c.batcher.failures.Load(),
		InsertP50Ms:    float64(p50.Microseconds()) / 1000,
		InsertP99Ms:    float64(p99.Microseconds()) / 1000,
		QueueLen:       len(c.batcher.in),
		WALSegments:    c.wal.Segments(),
		WALReleased:    c.walReleased.Load(),
		Payloads:       c.conv.payloads.Load(),
		PayloadBytes:   c.conv.payloadBytes.Load(),
		SeenLookups:    lookups,
		SeenHits:       hits,
	}
}
