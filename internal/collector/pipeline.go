package collector

import (
	"context"
	"hash/fnv"
	"log/slog"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanidhyasin/furcate/internal/model"
)

// Writer inserts a batch of spans. The ClickHouse store implements it.
type Writer interface {
	Insert(ctx context.Context, spans []model.Span) error
}

// ---------------------------------------------------------------------------
// tracker: which WAL records still have spans that haven't reached ClickHouse.

type tracker struct {
	mu      sync.Mutex
	pending map[uint64]int // WAL seq -> spans not yet flushed or dropped
}

func newTracker() *tracker { return &tracker{pending: map[uint64]int{}} }

// add registers a record's span count. The WAL calls it under its own lock, at
// the moment the seq is assigned, so no seq is ever unaccounted for.
func (t *tracker) add(seq uint64, n int) {
	if n == 0 {
		return
	}
	t.mu.Lock()
	t.pending[seq] += n
	t.mu.Unlock()
}

// done marks spans as flushed or deliberately dropped.
func (t *tracker) done(spans []model.Span) {
	t.mu.Lock()
	for _, s := range spans {
		if n := t.pending[s.WALSeq] - 1; n > 0 {
			t.pending[s.WALSeq] = n
		} else {
			delete(t.pending, s.WALSeq)
		}
	}
	t.mu.Unlock()
}

// lowWater returns the smallest seq still pending, or next if none are. Read the
// WAL's next seq before calling: every seq below it is already registered.
func (t *tracker) lowWater(next uint64) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	low := next
	for seq := range t.pending {
		low = min(low, seq)
	}
	return low
}

func (t *tracker) inflight() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, c := range t.pending {
		n += c
	}
	return n
}

// ---------------------------------------------------------------------------
// sampler: tail sampling over whole runs.

// Sampler keeps or drops whole traces once they are complete, never single spans:
// a run with a missing span can't be replayed. Errored runs and runs with a live
// checkpoint are always kept. With Ratio >= 1 it passes spans straight through.
type Sampler struct {
	Ratio  float64       // fraction of ordinary traces to keep
	Quiet  time.Duration // a trace is complete once its root ended and it has been quiet this long
	MaxAge time.Duration // decide anyway after this long, so memory stays bounded

	keep func([]model.Span)
	drop func([]model.Span)

	mu      sync.Mutex
	held    map[string]*heldTrace
	decided map[string]decision

	kept, dropped atomic.Uint64
}

type heldTrace struct {
	spans       []model.Span
	first, last time.Time
	rootEnded   bool
	mustKeep    bool
}

type decision struct {
	keep    bool
	expires time.Time
}

// attrCheckpoint marks a step that has a checkpoint; such runs are never sampled out.
const attrCheckpoint = "furcate.checkpoint_id"

func newSampler(ratio float64, quiet, maxAge time.Duration, keep, drop func([]model.Span)) *Sampler {
	return &Sampler{
		Ratio: ratio, Quiet: quiet, MaxAge: maxAge,
		keep: keep, drop: drop,
		held: map[string]*heldTrace{}, decided: map[string]decision{},
	}
}

func (s *Sampler) passthrough() bool { return s.Ratio >= 1 }

// Add takes spans from one export request.
func (s *Sampler) Add(spans []model.Span) {
	if s.passthrough() {
		s.kept.Add(uint64(len(spans)))
		s.keep(spans)
		return
	}
	now := time.Now()
	var toKeep, toDrop []model.Span
	s.mu.Lock()
	for _, sp := range spans {
		if d, ok := s.decided[sp.TraceID]; ok { // late span: follow the decision
			if d.keep {
				toKeep = append(toKeep, sp)
			} else {
				toDrop = append(toDrop, sp)
			}
			continue
		}
		h := s.held[sp.TraceID]
		if h == nil {
			h = &heldTrace{first: now}
			s.held[sp.TraceID] = h
		}
		h.spans = append(h.spans, sp)
		h.last = now
		if sp.IsRoot() {
			h.rootEnded = true // OTLP sends a span when it ends
		}
		if sp.Status == model.StatusError || sp.Attributes[attrCheckpoint] != "" {
			h.mustKeep = true
		}
	}
	s.mu.Unlock()
	s.route(toKeep, toDrop)
}

// Tick decides every trace that is complete or too old, and forgets old decisions.
func (s *Sampler) Tick(now time.Time) {
	if s.passthrough() {
		return
	}
	var toKeep, toDrop []model.Span
	s.mu.Lock()
	for id, h := range s.held {
		complete := h.rootEnded && now.Sub(h.last) >= s.Quiet
		if !complete && now.Sub(h.first) < s.MaxAge {
			continue
		}
		keep := h.mustKeep || keepByHash(id, s.Ratio)
		if keep {
			toKeep = append(toKeep, h.spans...)
		} else {
			toDrop = append(toDrop, h.spans...)
		}
		s.decided[id] = decision{keep: keep, expires: now.Add(max(s.MaxAge, 10*time.Minute))}
		delete(s.held, id)
	}
	for id, d := range s.decided {
		if now.After(d.expires) {
			delete(s.decided, id)
		}
	}
	s.mu.Unlock()
	s.route(toKeep, toDrop)
}

// Flush keeps every held trace, for shutdown. Nothing is lost by keeping.
func (s *Sampler) Flush() {
	s.mu.Lock()
	var all []model.Span
	for id, h := range s.held {
		all = append(all, h.spans...)
		delete(s.held, id)
	}
	s.mu.Unlock()
	s.route(all, nil)
}

func (s *Sampler) route(keep, drop []model.Span) {
	if len(keep) > 0 {
		s.kept.Add(uint64(len(keep)))
		s.keep(keep)
	}
	if len(drop) > 0 {
		s.dropped.Add(uint64(len(drop)))
		s.drop(drop)
	}
}

// Held reports how many traces are waiting for a decision.
func (s *Sampler) Held() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.held)
}

// keepByHash is deterministic per trace, so every collector makes the same call.
func keepByHash(traceID string, ratio float64) bool {
	if ratio <= 0 {
		return false
	}
	h := fnv.New64a()
	h.Write([]byte(traceID))
	return float64(h.Sum64())/math.MaxUint64 < ratio
}

// ---------------------------------------------------------------------------
// batcher: ClickHouse wants big inserts, not many small ones.

// Batcher collects spans and inserts them when it has Size of them or Interval
// has passed, whichever comes first, on one of its inserters: ClickHouse spends
// a couple of cores on each insert, so one at a time leaves the rest idle. A
// failed insert is retried with backoff; the spans stay safe in the WAL
// meanwhile. Batches can finish out of order: the tracker releases a WAL record
// only once all of its spans are flushed.
type Batcher struct {
	in        chan []model.Span
	w         Writer
	size      int
	interval  time.Duration
	inserters int
	flushed   func([]model.Span)
	log       *slog.Logger

	drainTimeout time.Duration // the final flush on shutdown gives up after this

	batches, spans, failures atomic.Uint64
	latMu                    sync.Mutex
	latencies                []time.Duration // recent insert latencies, ring
	latPos                   int
}

const latencyWindow = 1024

func newBatcher(w Writer, size int, interval time.Duration, queue, inserters int, flushed func([]model.Span), log *slog.Logger) *Batcher {
	return &Batcher{
		in: make(chan []model.Span, queue), w: w, size: size, interval: interval, inserters: max(1, inserters),
		flushed: flushed, log: log, drainTimeout: 10 * time.Second,
	}
}

// Submit queues spans, blocking while the queue is full (backpressure).
func (b *Batcher) Submit(spans []model.Span) { b.in <- spans }

// Saturated reports a full queue; the server refuses new requests while it is.
func (b *Batcher) Saturated() bool { return len(b.in) >= cap(b.in) }

// Run flushes until ctx is done, then drains what is queued and flushes it once
// more, giving up after drainTimeout. Anything not flushed stays in the WAL.
func (b *Batcher) Run(ctx context.Context) {
	// Handing a batch over blocks while every inserter is busy, so the queue
	// fills and the server pushes back, as with a single inserter.
	jobs := make(chan []model.Span)
	var wg sync.WaitGroup
	for range b.inserters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for buf := range jobs {
				b.flush(ctx, buf)
			}
		}()
	}
	defer wg.Wait()
	defer close(jobs)
	handOff := func(buf []model.Span) []model.Span {
		select {
		case jobs <- buf:
		case <-ctx.Done():
			return buf // drained below
		}
		return make([]model.Span, 0, b.size)
	}

	buf := make([]model.Span, 0, b.size)
	t := time.NewTicker(b.interval)
	defer t.Stop()
	for {
		select {
		case spans := <-b.in:
			buf = append(buf, spans...)
			if len(buf) >= b.size {
				buf = handOff(buf)
			}
		case <-t.C:
			if len(buf) > 0 {
				buf = handOff(buf)
			}
		case <-ctx.Done():
		drain:
			for {
				select {
				case spans := <-b.in:
					buf = append(buf, spans...)
				default:
					break drain
				}
			}
			if len(buf) > 0 {
				dctx, cancel := context.WithTimeout(context.Background(), b.drainTimeout)
				b.flush(dctx, buf)
				cancel()
			}
			return
		}
	}
}

func (b *Batcher) flush(ctx context.Context, buf []model.Span) {
	backoff := 100 * time.Millisecond
	for {
		start := time.Now()
		err := b.w.Insert(ctx, buf)
		if err == nil {
			b.recordLatency(time.Since(start))
			b.batches.Add(1)
			b.spans.Add(uint64(len(buf)))
			b.flushed(buf)
			return
		}
		b.failures.Add(1)
		b.log.Warn("insert failed; retrying", "spans", len(buf), "err", err, "backoff", backoff)
		select {
		case <-time.After(backoff):
			backoff = min(backoff*2, 10*time.Second)
		case <-ctx.Done():
			// Shutting down: leave the spans to the WAL, replayed on next start.
			return
		}
	}
}

func (b *Batcher) recordLatency(d time.Duration) {
	b.latMu.Lock()
	defer b.latMu.Unlock()
	if len(b.latencies) < latencyWindow {
		b.latencies = append(b.latencies, d)
		return
	}
	b.latencies[b.latPos] = d
	b.latPos = (b.latPos + 1) % latencyWindow
}

// LatencyQuantiles returns p50 and p99 over recent inserts.
func (b *Batcher) LatencyQuantiles() (p50, p99 time.Duration) {
	b.latMu.Lock()
	l := slices.Clone(b.latencies)
	b.latMu.Unlock()
	if len(l) == 0 {
		return 0, 0
	}
	slices.Sort(l)
	q := func(p float64) time.Duration { return l[min(len(l)-1, int(p*float64(len(l))))] }
	return q(0.50), q(0.99)
}
