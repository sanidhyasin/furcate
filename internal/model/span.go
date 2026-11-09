// Package model holds the span row that flows from the collector into the trace store.
package model

import "time"

// Span kinds. An agent step is an "llm" or "tool" span; "agent" is the run's root.
const (
	KindLLM   = "llm"
	KindTool  = "tool"
	KindAgent = "agent"
	KindOther = "other"
)

// Span statuses, lowered from the OTLP status code.
const (
	StatusUnset = "unset"
	StatusOK    = "ok"
	StatusError = "error"
)

// Payload is one prompt, completion or tool input/output, keyed by the SHA-256
// of its bytes.
type Payload struct {
	Hash string
	Data []byte
}

// Span is one row in the spans table. Payloads (prompts, completions, tool input
// and output) never live here: the row keeps only their SHA-256 hashes, and the
// bytes go to the payloads table, which dashboards never read.
type Span struct {
	TraceID       string
	SpanID        string
	ParentSpanID  string
	RunID         string
	Service       string
	Kind          string
	Name          string
	Model         string
	Start         time.Time
	End           time.Time
	TokensIn      uint64
	TokensOut     uint64
	CostUSD       float64
	Status        string
	StatusMessage string
	Payloads      map[string]string // attribute key -> sha256 hex
	Attributes    map[string]string

	// NewPayloads carries the bytes of payloads not yet known to be stored. They
	// are inserted in the same flush as the span, so a span is never stored
	// without its payloads. Not a column.
	NewPayloads []Payload

	// WALSeq is the write-ahead-log record this span came from. It never reaches
	// ClickHouse; the pipeline uses it to know when a WAL segment can be dropped.
	WALSeq uint64
}

// Duration is End - Start, floored at zero for clocks that disagree.
func (s *Span) Duration() time.Duration {
	if d := s.End.Sub(s.Start); d > 0 {
		return d
	}
	return 0
}

// IsRoot reports whether the span has no parent.
func (s *Span) IsRoot() bool { return s.ParentSpanID == "" }
