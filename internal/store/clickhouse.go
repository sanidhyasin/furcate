// Package store is the ClickHouse side of the trace store: schema, batch inserts
// and the read queries behind the CLI.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/sanidhyasin/furcate/internal/model"
)

//go:embed schema.sql
var schemaSQL string

// DefaultDSN matches docker-compose.yml.
const DefaultDSN = "clickhouse://furcate:furcate@localhost:9000/furcate"

// CH wraps a ClickHouse connection.
type CH struct {
	conn driver.Conn
	opts *clickhouse.Options
}

// Open connects using a clickhouse:// DSN.
func Open(ctx context.Context, dsn string) (*CH, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &CH{conn: conn, opts: opts}, nil
}

// Migrate creates the database and tables if they don't exist. It connects to
// the default database first, since the target may not exist yet.
func Migrate(ctx context.Context, dsn string) error {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	db := opts.Auth.Database
	if db == "" {
		db = "default"
	}
	boot := *opts
	boot.Auth.Database = "default"
	conn, err := clickhouse.Open(&boot)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %q", db)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	ch, err := Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer ch.Close()
	for _, stmt := range splitStatements(schemaSQL) {
		if err := ch.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("%w\n-- in statement:\n%s", err, stmt)
		}
	}
	return nil
}

// splitStatements drops comment lines, then splits on ";". Comments go first so a
// semicolon inside one can't split a statement.
func splitStatements(sql string) []string {
	var code []string
	for _, l := range strings.Split(sql, "\n") {
		if t := strings.TrimSpace(l); t != "" && !strings.HasPrefix(t, "--") {
			code = append(code, l)
		}
	}
	var out []string
	for _, stmt := range strings.Split(strings.Join(code, "\n"), ";") {
		if s := strings.TrimSpace(stmt); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// Close closes the connection.
func (c *CH) Close() error { return c.conn.Close() }

const insertSpans = `INSERT INTO spans (trace_id, span_id, parent_span_id, run_id, service, kind, name, model,
	start_time, end_time, duration_ns, tokens_in, tokens_out, cost_usd, status, status_message, payloads, attributes)`

// Insert writes one batch: first the payloads the spans carry, then the spans,
// so a span is never visible without its payloads. A retry after a partial
// failure re-inserts payloads, which the table dedupes. It implements
// collector.Writer.
func (c *CH) Insert(ctx context.Context, spans []model.Span) error {
	inBatch := map[string]bool{}
	var pb driver.Batch
	for i := range spans {
		for _, p := range spans[i].NewPayloads {
			if inBatch[p.Hash] {
				continue
			}
			inBatch[p.Hash] = true
			if pb == nil {
				var err error
				if pb, err = c.conn.PrepareBatch(ctx, `INSERT INTO payloads (hash, size, data)`); err != nil {
					return err
				}
			}
			if err := pb.Append(p.Hash, uint32(len(p.Data)), string(p.Data)); err != nil {
				pb.Abort()
				return err
			}
		}
	}
	if pb != nil {
		if err := pb.Send(); err != nil {
			return fmt.Errorf("insert payloads: %w", err)
		}
	}

	batch, err := c.conn.PrepareBatch(ctx, insertSpans)
	if err != nil {
		return err
	}
	for i := range spans {
		s := &spans[i]
		if err := batch.Append(s.TraceID, s.SpanID, s.ParentSpanID, s.RunID, s.Service, s.Kind, s.Name, s.Model,
			s.Start, s.End, uint64(s.Duration()), s.TokensIn, s.TokensOut, s.CostUSD, s.Status, s.StatusMessage,
			s.Payloads, s.Attributes); err != nil {
			batch.Abort()
			return err
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert spans: %w", err)
	}
	return nil
}

// ErrNoPayload is returned for an unknown payload hash.
var ErrNoPayload = errors.New("payload not found")

// Payload returns the bytes stored under hash.
func (c *CH) Payload(ctx context.Context, hash string) ([]byte, error) {
	var data string
	err := c.conn.QueryRow(ctx, `SELECT data FROM payloads WHERE hash = ? LIMIT 1`, hash).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoPayload
	}
	return []byte(data), err
}

// TableSize is one table's footprint on disk.
type TableSize struct {
	Table        string
	Rows         uint64
	Compressed   uint64
	Uncompressed uint64
}

// Storage reports each table's active parts: rows and bytes before and after
// compression.
func (c *CH) Storage(ctx context.Context) ([]TableSize, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT table, sum(rows), sum(data_compressed_bytes), sum(data_uncompressed_bytes)
		FROM system.parts
		WHERE database = currentDatabase() AND active
		GROUP BY table ORDER BY table`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TableSize
	for rows.Next() {
		var t TableSize
		if err := rows.Scan(&t.Table, &t.Rows, &t.Compressed, &t.Uncompressed); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// PayloadDedupe compares payload references on spans with distinct payloads
// stored: how many times, and how many bytes, deduplication saved.
func (c *CH) PayloadDedupe(ctx context.Context) (refs, unique, refBytes, uniqueBytes uint64, err error) {
	err = c.conn.QueryRow(ctx, `
		WITH sizes AS (SELECT hash, any(size) AS size FROM payloads GROUP BY hash)
		SELECT
			(SELECT sum(length(payloads)) FROM spans),
			(SELECT count() FROM sizes),
			(SELECT sum(s.size) FROM (SELECT arrayJoin(mapValues(payloads)) AS hash FROM spans) AS r
			   INNER JOIN sizes AS s USING hash),
			(SELECT sum(size) FROM sizes)`).Scan(&refs, &unique, &refBytes, &uniqueBytes)
	return
}

// ErrNoTrace is returned when a trace id is unknown.
var ErrNoTrace = errors.New("trace not found")

const spanCols = `trace_id, span_id, parent_span_id, run_id, service, kind, name, model,
	start_time, end_time, tokens_in, tokens_out, cost_usd, status, status_message, payloads, attributes`

// Trace loads every span of one trace. With useIndex it first reads the trace's
// time range from trace_index and scans only that slice; without, it scans the
// whole table (kept for the with/without benchmark). Duplicates from an
// at-least-once replay are collapsed by span_id.
func (c *CH) Trace(ctx context.Context, traceID string, useIndex bool) ([]model.Span, error) {
	query := `SELECT ` + spanCols + ` FROM spans WHERE trace_id = ? LIMIT 1 BY span_id`
	args := []any{traceID}
	if useIndex {
		var first, last time.Time
		var n uint64
		row := c.conn.QueryRow(ctx,
			`SELECT min(first_start), max(last_end), count() FROM trace_index WHERE trace_id = ?`, traceID)
		if err := row.Scan(&first, &last, &n); err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, ErrNoTrace
		}
		query = `SELECT ` + spanCols + ` FROM spans
			WHERE start_time >= fromUnixTimestamp64Nano(?) AND start_time <= fromUnixTimestamp64Nano(?)
			  AND trace_id = ? LIMIT 1 BY span_id`
		// Bound as integers: the driver formats a time.Time to whole seconds.
		args = []any{first.UnixNano(), last.UnixNano(), traceID}
	}
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Span
	for rows.Next() {
		var s model.Span
		if err := rows.Scan(&s.TraceID, &s.SpanID, &s.ParentSpanID, &s.RunID, &s.Service, &s.Kind, &s.Name, &s.Model,
			&s.Start, &s.End, &s.TokensIn, &s.TokensOut, &s.CostUSD, &s.Status, &s.StatusMessage,
			&s.Payloads, &s.Attributes); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, ErrNoTrace
	}
	return out, nil
}

// RunSummary is one row of `furcate runs`.
type RunSummary struct {
	TraceID  string
	RunID    string
	Name     string
	Service  string
	Start    time.Time
	Duration time.Duration
	Status   string
	Spans    uint64
	Errors   uint64
	Tokens   uint64
	CostUSD  float64
}

// Runs lists the most recent finished runs (traces whose root span has arrived).
func (c *CH) Runs(ctx context.Context, since time.Duration, limit int) ([]RunSummary, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT trace_id, run_id, name, service, start_time, duration_ns, status
		FROM spans
		WHERE parent_span_id = '' AND start_time >= now64(9) - toIntervalSecond(?)
		ORDER BY start_time DESC
		LIMIT 1 BY trace_id
		LIMIT ?`, int64(since.Seconds()), limit)
	if err != nil {
		return nil, err
	}
	var out []RunSummary
	byTrace := map[string]int{}
	var ids []string
	minStart := time.Now()
	for rows.Next() {
		var r RunSummary
		var dur uint64
		if err := rows.Scan(&r.TraceID, &r.RunID, &r.Name, &r.Service, &r.Start, &dur, &r.Status); err != nil {
			rows.Close()
			return nil, err
		}
		r.Duration = time.Duration(dur)
		byTrace[r.TraceID] = len(out)
		ids = append(ids, r.TraceID)
		if r.Start.Before(minStart) {
			minStart = r.Start
		}
		out = append(out, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(out) == 0 {
		return out, err
	}

	agg, err := c.conn.Query(ctx, `
		SELECT trace_id, uniqExact(span_id), uniqExactIf(span_id, status = 'error'),
		       sum(tokens_in + tokens_out), sum(cost_usd)
		FROM (SELECT * FROM spans WHERE start_time >= fromUnixTimestamp64Nano(?) AND has(?, trace_id)
		      LIMIT 1 BY span_id)
		GROUP BY trace_id`, minStart.UnixNano(), ids)
	if err != nil {
		return nil, err
	}
	defer agg.Close()
	for agg.Next() {
		var id string
		var spans, errs, tokens uint64
		var cost float64
		if err := agg.Scan(&id, &spans, &errs, &tokens, &cost); err != nil {
			return nil, err
		}
		if i, ok := byTrace[id]; ok {
			out[i].Spans, out[i].Errors, out[i].Tokens, out[i].CostUSD = spans, errs, tokens, cost
		}
	}
	return out, agg.Err()
}

// ModelStats is one row of `furcate stats`.
type ModelStats struct {
	Model     string
	Spans     uint64
	Errors    uint64
	P50, P95  time.Duration
	P99       time.Duration
	TokensIn  uint64
	TokensOut uint64
	CostUSD   float64
}

// Stats summarizes LLM calls by model over the last `since`. fromRollups reads
// the per-minute rollups; otherwise it scans raw spans (for the comparison).
func (c *CH) Stats(ctx context.Context, since time.Duration, fromRollups bool) ([]ModelStats, error) {
	q := `
		SELECT model, sum(spans), sum(errors), quantilesTDigestMerge(0.5, 0.95, 0.99)(latency),
		       sum(tokens_in), sum(tokens_out), sum(cost_usd)
		FROM rollup_minute
		WHERE kind = 'llm' AND minute >= toStartOfMinute(now() - toIntervalSecond(?))
		GROUP BY model ORDER BY sum(spans) DESC`
	if !fromRollups {
		q = `
		SELECT model, count(), countIf(status = 'error'), quantilesTDigest(0.5, 0.95, 0.99)(duration_ns),
		       sum(tokens_in), sum(tokens_out), sum(cost_usd)
		FROM spans
		WHERE kind = 'llm' AND start_time >= toStartOfMinute(now() - toIntervalSecond(?))
		GROUP BY model ORDER BY count() DESC`
	}
	rows, err := c.conn.Query(ctx, q, int64(since.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ModelStats
	for rows.Next() {
		var m ModelStats
		var qs []float64
		if err := rows.Scan(&m.Model, &m.Spans, &m.Errors, &qs, &m.TokensIn, &m.TokensOut, &m.CostUSD); err != nil {
			return nil, err
		}
		if len(qs) == 3 {
			m.P50, m.P95, m.P99 = time.Duration(qs[0]), time.Duration(qs[1]), time.Duration(qs[2])
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// Count returns the number of span rows, for load-test bookkeeping.
func (c *CH) Count(ctx context.Context) (uint64, error) {
	var n uint64
	err := c.conn.QueryRow(ctx, `SELECT count() FROM spans`).Scan(&n)
	return n, err
}

// LLMRequest returns the request body recorded on one LLM span of a run, for the
// replay engine. A run's ID is its trace ID.
func (c *CH) LLMRequest(ctx context.Context, runID, spanID string) ([]byte, error) {
	spans, err := c.Trace(ctx, runID, true)
	if err != nil {
		return nil, err
	}
	for _, s := range spans {
		if s.SpanID != spanID {
			continue
		}
		hash, ok := s.Payloads["furcate.llm.request"]
		if !ok {
			return nil, fmt.Errorf("span %s has no recorded request", spanID)
		}
		return c.Payload(ctx, hash)
	}
	return nil, fmt.Errorf("span %s not in trace %s yet", spanID, runID)
}
