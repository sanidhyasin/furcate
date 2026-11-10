package runstore

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// DefaultDSN matches docker-compose.yml.
const DefaultDSN = "postgres://furcate:furcate@localhost:5432/furcate?sslmode=disable"

// Postgres is the Store used outside tests.
type Postgres struct{ pool *pgxpool.Pool }

// OpenPostgres connects and creates the tables if they don't exist.
func OpenPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("create tables: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

// Close closes the pool.
func (p *Postgres) Close() { p.pool.Close() }

func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func (p *Postgres) CreateRun(ctx context.Context, r Run) error {
	if r.Status == "" {
		r.Status = StatusRunning
	}
	var parentStep *int
	if r.ParentRunID != "" {
		parentStep = &r.ParentStep
	}
	var change any
	if len(r.Change) > 0 {
		change = r.Change
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO runs (run_id, task_id, task, driver, parent_run_id, parent_step, change, status,
			eval_case, eval_variant, eval_batch)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		r.RunID, r.TaskID, r.Task, r.Driver, nullStr(r.ParentRunID), parentStep, change, r.Status,
		nullStr(r.Eval.Case), nullStr(r.Eval.Variant), nullStr(r.Eval.Batch))
	return err
}

func (p *Postgres) FinishRun(ctx context.Context, runID string, end End) error {
	var files any
	if end.FinalFiles != nil {
		files = end.FinalFiles
	}
	u := end.Usage
	tag, err := p.pool.Exec(ctx, `
		UPDATE runs SET status = $2, outcome = $3, error = $4, finished_at = now(),
			llm_calls = $5, input_tokens = $6, output_tokens = $7, cache_read_tokens = $8,
			cache_write_tokens = $9, cost_usd = $10, final_files = $11
		WHERE run_id = $1`,
		runID, end.Status, nullStr(end.Outcome), nullStr(end.Error),
		u.LLMCalls, u.InputTokens, u.OutputTokens, u.CacheReadTokens, u.CacheWriteTokens, u.CostUSD, files)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (p *Postgres) AddCheckpoint(ctx context.Context, c Checkpoint) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO checkpoints (checkpoint_id, run_id, step, driver, level, driver_ref, size_bytes, pinned, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		c.CheckpointID, c.RunID, c.Step, c.Driver, c.Level, c.DriverRef, c.SizeBytes, c.Pinned, nullTime(c.ExpiresAt))
	return err
}

func (p *Postgres) AddStep(ctx context.Context, s Step) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO steps (run_id, step_index, span_id, kind, checkpoint_id) VALUES ($1, $2, $3, $4, $5)`,
		s.RunID, s.Index, s.SpanID, s.Kind, nullStr(s.CheckpointID))
	return err
}

const runCols = `run_id, task_id, task, driver, coalesce(parent_run_id, ''), coalesce(parent_step, 0), change,
	status, coalesce(outcome, ''), coalesce(error, ''), started_at, finished_at,
	llm_calls, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, cost_usd, final_files,
	coalesce(eval_case, ''), coalesce(eval_variant, ''), coalesce(eval_batch, '')`

func scanRun(row pgx.Row) (Run, error) {
	var r Run
	var finished *time.Time
	u := &r.Usage
	err := row.Scan(&r.RunID, &r.TaskID, &r.Task, &r.Driver, &r.ParentRunID, &r.ParentStep, &r.Change,
		&r.Status, &r.Outcome, &r.Error, &r.StartedAt, &finished,
		&u.LLMCalls, &u.InputTokens, &u.OutputTokens, &u.CacheReadTokens, &u.CacheWriteTokens, &u.CostUSD, &r.FinalFiles,
		&r.Eval.Case, &r.Eval.Variant, &r.Eval.Batch)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	if finished != nil {
		r.FinishedAt = *finished
	}
	return r, err
}

func (p *Postgres) Run(ctx context.Context, runID string) (Run, error) {
	return scanRun(p.pool.QueryRow(ctx, `SELECT `+runCols+` FROM runs WHERE run_id = $1`, runID))
}

func (p *Postgres) Step(ctx context.Context, runID string, index int) (Step, error) {
	s := Step{RunID: runID, Index: index}
	err := p.pool.QueryRow(ctx, `
		SELECT span_id, kind, coalesce(checkpoint_id, '') FROM steps WHERE run_id = $1 AND step_index = $2`,
		runID, index).Scan(&s.SpanID, &s.Kind, &s.CheckpointID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Step{}, ErrNotFound
	}
	return s, err
}

func (p *Postgres) Steps(ctx context.Context, runID string) ([]Step, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT step_index, span_id, kind, coalesce(checkpoint_id, '') FROM steps
		WHERE run_id = $1 ORDER BY step_index`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Step
	for rows.Next() {
		s := Step{RunID: runID}
		if err := rows.Scan(&s.Index, &s.SpanID, &s.Kind, &s.CheckpointID); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (p *Postgres) Checkpoint(ctx context.Context, id string) (Checkpoint, error) {
	c := Checkpoint{CheckpointID: id}
	var size *int64
	var expires *time.Time
	err := p.pool.QueryRow(ctx, `
		SELECT run_id, step, driver, level, driver_ref, size_bytes, pinned, expires_at, created_at, deleted_at IS NOT NULL
		FROM checkpoints WHERE checkpoint_id = $1`, id).
		Scan(&c.RunID, &c.Step, &c.Driver, &c.Level, &c.DriverRef, &size, &c.Pinned, &expires, &c.CreatedAt, &c.Deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return Checkpoint{}, ErrNotFound
	}
	if size != nil {
		c.SizeBytes = *size
	}
	if expires != nil {
		c.ExpiresAt = *expires
	}
	return c, err
}

func (p *Postgres) Children(ctx context.Context, runID string) ([]Run, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+runCols+` FROM runs WHERE parent_run_id = $1 ORDER BY started_at`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (p *Postgres) PinCheckpoint(ctx context.Context, id string) error {
	tag, err := p.pool.Exec(ctx, `UPDATE checkpoints SET pinned = true, expires_at = NULL WHERE checkpoint_id = $1`, id)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (p *Postgres) AddEvalCase(ctx context.Context, c EvalCase) error {
	var check, change any
	if c.Check != nil {
		check = c.Check
	}
	if len(c.Change) > 0 {
		change = c.Change
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO eval_cases (case_id, run_id, step, checkpoint_id, check_cmd, change, notes)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, c.CaseID, c.RunID, c.Step, c.CheckpointID, check, change, c.Notes)
	return err
}

const caseCols = `case_id, run_id, step, checkpoint_id, check_cmd, change, notes, created_at`

func scanCase(row pgx.Row) (EvalCase, error) {
	var c EvalCase
	err := row.Scan(&c.CaseID, &c.RunID, &c.Step, &c.CheckpointID, &c.Check, &c.Change, &c.Notes, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return EvalCase{}, ErrNotFound
	}
	return c, err
}

func (p *Postgres) EvalCase(ctx context.Context, id string) (EvalCase, error) {
	return scanCase(p.pool.QueryRow(ctx, `SELECT `+caseCols+` FROM eval_cases WHERE case_id = $1`, id))
}

func (p *Postgres) EvalCases(ctx context.Context) ([]EvalCase, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+caseCols+` FROM eval_cases ORDER BY case_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EvalCase
	for rows.Next() {
		c, err := scanCase(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (p *Postgres) Expirable(ctx context.Context, cutoff time.Time) ([]Checkpoint, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT checkpoint_id, run_id, step, driver, level, driver_ref, created_at FROM checkpoints
		WHERE NOT pinned AND deleted_at IS NULL AND created_at < $1 ORDER BY created_at`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Checkpoint
	for rows.Next() {
		var c Checkpoint
		if err := rows.Scan(&c.CheckpointID, &c.RunID, &c.Step, &c.Driver, &c.Level, &c.DriverRef, &c.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (p *Postgres) MarkDeleted(ctx context.Context, id string) error {
	tag, err := p.pool.Exec(ctx, `UPDATE checkpoints SET deleted_at = now() WHERE checkpoint_id = $1`, id)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}
