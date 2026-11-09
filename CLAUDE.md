# Furcate

Agent observability that can rewind: trace every agent step, checkpoint it with a
sandbox driver, then fork, bisect and build evals from any step. Working name was
"Agent Rewind"; renamed Furcate on 2026-11-09 (the name pairs with tine).

- Spec: the design doc at https://claude.ai/artifact/QsPmNLTXC9aALQR5ZT1hTK (a
  Claude Docs doc; read it with the docs tools, not a web fetch). As of 2026-11-09
  it still had ~32 unaccepted suggestions that change Phase 2 decisions (driver
  interface shape, checkpoint strategy, fork networking). Confirm with the user
  which version holds before building Phase 2.
- tine, the first sandbox driver, is the user's own Firecracker VM-fork project in
  ../tine. It needs Linux and KVM, so Phase 2+ runs on a Linux host, not this Mac.
- Goal: a portfolio project for agent-infrastructure roles. Published numbers
  matter as much as features. Every number needs its hardware, data size and method.

## Conventions

- Go 1.26, one binary in cmd/furcate, packages under internal/.
- `go test -race ./...` must pass. ClickHouse runs via `docker compose up -d --wait`.
- The loadgen and collector are the same binary: never `pkill -x furcate` during a
  load test, or the load generator dies too. Kill by PID.
- Bound times to ClickHouse as `UnixNano()` with `fromUnixTimestamp64Nano(?)`: the
  driver formats a bound `time.Time` to whole seconds. Bound slices become array
  literals, so use `has(?, col)` rather than `IN ?`.
- schema.sql is split on `;` after comment lines are dropped, so never put a `;`
  inside a string literal there.

## Known gaps (Phase 1)

- Crash recovery is at-least-once: about 1 s of flushed records replay as
  duplicate rows. Trace reads dedupe by span_id; rollups don't.
- The trace_index only beats a full scan on large tables; not yet measured at scale.
- `runs` lists a run only once its root span arrives, so in-progress runs are hidden.
