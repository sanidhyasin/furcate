# Furcate

Agent observability that can rewind: trace every agent step, checkpoint it with a
sandbox driver, then fork, bisect and build evals from any step. Working name was
"Agent Rewind"; renamed Furcate on 2026-11-09 (the name pairs with tine).

- Spec: the design doc at https://claude.ai/artifact/QsPmNLTXC9aALQR5ZT1hTK (a
  Claude Docs doc; read it with the docs tools, not a web fetch). All its
  suggestions were accepted on 2026-11-10, so it reads as one version: 7-call
  driver interface, a diff snapshot per step, forks with no network, the agent
  loop on the host.
- tine, the first sandbox driver, is the user's own Firecracker VM-fork project in
  ../tine. Its API is JSON over HTTP on /run/tine/tine.sock (or `ssh host tine
  dial-stdio`); its Go client is under internal/ so Furcate needs its own. The tine
  driver needs three tine additions first: VMs from a saved snapshot, snapshots
  that outlive their VM (and more than 16 per VM), and offline VMs.
- Linux testing uses tine's AWS dev server, as tine does: `../tine/scripts/dev-server.sh`
  (up / status / ssh / stop; m8i.xlarge with nested virt, ap-south-1, ~$0.23/h,
  auto-stops after 60 idle min). Stop it when done. Don't start c5d.metal
  ($4.75/h) without asking.
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
- Never decode a recorded request into `anthropic.MessageNewParams` and re-encode
  it: that reorders tool-input keys. Conversations are `agent.Conversation` (raw
  JSON messages) and `agent.Model.Send` posts bytes as they are.
- Fork changes must stay append-only or touch only the newest message (preserved
  thinking's history-editing check; prompt caching).
- Integration tests: `FURCATE_TEST_PG=<dsn>` for Postgres, `FURCATE_TEST_E2E=1`
  with `furcate collector` running for the full chain.

## Known gaps

- Crash recovery is at-least-once: about 1 s of flushed records replay as
  duplicate rows. Trace reads dedupe by span_id; rollups don't.
- The trace_index only beats a full scan on large tables; not yet measured at scale.
- `runs` lists a run only once its root span arrives, so in-progress runs are hidden.
- No real sandbox driver yet; the CLI's `run` and `fork` refuse until the tine
  driver exists. `testdir` is test-only (no isolation) and must stay out of the CLI.
- Checkpoints are never expired or deleted yet; branches run with no concurrency
  limit (the fork scheduler is Phase 3).
- `gen_ai.usage.input_tokens` on agent spans includes cache reads, so a price
  table overprices cached input.
