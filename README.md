# Furcate

Agent traces you can fork from any step. Every step of an agent run is recorded as
a span and checkpointed by a sandbox driver, so a failed run can be reopened at
step k and run again from that exact state: same files, processes and
conversation, with a different prompt, model or tool output. On top of that come
**agent bisect** (find the step where a run went wrong) and **checkpoints as eval
cases**.

Design doc: <https://claude.ai/artifact/QsPmNLTXC9aALQR5ZT1hTK>

## Status

**Phase 2 of 5: checkpoints and fork.** The core is built and tested: the driver
interface, the run controller that checkpoints every step, run state in Postgres,
the replay engine and `furcate fork`. What's missing is a real sandbox driver. The
tine driver needs three operations tine doesn't have yet: starting VMs from a
saved snapshot, snapshots that outlive their VM, and VMs with no network. Until
then, forking is exercised only through a test-only directory driver.

| Phase | What | State |
| --- | --- | --- |
| 1 | Trace store: collector, ClickHouse, payloads, CLI, load generator | built; numbers below are from a laptop |
| 2 | Driver interface, tine driver, checkpoints, `furcate fork` | core built and tested; tine driver waits on tine |
| 3 | Bisect and branch compare, Docker driver | not started |
| 4 | Evals from checkpoints, GitHub Action | not started |
| 5 | Drop-in mode, web UI, more drivers | not started |

## Quick start

```bash
docker compose up -d --wait        # ClickHouse 25.8 on :9000, Postgres 17 on :5432
go build -o bin/furcate ./cmd/furcate
./bin/furcate collector            # OTLP on :4318 (HTTP) and :4317 (gRPC)
```

In another terminal:

```bash
./bin/furcate loadgen --runs 200   # synthetic agent runs
./bin/furcate runs                 # recent runs
./bin/furcate trace <trace-id>     # one run as a tree; -payloads shows prompts
./bin/furcate stats                # LLM calls by model: latency, tokens, cost
./bin/furcate bench                # trace-load and query timings, storage per span
```

Forking (needs a sandbox driver, so not yet runnable outside tests):

```bash
./bin/furcate run -task task.json                 # run an agent; checkpoint before every LLM call
./bin/furcate steps <run-id>                      # steps, spans and checkpoints
./bin/furcate fork -step 5 -n 10 \
    -append-system "Never delete test fixtures." <run-id>
./bin/furcate branches <run-id>                   # pass rate per (step, change), with 95% CIs
```

Any OpenTelemetry SDK can send to `http://localhost:4318/v1/traces`. Spans that
follow the [GenAI semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/)
get their model, tokens and cost pulled into columns. Pass `-prices prices.json` to the
collector to price tokens (see `internal/pricing`); there are no built-in prices.

## How it works

```
OTLP request ─► parse ─► convert ─► WAL append + fsync ─► 200 OK
                            │
                            └─► tail sampler ─► batcher ─► ClickHouse ─► release WAL
```

- **Collector** (`internal/collector`): OTLP/HTTP (protobuf and JSON, gzip) and
  OTLP/gRPC. A request is acknowledged only after it is in the write-ahead log.
  When the insert queue is full it answers 503 with `Retry-After`, so a client
  backs off instead of losing data.
- **WAL** (`internal/wal`): CRC-checked segments with group commit, so concurrent
  requests share one fsync. A segment is deleted once every span from it is in
  ClickHouse. A watermark file lets a restart skip records that were already
  flushed. A torn record at the tail is cut off on replay.
- **Tail sampling**: off by default (`-sample 1`). Below 1, whole runs are kept or
  dropped once they finish, never single spans, because a run with a missing span
  can't be replayed. Runs with an error or a checkpoint are always kept.
- **Batcher**: inserts at 10,000 spans or every second, retrying with backoff.
- **Trace store** (`internal/store/schema.sql`): `spans` sorted by time;
  `trace_index` maps a trace to its time range; `rollup_minute` holds per-minute
  stats by model, filled by materialized views; `payloads` holds prompts,
  completions and tool I/O once per SHA-256. The tree is rebuilt at read time
  (`internal/tracetree`), since children arrive before their parents.

## Numbers so far (laptop, not the benchmark machine)

Apple M5 (10 cores), macOS 26.7, ClickHouse 25.8 in Docker Desktop with 8 GB.
Load from `furcate loadgen --concurrency 16 --steps 10`: 21-span runs, a 6 KB shared
system prompt, a conversation that grows each step, and 5% failing runs. These are
first measurements on a dev machine; the published ones will come from the c5d.metal
used for tine.

| Metric | Result |
| --- | --- |
| Sustained ingest | 9,631 spans/s (459 runs/s, 103 MiB/s of payloads) for 30 s, 0 lost |
| Insert latency | p50 934 ms, p99 1.64 s per 10,000-span batch with payloads |
| Trace load (289k spans) | with index p50 6.5 ms; full scan p50 4.2 ms |
| Stats by model, 24 h | rollups p50 2.2 ms; raw spans p50 10.1 ms |
| Storage per span | 2,046 B on disk with payloads; spans alone 241 B (1.5× compression) |
| Payloads | 4.7× compression; 1.3× dedupe (688,600 references, 550,747 stored) |
| Crash (kill -9 mid-load) | 56,616 spans acknowledged, 56,763 stored: none lost |

What the numbers say so far:

- **ClickHouse in Docker is the ingest bottleneck.** Inserts take about a second
  per batch, and the collector pushed back 657 times in 30 s.
- **The trace index doesn't pay off yet.** At 289k rows a full scan beats it,
  because the index costs an extra round trip. It needs testing at 10–100M rows.
- **Payloads are quadratic.** Each step re-sends the whole conversation. Storing
  messages one by one, by hash, would dedupe the history across steps.
- **Recovery is at-least-once.** A crash replays roughly the last second of
  flushed records: about 10,000 duplicate rows in the test above. Trace reads
  collapse duplicates by `span_id`; rollups overcount until that's fixed.

## How a fork works

1. The **run controller** (`internal/controller`) runs the agent loop on the host.
   Only tools (`bash`, `write_file`) execute in the sandbox. Before every LLM call
   it asks the driver for a checkpoint. Each call's full request body goes on its
   span, and each step gets a row in Postgres.
2. `furcate fork -step k` looks up step k's checkpoint and span, then reads the
   recorded request back from ClickHouse.
3. The driver restores the checkpoint into N sandboxes, offline if it can. The
   change is applied to the request, and N continuations run in parallel. Each
   continuation is a new run and a new trace, linked to its parent and step.
4. The task's check (a command, exit 0 = pass) scores each branch. The result is a
   pass rate with a Wilson 95% interval, not one anecdote.

A change only appends or touches the newest message. A prompt patch becomes a
mid-conversation `system` message rather than an edit to the system prompt; a
tool-output override replaces only the last tool result. The history the model
already reasoned over stays byte-identical. That keeps the prompt cache warm and
passes the API's check that replayed thinking blocks belong to an unedited
conversation.

## Decisions taken while building

- **The conversation is kept as raw JSON, never re-encoded.** The first version
  decoded the recorded request into the SDK's typed params for each branch. A test
  caught that this reorders the keys inside earlier `tool_use` inputs, so a branch
  would send different bytes than the model saw. Messages are now encoded once
  when appended, and a branch resends the recorded body as is.
- **A branch reuses its parent's checkpoint for its first step** instead of taking
  a new one of an identical state.
- **Refusal fallbacks are on by default** (`-no-fallbacks` to turn off). The span
  records which model actually answered.

- **Payloads in a ClickHouse table**, not one file each. The first version wrote a
  zstd file per payload. Profiling showed 87% of collector CPU in file-system
  syscalls, at 382 spans/s. The table version runs 25× faster and compresses across
  payloads. S3-compatible storage can still be added at the writer.
- **The CLI reads ClickHouse directly.** The HTTP API in the design doc comes when
  a second client (the web UI) needs it.
- **Runs are traces.** A run's ID is its trace ID unless the SDK sets
  `furcate.run_id`. The `runs` and `checkpoints` tables in Postgres arrive with
  Phase 2.

## Layout

```
cmd/furcate          one binary: collector, CLI, loadgen, bench
internal/collector   OTLP servers, conversion, sampling, batching
internal/wal         write-ahead log
internal/store       ClickHouse schema, inserts, queries
internal/blob        payload hashing and the stored-hash cache
internal/tracetree   rebuilds and renders a trace tree
internal/loadgen     synthetic agent runs
internal/pricing     tokens to dollars
internal/driver      the sandbox driver interface; testdir is a test-only driver
internal/controller  the agent loop: checkpoints, spans and steps for every run
internal/replay      fork: restore step k, apply a change, run N continuations
internal/runstore    run, step and checkpoint state (Postgres; in-memory for tests)
internal/agent       the raw-JSON conversation, the Claude client, sandbox tools
internal/task        task specs: template, prompt, model, success check
internal/stats       Wilson intervals
```

## Development

```bash
go test -race ./...
# against the compose stores, with `furcate collector` running:
FURCATE_TEST_PG='postgres://furcate:furcate@localhost:5432/furcate?sslmode=disable' \
FURCATE_TEST_E2E=1 go test -race ./...
```

`/debug/stats` on the collector shows live counters; `/debug/pprof` profiles it.
