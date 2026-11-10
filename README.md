# Furcate

Agent traces you can fork from any step. Every step of an agent run is recorded
as a span and checkpointed by a sandbox driver, so a failed run can be reopened
at step k and run again from that exact state (same files, processes and
conversation) with a different prompt, model or tool output. On top of that:
**agent bisect** finds the step where a run went wrong, and **eval cases** turn
checkpoints of real runs into tests that run in CI.

Design doc: <https://claude.ai/artifact/QsPmNLTXC9aALQR5ZT1hTK>

## Status

All five phases are built and tested. The gates that need only sandboxes are met;
the ones that need a real model are waiting on Claude API credentials (the key in
the build environment is rejected).

| Phase | Built | Gate | State |
| --- | --- | --- | --- |
| 1 | OTLP collector with a write-ahead log, ClickHouse trace store, payload table, CLI, load generator | ingest and query numbers published | laptop numbers below; c5d.metal pending |
| 2 | driver interface; tine and Docker drivers; run controller; `furcate fork` | a fork of step k matches its files and conversation; step overhead measured | **met on tine** |
| 3 | bisect, branch compare, fork scheduler, checkpoint gc | bisect finds the faulty step of a real failure on tine and Docker | met with a scripted agent; real-model failure pending |
| 4 | eval cases, portable bundles, `furcate eval run`, GitHub Action | a prompt change in a real repo shows its pass-rate change on the PR | built; needs a key and the repo secret |
| 5 | drop-in mode for unmodified agents, web UI | an unmodified Claude Code run forked from any LLM call | drop-in tested with a transcript-keeping agent; Claude Code run pending |

## Quick start

```bash
docker compose up -d --wait                  # ClickHouse 25.8 and Postgres 17
go build -o bin/furcate ./cmd/furcate
./bin/furcate collector                      # OTLP on :4318 (HTTP) and :4317 (gRPC)
```

Then, with `ANTHROPIC_API_KEY` set:

```bash
./bin/furcate run -task examples/csv-report/task.json -driver docker
./bin/furcate steps <run-id>                 # steps, spans and checkpoints
./bin/furcate fork -step 5 -n 10 -append-system "Never edit test fixtures." <run-id>
./bin/furcate branches <run-id>              # pass rate per (step, change), with 95% CIs
./bin/furcate bisect -n 10 -max-usd 5 <run-id>
./bin/furcate compare <run-a> <run-b>
./bin/furcate eval add -run <run-id> -step 5 -name fixture-edit-01
./bin/furcate eval export fixture-edit-01
./bin/furcate eval run -variant name=main -variant name=careful,system-file=examples/csv-report/task-careful.json
./bin/furcate dropin run -task examples/claude-code/task.json -driver docker
./bin/furcate gc -driver docker -older-than 24h
```

Any OpenTelemetry SDK can also send plain traces to `http://localhost:4318/v1/traces`
(`runs`, `trace`, `stats` work on them); the rewind features need a sandbox driver.

## How it works

```
                 ┌──────────── host ─────────────┐        ┌─── sandbox ───┐
 Claude API ◄──► │ run controller (agent loop)   │ ─exec─► │ tools run here │
                 │  checkpoint before each call  │ ─ckpt─► │ (tine VM or    │
                 │  span per step ───────────────┼──OTLP──► collector ─► ClickHouse
                 │  row per step/checkpoint ─────┼──────► Postgres        │
                 └───────────────────────────────┘        └───────────────┘
```

- **Collector** (`internal/collector`): OTLP/HTTP and gRPC. A request is
  acknowledged only after it is in the write-ahead log (`internal/wal`); it answers
  503 when its queue is full. Payloads (prompts, completions, tool I/O) go to their
  own table, stored once per SHA-256.
- **Run controller** (`internal/controller`): the agent loop runs on the host, and
  tools (`bash`, `write_file`) run in the sandbox. Before every LLM call it takes a
  checkpoint; the call's full request body goes on its span.
- **Fork** (`internal/replay`): restores step k's checkpoint into N sandboxes
  (offline if the driver can), resends the recorded request with a change, and runs
  the continuations in parallel under a per-process sandbox limit. Changes are
  append-only (a prompt patch is a mid-conversation `system` message), so the
  history the model reasoned over stays byte-identical: the prompt cache stays warm
  and replayed thinking blocks stay valid. Replacing the system prompt is the one
  change that edits history; it strips earlier thinking blocks.
- **Bisect** (`internal/bisect`): binary search over checkpointed steps by success
  rate, a Wilson interval per probe, more samples when a probe is ambiguous, and a
  budget in runs and dollars.
- **Evals** (`internal/evals`): a case is a checkpoint, a check and an optional
  change, with its checkpoint pinned. Exported, it is a portable bundle: the task,
  the recorded request byte for byte, and a tarball of the workdir. Any driver can
  restore it, so CI needs only Docker and a key.
- **Drop-in** (`internal/dropin`): an unmodified agent runs inside the sandbox with
  `ANTHROPIC_BASE_URL` pointed at a recording proxy on the host and a placeholder
  key. Each Messages request is a step: the proxy checkpoints the sandbox while the
  agent waits, records the request, and forwards it with the host's key. A fork
  restores the agent's own transcript and runs its resume command (level 2).

### Drivers

| Driver | Level | Checkpoint | Fork | Notes |
| --- | --- | --- | --- | --- |
| `tine` | 3: memory, processes, files | retained snapshot | N VMs from a snapshot, offline | over tined's socket or `ssh host tine dial-stdio` |
| `docker` | 2: files | `docker commit` | N containers, `--network none` | runs on a laptop; processes restart in forks |
| `testdir` | 2 | directory copy | directory copies | tests only: no isolation |

## Results so far

Hardware and method for each line; nothing here is from the c5d.metal yet.

**Trace store**, Apple M5 (10 cores), ClickHouse 25.8 in Docker Desktop (8 GB),
`furcate loadgen --concurrency 16 --steps 10` for 30 s:

| Metric | Result |
| --- | --- |
| Sustained ingest | 9,631 spans/s (459 runs/s, 103 MiB/s of payloads), 0 lost |
| Insert latency | p50 934 ms, p99 1.64 s per 10,000-span batch |
| Stats by model, 24 h | rollups p50 2.2 ms; raw spans 10.1 ms |
| Storage per span | 2,046 B with payloads; payloads compress 4.7x |
| Crash (kill -9 mid-load) | 56,616 spans acknowledged, 56,763 stored: none lost |

**Checkpoints and forks on tine**, AWS m8i.xlarge (4 vCPU, nested virtualization),
1 GiB VMs, EBS, a 4-step scripted agent (`internal/driver/tine/gate_test.go`):

| Metric | Full snapshots | Diff snapshots |
| --- | --- | --- |
| Checkpoint per step (the wait for the last snapshot to reach disk dominates) | 7.1 s | 1.55 s |
| Fork 1 VM from a retained snapshot, offline | 90 ms | 229 ms |
| Fork 10 VMs | 367 ms | 441 ms |

A fork of step 5 had the checkpoint's files, a counter process started at step 1
still running from its in-memory value, no network ("network unreachable"), and its
first request byte-identical to the recorded one.

**Docker**, Apple M5: checkpoint 46 ms, fork of 3 containers 197 ms.

**Same bisect, both drivers** (`internal/casestudy`): a scripted agent starts a dev
server, writes notes, and at step 5 kills the server one time in five; the check
needs the server alive.

| Driver | Probes (passes/10 per step) | Verdict | Cost |
| --- | --- | --- | --- |
| tine | 1: 8, 3: 8, 5: 7, 7: 0 | step 5 breaks the run | 40 continuations, 27 s |
| docker | 1: 0/20, 7: 0/20 | can't name a step: forks lose the running server | 40 continuations, 5 s |

Docker is five times faster and wrong; tine finds the step. When all the state is
files, both get the same answer and Docker wins on time.

## Decisions taken while building

- **Payloads live in a ClickHouse table.** One file per payload ran at 382 spans/s
  with 87% of CPU in file-system syscalls; the table is 25x faster.
- **Conversations are raw JSON, never re-encoded.** Decoding a recorded request
  into the SDK's typed params reorders earlier tool-input keys, so a branch would
  send history the model never saw.
- **A checkpoint before every LLM call**, and a branch reuses its parent's for its
  first step.
- **Drop-in at level 2 first**, as the doc suggests: restore the agent's transcript
  and resume. Keeping the waiting process needs tine and an in-sandbox shim.
- **Refusal fallbacks on by default** (`-no-fallbacks` to turn off); spans record
  which model answered. Default model `claude-opus-5-5`, effort `high`.
- **Recovery is at-least-once**: a crash replays about a second of flushed records.
  Trace reads collapse duplicates; rollups don't yet.

## Layout

```
cmd/furcate          the one binary: collector, CLI, loadgen, bench, serve
internal/collector   OTLP servers, conversion, tail sampling, batching
internal/wal         write-ahead log
internal/store       ClickHouse schema, inserts, queries
internal/controller  the agent loop: checkpoints, spans and steps for every run
internal/replay      fork: restore step k, apply a change, run N continuations
internal/bisect      find the faulty step
internal/evals       eval cases, bundles, suites, reports
internal/dropin      the recording proxy for unmodified agents
internal/driver      the driver interface; tine, docker and testdir drivers
internal/runstore    runs, steps, checkpoints, eval cases (Postgres; memory for tests)
internal/agent       the raw-JSON conversation, the Claude client, sandbox tools
internal/server      HTTP API and web UI
examples/            csv-report (Furcate's loop) and claude-code (drop-in)
action.yml           GitHub Action: eval cases on a pull request
```

## Development

```bash
go test -race ./...
FURCATE_TEST_DOCKER=1 go test ./internal/driver/docker/ ./internal/dropin/
FURCATE_TEST_PG='postgres://furcate:furcate@localhost:5432/furcate?sslmode=disable' go test ./internal/runstore/
FURCATE_TEST_E2E=1 go test ./internal/replay/        # with `furcate collector` running
FURCATE_TEST_TINE=ssh://tine-dev FURCATE_TINE_SSH="ssh -F ../tine/.tine-ssh-config" go test ./internal/driver/tine/
FURCATE_CASESTUDY=tine ... go test ./internal/casestudy/
```

The tine driver needs tine's `furcate-additions` branch (retained snapshots, forks
of snapshots, offline VMs).
