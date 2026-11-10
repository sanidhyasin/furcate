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
- Linux testing uses tine's AWS dev server, as tine does: from ../tine,
  `./scripts/dev-server.sh up`, `make deploy` (installs tined from the checked-out
  branch; tine's `furcate-additions` branch has the retained-snapshot, snapshot-fork
  and offline-VM API), `./scripts/dev-server.sh ssh-config > .tine-ssh-config`, then
  from here `FURCATE_TEST_TINE=ssh://tine-dev FURCATE_TINE_SSH="ssh -F ../tine/.tine-ssh-config"`.
  Before `./scripts/dev-server.sh stop`: delete leftover retained snapshots (they hold
  disk) and remove any systemd override you added. The user wants no idle AWS
  charges. c5d.metal costs $4.75/h: batch work before starting it.
- Bare metal (done once, 2026-11-10, ~31 min, ~$2.50): from ../tine, `TINE_NAME=tine-metal
  TINE_INSTANCE_TYPE=c5d.metal TINE_LOCAL_DATA=1 TINE_ZONE=ap-south-1b
  ./scripts/dev-server.sh up` (1a had no capacity), then `setup`, `make deploy`,
  `make base-image` with the same env. Run Furcate's benchmarks on the host over
  unix:///run/tine/tine.sock (cross-compile with GOOS=linux GOARCH=amd64). The
  host's Docker has no bridge network: run ClickHouse with --network host and a
  config.d file that sets listen_host to 127.0.0.1. Terminate with
  `echo tine-metal | ./scripts/dev-server.sh down`; a .metal can sit in
  shutting-down for over 10 minutes (billing stops at shutting-down).
- Claude API access: use only the user's Anthropic key, the one tine uses. It is
  `ANTHROPIC_API_KEY` here (loaded from the keychain item `anthropic-api-key`) and
  the same key as `~/.config/tine/anthropic-api-key`. A 401 seen earlier on
  2026-11-10 didn't recur. Never print it, and ask before storing it anywhere else
  (e.g. as a GitHub secret). Put a `-max-usd` cap on experiments and report spend.
- Docker Desktop's 58 GB disk is ~98% full, mostly the user's build cache and
  other projects' images: never prune those. Postgres "No space left on device"
  means this. Free space with `furcate gc -driver docker <run-id>...` for runs that
  won't be forked again. An original run's Docker checkpoint copies everything
  changed since the template (~23 MB for csv-report's pip install), so install
  dependencies in the template, as examples/claude-code/Dockerfile does.
- Goal: a portfolio project for agent-infrastructure roles. Published numbers
  matter as much as features. Every number needs its hardware, data size and method.

## Conventions

- Go 1.26, one binary in cmd/furcate, packages under internal/.
- `go test -race ./...` must pass. ClickHouse runs via `docker compose up -d --wait`.
  On macOS, a -race test that execs many processes at once can hang: a forked
  child spins at 100% CPU in `__tsan::TraceSwitchPartImpl` before exec (a race
  detector bug, seen 2026-11-10 with Go 1.26.3) and holds the parent's pipes.
  `sample <pid>` on the busy child shows it; kill the test processes by PID and
  rerun. Not a Furcate bug.
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
  with `furcate collector` running for the full chain, `FURCATE_TEST_DOCKER=1` for
  the Docker driver and drop-in, `FURCATE_TEST_TINE=ssh://tine-dev` for the tine gate,
  `FURCATE_CASESTUDY=docker|tine` for the side-by-side bisect.
- Drop-in proxies listen on 127.0.0.1 and sandboxes reach them as
  host.docker.internal (Docker Desktop). Never bind 0.0.0.0.

## Known gaps

- Crash recovery is at-least-once: about 1 s of flushed records replay as
  duplicate rows. Trace reads dedupe by span_id; rollups don't.
- The trace_index barely helps: at 30M spans a trace loads in 28 ms with it, 39 ms
  without (p50, c5d.metal).
- `runs` lists a run only once its root span arrives, so in-progress runs are hidden.
- `testdir` is test-only (no isolation) and must stay out of the CLI.
- Checkpoints expire only through `furcate gc`; nothing runs it on a schedule.
- The fork scheduler is per process; two CLI processes don't share slots.
- Drop-in is level 2 only (a fork resumes the agent from its on-disk transcript);
  forks keep the network to reach the proxy.
- Eval export reads the task's starting files from disk (`task.files`); if they
  changed since the run, the bundle's setup sees the new ones.
- The Phase 4 PR comment hasn't run on GitHub: the repo has no ANTHROPIC_API_KEY
  secret yet, and adding one is the user's call.
