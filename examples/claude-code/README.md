# Claude Code, unmodified, under Furcate

The csv-report task again, but the agent is Claude Code itself, run inside the
sandbox. Furcate doesn't drive it: a proxy on the host records each request
Claude Code makes, checkpoints the sandbox while Claude Code waits for the
answer, and forwards the request with the host's API key. The sandbox only has a
placeholder key.

```bash
docker build -t furcate-example/claude-code:2.1.322 examples/claude-code
./bin/furcate dropin run -task examples/claude-code/task.json -driver docker
./bin/furcate steps <run-id>
./bin/furcate dropin fork -step 2 -n 3 -append-system "Also make load() skip amounts that parse as nan or inf. Don't change the tests." <run-id>
./bin/furcate dropin fork -step 3 -n 3 -model claude-sonnet-5-5 <run-id>
```

The template (`Dockerfile`) is Python with pytest plus Claude Code's native
binary at a pinned version, checked against the npm registry's integrity hash.
Installing them in the task's setup would work too, but `docker commit` copies a
container's whole writable layer, so every checkpoint of the original run would
carry them again: about 400 MB a step.

Claude Code refuses `--dangerously-skip-permissions` as root unless
`IS_SANDBOX=1` is set; the task sets it, since the agent does run in a sandbox.

A fork restores the files of step k, including Claude Code's own transcript
under `~/.claude`, and runs `claude --continue` with the instruction. Claude
Code rebuilds its conversation from that transcript, so a branch's first request
is Claude Code's, not a byte-for-byte replay of the recorded one: that is what
level 2 drop-in can do. Resuming the very process that was waiting needs a
level-3 driver and an in-sandbox shim.

Drop-in forks keep the network, because they must reach the proxy. The proxy
needs a Claude API key (`ANTHROPIC_API_KEY`) on the host; Claude Code's
subscription login isn't supported.

## A real run (2026-11-10, Docker on an Apple M5)

Claude Code 2.1.322 (its default model, Claude Opus 5.5) made 4 requests, fixed
`report.py` and passed the check: $0.11, 14.5 s. Forks of that run:

| Fork from | Change | Passed |
| --- | --- | --- |
| request 2 | append "Also make load() skip amounts that parse as nan or inf. Don't change the tests." | 3/3 |
| request 2 | append "Before you finish, also add a test for a row whose amount is 'nan'." | 0/3: each added the test as asked, which the check forbids |
| request 3 | model `claude-sonnet-5-5` | 3/3, every request answered by Sonnet 5.5 |
