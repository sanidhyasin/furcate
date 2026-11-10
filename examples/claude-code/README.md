# Claude Code, unmodified, under Furcate

The csv-report task again, but the agent is Claude Code itself, run inside the
sandbox. Furcate doesn't drive it: a proxy on the host records each request
Claude Code makes, checkpoints the sandbox while Claude Code waits for the
answer, and forwards the request with the host's API key. The sandbox only has a
placeholder key.

```bash
./bin/furcate dropin run -task examples/claude-code/task.json -driver docker
./bin/furcate steps <run-id>
./bin/furcate dropin fork -step 4 -n 5 -append-system "Never edit tests or fixtures." <run-id>
```

A fork restores the files of step k, including Claude Code's own transcript
under `~/.claude`, and runs `claude --continue` with the instruction. Claude
Code rebuilds its conversation from that transcript, so a branch's first request
is Claude Code's, not a byte-for-byte replay of the recorded one: that is what
level 2 drop-in can do. Resuming the very process that was waiting needs a
level-3 driver and an in-sandbox shim.

Drop-in forks keep the network, because they must reach the proxy. The proxy
needs a Claude API key (`ANTHROPIC_API_KEY`) on the host; Claude Code's
subscription login isn't supported.
