# syntax=docker/dockerfile:1
# The sandbox template for the Claude Code drop-in example: Python with pytest for
# the csv-report repo, and Claude Code's native binary at a pinned version.
#
#   docker build -t furcate-example/claude-code:2.1.322 examples/claude-code
#
# Installing these in the task's setup instead would put them in every checkpoint
# of the original run (docker commit copies the container's whole writable
# layer): about 400 MB per step.
FROM python:3.13-slim

ARG CLAUDE_CODE_VERSION=2.1.322
ARG TARGETARCH

RUN pip install --no-cache-dir --quiet --disable-pip-version-check pytest==8.4.2

# Claude Code's npm package for this platform holds one file that matters, the
# native binary, so no Node.js. Fetch it, check it against the registry's
# integrity hash as npm would, and keep only the binary.
RUN python3 - <<'EOF'
import base64, hashlib, json, os, shutil, tarfile, urllib.request

arch = {"amd64": "x64", "arm64": "arm64"}[os.environ["TARGETARCH"]]
name = f"@anthropic-ai/claude-code-linux-{arch}"
meta = json.load(urllib.request.urlopen(f"https://registry.npmjs.org/{name}/{os.environ['CLAUDE_CODE_VERSION']}"))
algo, want = meta["dist"]["integrity"].split("-", 1)
path, _ = urllib.request.urlretrieve(meta["dist"]["tarball"])
h = hashlib.new(algo)
with open(path, "rb") as f:
    for chunk in iter(lambda: f.read(1 << 20), b""):
        h.update(chunk)
if base64.b64encode(h.digest()).decode() != want:
    raise SystemExit(f"{name}: integrity mismatch")
with tarfile.open(path) as t, open("/usr/local/bin/claude", "wb") as out:
    shutil.copyfileobj(t.extractfile("package/claude"), out)
os.chmod("/usr/local/bin/claude", 0o755)
os.remove(path)
EOF
