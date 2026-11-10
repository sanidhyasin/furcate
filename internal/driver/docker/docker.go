// Package docker is a sandbox driver that runs each instance as a Docker
// container. It exists so anyone can try Furcate on a laptop without Linux and
// KVM: it works with Docker Desktop on a Mac.
//
// It saves files, not machines (level 2). A checkpoint is `docker commit` of the
// container's filesystem; a fork is a new container started from that image, so
// files come back but running programs start fresh. Forks can run with no
// network at all (`--network none`).
//
// Everything it makes is labelled furcate=1 and furcate.prefix=<Prefix>, and
// named <Prefix>-<hex> (containers) or <Prefix>-cp:<hex> (checkpoint images), so
// Cleanup can find and remove it.
//
// Things to know:
//   - The image needs a POSIX sh and a sleep that takes "infinity" (busybox and
//     coreutils both do). Each container's entrypoint is replaced with
//     `sleep infinity` under docker-init so it stays up between commands.
//   - commit pauses the container while it copies the writable layer, and the
//     layer holds every change since the container started, not since the last
//     checkpoint: checkpoints of one container grow with the run.
//   - A checkpoint taken in a fork is a child image of the fork's checkpoint;
//     each generation adds one image layer.
//   - Paths the image declares as VOLUME are not part of a commit, so they are
//     not checkpointed.
package docker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sanidhyasin/furcate/internal/driver"
)

// Options configures a Driver. The zero value is usable.
type Options struct {
	// Prefix names containers <Prefix>-<hex> and checkpoint images
	// <Prefix>-cp:<hex>, and scopes Cleanup. Lowercase letters, digits and
	// single '.', '_' or '-' between them. Default "furcate".
	Prefix string
	// Workdir is where commands run when ExecRequest.Dir is empty, and what a
	// relative Dir is resolved against. Containers start with it as their
	// working directory (`docker run -w`), which creates it. Empty: the image's
	// own WORKDIR.
	Workdir string
	// CPUs is passed to `docker run --cpus`. Default 2.
	CPUs float64
	// Memory is passed to `docker run --memory`, e.g. "512m". Default "2g".
	Memory string
}

const (
	labelOwner  = "furcate"
	labelPrefix = "furcate.prefix"
	execIDEnv   = "FURCATE_EXEC_ID"

	// timeoutGrace is how long past ExecRequest.Timeout the driver waits for the
	// in-container timeout before killing the docker CLI itself. Docker waits up
	// to 2 s for output after a process exits when its children still hold
	// stdout, so the grace must be longer than that.
	timeoutGrace = 3 * time.Second
	// maxParallel bounds the docker CLIs a Fork runs at once.
	maxParallel = 16
)

var prefixRE = regexp.MustCompile(`^[a-z0-9]+(?:[._-][a-z0-9]+)*$`)

// timeoutScript runs "$@" under `timeout -s KILL "$0"` when the image has a
// timeout that accepts that form, and plainly otherwise; the host-side deadline
// covers the second case.
const timeoutScript = `if timeout -s KILL "$0" true </dev/null >/dev/null 2>&1; then exec timeout -s KILL "$0" "$@"; fi; exec "$@"`

// killScript SIGKILLs every process whose environment holds FURCATE_EXEC_ID=$1:
// the command an Exec started and everything it spawned. Killing the docker CLI
// does not stop a process inside the container, and busybox timeout kills only
// the command, not its children.
const killScript = `for d in /proc/[0-9]*; do if tr '\0' '\n' < "$d/environ" 2>/dev/null | grep -qxF "` + execIDEnv + `=$1"; then kill -9 "${d#/proc/}" 2>/dev/null; fi; done; true`

// Driver runs instances as Docker containers through the docker CLI. It keeps no
// state of its own, so it is safe for concurrent use.
type Driver struct {
	prefix  string
	workdir string
	cpus    string
	memory  string
}

// New checks that the docker daemon answers and returns a driver.
func New(opts Options) (*Driver, error) {
	if opts.Prefix == "" {
		opts.Prefix = "furcate"
	}
	if !prefixRE.MatchString(opts.Prefix) {
		return nil, fmt.Errorf("docker: prefix %q: want lowercase letters and digits, with single '.', '_' or '-' between them", opts.Prefix)
	}
	if opts.Workdir != "" {
		if !path.IsAbs(opts.Workdir) {
			return nil, fmt.Errorf("docker: workdir %q must be absolute", opts.Workdir)
		}
		opts.Workdir = path.Clean(opts.Workdir)
	}
	if opts.CPUs < 0 {
		return nil, fmt.Errorf("docker: cpus %v is negative", opts.CPUs)
	}
	if opts.CPUs == 0 {
		opts.CPUs = 2
	}
	if opts.Memory == "" {
		opts.Memory = "2g"
	}
	d := &Driver{
		prefix:  opts.Prefix,
		workdir: opts.Workdir,
		cpus:    strconv.FormatFloat(opts.CPUs, 'f', -1, 64),
		memory:  opts.Memory,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := d.docker(ctx, "version", "--format", "{{.Server.Version}}"); err != nil {
		return nil, fmt.Errorf("docker: daemon not reachable: %w", err)
	}
	return d, nil
}

// Caps: files, and forks can run offline.
func (d *Driver) Caps() driver.Caps {
	return driver.Caps{Name: "docker", Level: driver.LevelFiles, OfflineForks: true}
}

// Create starts a container from template, an image reference (pulled if
// missing), on Docker's default network so setup steps can install packages.
func (d *Driver) Create(ctx context.Context, template string) (driver.Instance, error) {
	if template == "" {
		return "", errors.New("docker: template must be an image reference")
	}
	return d.run(ctx, template, false)
}

// Checkpoint commits the container's filesystem to a new image tagged
// <Prefix>-cp:<hex> and returns its image ID.
func (d *Driver) Checkpoint(ctx context.Context, inst driver.Instance) (driver.CheckpointID, error) {
	tag := d.prefix + "-cp:" + randHex()
	// The commit keeps the container's config, including the sleep entrypoint and
	// labels; the LABEL changes make the labels explicit all the same.
	out, err := d.docker(ctx, "commit",
		"--change", "LABEL "+labelOwner+"=1",
		"--change", "LABEL "+labelPrefix+"="+d.prefix,
		string(inst), tag)
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(out)
	if !strings.HasPrefix(id, "sha256:") {
		return "", fmt.Errorf("docker commit: unexpected output %q", out)
	}
	return driver.CheckpointID(id), nil
}

// Fork starts n containers from a checkpoint in parallel, with no network when
// offline. If any fails, Fork removes every container it started and returns an
// error with the ones that had started (already destroyed), so a caller that
// destroys partial results stays correct.
func (d *Driver) Fork(ctx context.Context, cp driver.CheckpointID, n int, offline bool) ([]driver.Instance, error) {
	if n < 1 {
		return nil, fmt.Errorf("docker: fork n=%d: want at least 1", n)
	}
	insts := make([]driver.Instance, n)
	errs := make([]error, n)
	sem := make(chan struct{}, maxParallel)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			insts[i], errs[i] = d.run(ctx, string(cp), offline)
		})
	}
	wg.Wait()

	started := make([]driver.Instance, 0, n)
	for _, inst := range insts {
		if inst != "" {
			started = append(started, inst)
		}
	}
	err := errors.Join(errs...)
	if err == nil {
		return started, nil
	}
	cleanCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	for _, inst := range started {
		d.Destroy(cleanCtx, inst)
	}
	return started, fmt.Errorf("docker: fork: %d of %d failed: %w", n-len(started), n, err)
}

// run starts one kept-alive container from image.
func (d *Driver) run(ctx context.Context, image string, offline bool) (driver.Instance, error) {
	name := d.prefix + "-" + randHex()
	args := []string{"run", "-d", "--name", name,
		"--label", labelOwner + "=1", "--label", labelPrefix + "=" + d.prefix,
		"--init", "--cpus", d.cpus, "--memory", d.memory}
	if d.workdir != "" {
		args = append(args, "-w", d.workdir)
	}
	if offline {
		args = append(args, "--network", "none")
	}
	args = append(args, "--entrypoint", "sleep", image, "infinity")
	if _, err := d.docker(ctx, args...); err != nil {
		// run can fail after creating the container, e.g. when it can't start.
		cleanCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		d.docker(cleanCtx, "rm", "-f", "-v", name)
		return "", err
	}
	return driver.Instance(name), nil
}

// Exec runs a command with `docker exec`. A command that fails to start inside
// the container (no such binary or directory) is a result with exit code 126 or
// 127, like a shell's; only a docker failure is an error. With a Timeout, the
// command runs under the image's `timeout -s KILL` when it has one, and the
// driver also stops waiting at Timeout plus a short grace; either way it then
// kills everything the command started and reports TimedOut. Duration includes
// the docker CLI's own overhead.
func (d *Driver) Exec(ctx context.Context, inst driver.Instance, req driver.ExecRequest) (driver.ExecResult, error) {
	if len(req.Args) == 0 {
		return driver.ExecResult{}, errors.New("docker: empty command")
	}
	dir, err := d.dir(req.Dir)
	if err != nil {
		return driver.ExecResult{}, err
	}
	marker := randHex()
	args := []string{"exec", "-i", "-e", execIDEnv + "=" + marker}
	if dir != "" {
		args = append(args, "-w", dir)
	}
	for _, kv := range req.Env {
		// A bare KEY would make the docker CLI copy this machine's value in.
		if k, _, ok := strings.Cut(kv, "="); !ok || k == "" {
			return driver.ExecResult{}, fmt.Errorf("docker: env %q is not KEY=VALUE", kv)
		}
		args = append(args, "-e", kv)
	}
	args = append(args, string(inst))
	runCtx := ctx
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, req.Timeout+timeoutGrace)
		defer cancel()
		args = append(args, "sh", "-c", timeoutScript, seconds(req.Timeout))
	}
	args = append(args, req.Args...)

	cmd := exec.CommandContext(runCtx, "docker", args...)
	cmd.Stdin = bytes.NewReader(req.Stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.WaitDelay = 2 * time.Second
	start := time.Now()
	runErr := cmd.Run()
	res := driver.ExecResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes(), Duration: time.Since(start)}

	var exitErr *exec.ExitError
	switch {
	case runErr == nil:
		return res, nil
	case ctx.Err() != nil:
		// The caller gave up. The CLI is gone but the command may still be running.
		d.kill(inst, marker)
		return res, fmt.Errorf("docker: exec: %w", ctx.Err())
	case runCtx.Err() != nil:
		d.kill(inst, marker)
		res.TimedOut, res.ExitCode = true, -1
		return res, nil
	case !errors.As(runErr, &exitErr):
		return res, fmt.Errorf("docker: exec: %w", runErr)
	}
	code := exitErr.ExitCode()
	if req.Timeout > 0 && code == 128+9 && res.Duration >= req.Timeout {
		// The in-container timeout fired. Busybox's kills only the command.
		d.kill(inst, marker)
		res.TimedOut, res.ExitCode = true, -1
		return res, nil
	}
	if d.cliFailed(ctx, inst, stderr.String()) {
		return res, fmt.Errorf("docker: exec in %s: %s", inst, strings.TrimSpace(stderr.String()))
	}
	res.ExitCode = code
	return res, nil
}

// cliFailed tells a docker CLI failure from a command's own non-zero exit by
// what the CLI prints. A sandboxed command can print "Cannot connect to the
// Docker daemon" too (an agent running docker in the box), so that case counts
// only if the daemon really can't be reached.
func (d *Driver) cliFailed(ctx context.Context, inst driver.Instance, stderr string) bool {
	s := strings.TrimSpace(stderr)
	if strings.HasPrefix(s, "Error response from daemon:") {
		return true
	}
	if strings.HasPrefix(s, "Cannot connect to the Docker daemon") || strings.HasPrefix(s, "error during connect") {
		out, err := d.docker(ctx, "container", "inspect", "-f", "{{.State.Running}}", string(inst))
		return err != nil || strings.TrimSpace(out) != "true"
	}
	return false
}

// kill stops everything an Exec started, by its marker.
func (d *Driver) kill(inst driver.Instance, marker string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d.docker(ctx, "exec", "-u", "0", string(inst), "sh", "-c", killScript, "sh", marker)
}

func (d *Driver) dir(dir string) (string, error) {
	switch {
	case dir == "":
		return d.workdir, nil
	case path.IsAbs(dir):
		return dir, nil
	case d.workdir == "":
		return "", fmt.Errorf("docker: relative dir %q needs Options.Workdir", dir)
	}
	return path.Join(d.workdir, dir), nil
}

// seconds formats a timeout for timeout(1), at least 1 ms: 0 would mean none.
func seconds(t time.Duration) string {
	return strconv.FormatFloat(max(t.Seconds(), 0.001), 'f', 3, 64)
}

// Destroy removes the container and its anonymous volumes. Removing one that is
// already gone is not an error.
func (d *Driver) Destroy(ctx context.Context, inst driver.Instance) error {
	if !strings.HasPrefix(string(inst), d.prefix+"-") {
		return fmt.Errorf("docker: %q is not a container of this driver (prefix %s)", inst, d.prefix)
	}
	_, err := d.docker(ctx, "rm", "-f", "-v", string(inst))
	if err != nil && strings.Contains(err.Error(), "No such container") {
		return nil
	}
	return err
}

// DeleteCheckpoint removes the checkpoint's tag, which deletes the image. It
// fails while a container still uses the image. A checkpoint taken in a fork of
// this one is a child image; Docker won't delete an image with children, so it
// is only untagged and Docker frees it when the last child goes. Deleting one
// that is already gone is not an error.
func (d *Driver) DeleteCheckpoint(ctx context.Context, cp driver.CheckpointID) error {
	out, err := d.docker(ctx, "image", "inspect", string(cp))
	if err != nil {
		if strings.Contains(err.Error(), "No such image") {
			return nil
		}
		return err
	}
	var imgs []struct {
		RepoTags []string
		Config   struct{ Labels map[string]string }
	}
	if err := json.Unmarshal([]byte(out), &imgs); err != nil || len(imgs) != 1 {
		return fmt.Errorf("docker: inspect %s: unexpected output", cp)
	}
	if imgs[0].Config.Labels[labelPrefix] != d.prefix {
		return fmt.Errorf("docker: image %s is not a checkpoint of this driver (prefix %s)", cp, d.prefix)
	}
	var tags []string
	for _, t := range imgs[0].RepoTags {
		if strings.HasPrefix(t, d.prefix+"-cp:") {
			tags = append(tags, t)
		}
	}
	if len(tags) == 0 {
		return nil // already untagged; kept only as a later checkpoint's parent
	}
	_, err = d.docker(ctx, append([]string{"rmi"}, tags...)...)
	return err
}

// Cleanup removes every container and image labelled with this driver's prefix,
// running or not. It is for tests and a CLI command, not the run controller.
func (d *Driver) Cleanup(ctx context.Context) error {
	filters := []string{"--filter", "label=" + labelOwner + "=1", "--filter", "label=" + labelPrefix + "=" + d.prefix}
	out, err := d.docker(ctx, append([]string{"ps", "-aq", "--no-trunc"}, filters...)...)
	if err != nil {
		return err
	}
	if ids := uniq(out); len(ids) > 0 {
		if _, err := d.docker(ctx, append([]string{"rm", "-f", "-v"}, ids...)...); err != nil {
			return err
		}
	}
	// An image with children can't be removed until they are, and removing a
	// child frees its untagged parents, so repeat until nothing is left.
	var lastErr error
	prev := -1
	for {
		out, err := d.docker(ctx, append([]string{"images", "-aq", "--no-trunc"}, filters...)...)
		if err != nil {
			return err
		}
		ids := uniq(out)
		if len(ids) == 0 {
			return nil
		}
		if len(ids) == prev {
			return fmt.Errorf("docker: cleanup: %d images left: %w", len(ids), lastErr)
		}
		prev = len(ids)
		_, lastErr = d.docker(ctx, append([]string{"rmi", "-f"}, ids...)...)
	}
}

func uniq(out string) []string {
	var ids []string
	seen := map[string]bool{}
	for _, id := range strings.Fields(out) {
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids
}

// docker runs the CLI and returns its stdout. A failure carries its stderr.
func (d *Driver) docker(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.WaitDelay = 2 * time.Second
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return stdout.String(), fmt.Errorf("docker %s: %w", args[0], ctx.Err())
		}
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return stdout.String(), fmt.Errorf("docker %s: %s", args[0], msg)
	}
	return stdout.String(), nil
}

func randHex() string {
	b := make([]byte, 6)
	rand.Read(b)
	return hex.EncodeToString(b)
}
