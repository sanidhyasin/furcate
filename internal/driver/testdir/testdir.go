// Package testdir is a driver for tests only. Each instance is a directory, a
// checkpoint is a copy of it, and commands run on this machine with the instance
// as their working directory.
//
// It has NO isolation: a command can read and write anything the test process
// can. That is why it is not offered by the CLI, and why the design doc rules out
// a bare local folder as a real driver. It exists so the run controller and the
// replay engine can be tested end to end without Linux and KVM.
package testdir

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/sanidhyasin/furcate/internal/driver"
)

// Driver keeps instances and checkpoints under a root directory.
type Driver struct {
	root string

	mu    sync.Mutex
	insts map[driver.Instance]string
	cps   map[driver.CheckpointID]string
}

// New uses root (created if needed) for its directories.
func New(root string) (*Driver, error) {
	for _, d := range []string{"inst", "cp"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, err
		}
	}
	return &Driver{root: root, insts: map[driver.Instance]string{}, cps: map[driver.CheckpointID]string{}}, nil
}

func id(prefix string) string {
	b := make([]byte, 6)
	rand.Read(b)
	return prefix + hex.EncodeToString(b)
}

// Caps: files only, and forks can't be cut off from the network.
func (d *Driver) Caps() driver.Caps {
	return driver.Caps{Name: "testdir", Level: driver.LevelFiles, OfflineForks: false}
}

// Create copies the template directory into a new instance; "" starts empty.
func (d *Driver) Create(_ context.Context, template string) (driver.Instance, error) {
	inst := driver.Instance(id("i-"))
	dir := filepath.Join(d.root, "inst", string(inst))
	if template == "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", err
		}
	} else if err := os.CopyFS(dir, os.DirFS(template)); err != nil {
		return "", fmt.Errorf("testdir: copy template: %w", err)
	}
	d.mu.Lock()
	d.insts[inst] = dir
	d.mu.Unlock()
	return inst, nil
}

func (d *Driver) instDir(inst driver.Instance) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	dir, ok := d.insts[inst]
	if !ok {
		return "", fmt.Errorf("testdir: no instance %s", inst)
	}
	return dir, nil
}

// Checkpoint copies the instance's directory.
func (d *Driver) Checkpoint(_ context.Context, inst driver.Instance) (driver.CheckpointID, error) {
	src, err := d.instDir(inst)
	if err != nil {
		return "", err
	}
	cp := driver.CheckpointID(id("cp-"))
	dst := filepath.Join(d.root, "cp", string(cp))
	if err := os.CopyFS(dst, os.DirFS(src)); err != nil {
		return "", err
	}
	d.mu.Lock()
	d.cps[cp] = dst
	d.mu.Unlock()
	return cp, nil
}

// Fork copies a checkpoint into n new instances. offline can't be honoured.
func (d *Driver) Fork(_ context.Context, cp driver.CheckpointID, n int, offline bool) ([]driver.Instance, error) {
	if offline {
		return nil, fmt.Errorf("testdir: offline forks: %w", driver.ErrUnsupported)
	}
	d.mu.Lock()
	src, ok := d.cps[cp]
	d.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("testdir: no checkpoint %s", cp)
	}
	out := make([]driver.Instance, 0, n)
	for range n {
		inst := driver.Instance(id("i-"))
		dir := filepath.Join(d.root, "inst", string(inst))
		if err := os.CopyFS(dir, os.DirFS(src)); err != nil {
			return out, err
		}
		d.mu.Lock()
		d.insts[inst] = dir
		d.mu.Unlock()
		out = append(out, inst)
	}
	return out, nil
}

// Exec runs the command on this machine inside the instance's directory.
func (d *Driver) Exec(ctx context.Context, inst driver.Instance, req driver.ExecRequest) (driver.ExecResult, error) {
	dir, err := d.instDir(inst)
	if err != nil {
		return driver.ExecResult{}, err
	}
	if len(req.Args) == 0 {
		return driver.ExecResult{}, errors.New("testdir: empty command")
	}
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(ctx, req.Args[0], req.Args[1:]...)
	cmd.Dir = filepath.Join(dir, req.Dir)
	cmd.Env = append(os.Environ(), req.Env...)
	cmd.Stdin = bytes.NewReader(req.Stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	start := time.Now()
	runErr := cmd.Run()
	res := driver.ExecResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes(), Duration: time.Since(start)}
	var exitErr *exec.ExitError
	switch {
	case runErr == nil:
	case ctx.Err() == context.DeadlineExceeded:
		res.TimedOut, res.ExitCode = true, -1
	case errors.As(runErr, &exitErr):
		res.ExitCode = exitErr.ExitCode()
	default:
		return res, runErr
	}
	return res, nil
}

// Destroy removes the instance's directory.
func (d *Driver) Destroy(_ context.Context, inst driver.Instance) error {
	d.mu.Lock()
	dir, ok := d.insts[inst]
	delete(d.insts, inst)
	d.mu.Unlock()
	if !ok {
		return nil
	}
	return os.RemoveAll(dir)
}

// DeleteCheckpoint removes the checkpoint's copy.
func (d *Driver) DeleteCheckpoint(_ context.Context, cp driver.CheckpointID) error {
	d.mu.Lock()
	dir, ok := d.cps[cp]
	delete(d.cps, cp)
	d.mu.Unlock()
	if !ok {
		return nil
	}
	return os.RemoveAll(dir)
}
