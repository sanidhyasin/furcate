package tine_test

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sanidhyasin/furcate/internal/driver"
	"github.com/sanidhyasin/furcate/internal/driver/tine"
)

// Measurements of the tine driver for the README: checkpoint time per agent
// step and fork fan-out, each repeated. Run it on the tine host, over tined's
// socket, so no network sits between Furcate and tined:
//
//	FURCATE_TINE_BENCH=unix:///run/tine/tine.sock ./tine.test -test.run Bench -test.v
//
// FURCATE_TINE_BENCH_STEPS (20), _VMS (3), _FORKS ("1,10,50,100") and _REPS (5)
// size it; FURCATE_TINE_IMAGE picks the image ("base").
func TestBenchOnTine(t *testing.T) {
	host := os.Getenv("FURCATE_TINE_BENCH")
	if host == "" {
		t.Skip("set FURCATE_TINE_BENCH to a tine host, e.g. unix:///run/tine/tine.sock")
	}
	image := envOr("FURCATE_TINE_IMAGE", "base")
	steps, vms, reps := envInt(t, "FURCATE_TINE_BENCH_STEPS", 20), envInt(t, "FURCATE_TINE_BENCH_VMS", 3), envInt(t, "FURCATE_TINE_BENCH_REPS", 5)
	var sizes []int
	for _, s := range strings.Split(envOr("FURCATE_TINE_BENCH_FORKS", "1,10,50,100"), ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			t.Fatalf("FURCATE_TINE_BENCH_FORKS: %v", err)
		}
		sizes = append(sizes, n)
	}
	ctx := context.Background()
	d, err := tine.New(tine.Options{Host: host, SSH: os.Getenv("FURCATE_TINE_SSH"), TTL: 30 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("tine %s, features %v; client: %d CPUs, %s", d.Version, d.Features, runtime.NumCPU(), cpuModel())
	t.Logf("image %s; %d VMs x %d steps; forks %v x %d", image, vms, steps, sizes, reps)

	// Checkpoints: each step writes 1 MiB of new data while a process runs, as an
	// agent's tool call would, then the controller checkpoints before the next
	// LLM call.
	var first, later []time.Duration
	var cps []driver.CheckpointID
	for v := 0; v < vms; v++ {
		inst, err := d.Create(ctx, image)
		if err != nil {
			t.Fatal(err)
		}
		mustExec(t, ctx, d, inst, `nohup sh -c 'while :; do date +%s%N > /tmp/hb; sleep 0.2; done' >/dev/null 2>&1 &`)
		for s := 0; s < steps; s++ {
			mustExec(t, ctx, d, inst, fmt.Sprintf("dd if=/dev/urandom of=/root/step-%d bs=1M count=1 status=none", s))
			start := time.Now()
			cp, err := d.Checkpoint(ctx, inst)
			took := time.Since(start)
			if err != nil {
				t.Fatalf("vm %d step %d: checkpoint: %v", v, s, err)
			}
			cps = append(cps, cp)
			if s == 0 {
				first = append(first, took)
			} else {
				later = append(later, took)
			}
		}
		d.Destroy(ctx, inst)
	}
	report(t, "checkpoint, a VM's first", first)
	report(t, "checkpoint, later steps", later)

	// Forks from the last checkpoint, a retained snapshot whose VM is gone, offline:
	// until every child is running, and until every child has answered a command.
	from := cps[len(cps)-1]
	for _, n := range sizes {
		var running, ready []time.Duration
		for r := 0; r < reps; r++ {
			start := time.Now()
			kids, err := d.Fork(ctx, from, n, true)
			up := time.Since(start)
			if err != nil {
				t.Logf("fork %d, rep %d: %v (%d started)", n, r, err, len(kids))
				destroyAll(ctx, d, kids)
				continue
			}
			var wg sync.WaitGroup
			for _, k := range kids {
				wg.Add(1)
				go func() {
					defer wg.Done()
					mustExec(t, ctx, d, k, "test -f /root/step-0")
				}()
			}
			wg.Wait()
			running, ready = append(running, up), append(ready, time.Since(start))
			destroyAll(ctx, d, kids)
		}
		report(t, fmt.Sprintf("fork %d, all running", n), running)
		report(t, fmt.Sprintf("fork %d, all answered", n), ready)
	}

	var dels []time.Duration
	for _, cp := range cps {
		start := time.Now()
		if err := d.DeleteCheckpoint(ctx, cp); err != nil {
			t.Errorf("delete %s: %v", cp, err)
		}
		dels = append(dels, time.Since(start))
	}
	report(t, "delete a checkpoint", dels)
}

func mustExec(t *testing.T, ctx context.Context, d *tine.Driver, inst driver.Instance, cmd string) {
	t.Helper()
	res, err := d.Exec(ctx, inst, driver.ExecRequest{Args: []string{"sh", "-c", cmd}, Timeout: time.Minute})
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("%s: %v exit %d %s", cmd, err, res.ExitCode, res.Stderr)
	}
}

func destroyAll(ctx context.Context, d *tine.Driver, insts []driver.Instance) {
	var wg sync.WaitGroup
	for _, i := range insts {
		wg.Add(1)
		go func() { defer wg.Done(); d.Destroy(ctx, i) }()
	}
	wg.Wait()
}

func report(t *testing.T, what string, ds []time.Duration) {
	if len(ds) == 0 {
		t.Logf("%-32s no data", what)
		return
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	q := func(p float64) time.Duration { return s[min(len(s)-1, int(p*float64(len(s))))] }
	t.Logf("%-32s n=%-3d p50 %-9s p95 %-9s max %s", what, len(s),
		q(0.50).Round(time.Millisecond), q(0.95).Round(time.Millisecond), s[len(s)-1].Round(time.Millisecond))
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(t *testing.T, k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("%s: %v", k, err)
	}
	return n
}

func cpuModel() string {
	b, _ := os.ReadFile("/proc/cpuinfo")
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "model name") {
			if _, v, ok := strings.Cut(l, ":"); ok {
				return strings.TrimSpace(v)
			}
		}
	}
	return runtime.GOOS + "/" + runtime.GOARCH
}
