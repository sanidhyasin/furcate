package docker

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sanidhyasin/furcate/internal/driver"
)

// Needs a running Docker daemon; enable with FURCATE_TEST_DOCKER=1.
func newTestDriver(t *testing.T) *Driver {
	t.Helper()
	if os.Getenv("FURCATE_TEST_DOCKER") == "" {
		t.Skip("set FURCATE_TEST_DOCKER=1 to run against Docker")
	}
	d, err := New(Options{Prefix: "furcatetest", Workdir: "/work", Memory: "256m", CPUs: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := d.Cleanup(ctx); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	return d
}

const image = "alpine:3.22"

func TestCheckpointForkOffline(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	if c := d.Caps(); c.Level != driver.LevelFiles || !c.OfflineForks {
		t.Fatalf("caps %+v", c)
	}
	inst, err := d.Create(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	write := func(inst driver.Instance, path, content string) {
		t.Helper()
		res, err := d.Exec(ctx, inst, driver.ExecRequest{
			Args:  []string{"sh", "-c", `mkdir -p "$(dirname "$1")" && cat > "$1"`, "sh", path},
			Stdin: []byte(content),
		})
		if err != nil || res.ExitCode != 0 {
			t.Fatalf("write %s: %v %+v", path, err, res)
		}
	}
	read := func(inst driver.Instance, path string) string {
		t.Helper()
		res, err := d.Exec(ctx, inst, driver.ExecRequest{Args: []string{"cat", path}})
		if err != nil {
			t.Fatal(err)
		}
		return string(res.Stdout)
	}
	write(inst, "notes/plan.md", "keep the fixture")

	start := time.Now()
	cp, err := d.Checkpoint(ctx, inst)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("checkpoint took %s", time.Since(start).Round(time.Millisecond))

	// Change the original after the checkpoint; forks must not see it.
	write(inst, "notes/plan.md", "changed later")

	start = time.Now()
	forks, err := d.Fork(ctx, cp, 3, true)
	if err != nil || len(forks) != 3 {
		t.Fatalf("fork: %v (%d)", err, len(forks))
	}
	t.Logf("fork of 3 took %s", time.Since(start).Round(time.Millisecond))
	for _, f := range forks {
		if got := read(f, "notes/plan.md"); got != "keep the fixture" {
			t.Fatalf("fork %s has %q", f, got)
		}
	}
	// Offline forks can't reach anything.
	res, err := d.Exec(ctx, forks[0], driver.ExecRequest{Args: []string{"wget", "-q", "-T", "3", "-O", "-", "http://1.1.1.1"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.ExitCode == 0 {
		t.Fatal("an offline fork reached the network")
	}
	// The original still has the network.
	if got := read(inst, "notes/plan.md"); got != "changed later" {
		t.Fatalf("original has %q", got)
	}

	for _, f := range append(forks, inst) {
		if err := d.Destroy(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Destroy(ctx, inst); err != nil {
		t.Fatalf("destroy twice: %v", err)
	}
	if err := d.DeleteCheckpoint(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteCheckpoint(ctx, cp); err != nil {
		t.Fatalf("delete twice: %v", err)
	}
}

func TestExecResults(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	inst, err := d.Create(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Destroy(ctx, inst)

	res, err := d.Exec(ctx, inst, driver.ExecRequest{Args: []string{"sh", "-c", "echo out; echo err >&2; exit 3"}})
	if err != nil || res.ExitCode != 3 || string(res.Stdout) != "out\n" || string(res.Stderr) != "err\n" {
		t.Fatalf("exit: %v %+v", err, res)
	}
	res, err = d.Exec(ctx, inst, driver.ExecRequest{Args: []string{"no-such-binary"}})
	if err != nil || res.ExitCode == 0 {
		t.Fatalf("missing binary should be a non-zero result: %v %+v", err, res)
	}
	res, err = d.Exec(ctx, inst, driver.ExecRequest{Args: []string{"pwd"}, Dir: "sub"})
	if err != nil || res.ExitCode == 0 && strings.TrimSpace(string(res.Stdout)) != "/work/sub" {
		t.Fatalf("relative dir: %v %+v", err, res)
	}

	start := time.Now()
	res, err = d.Exec(ctx, inst, driver.ExecRequest{Args: []string{"sh", "-c", "sleep 30 & sleep 30"}, Timeout: time.Second})
	if err != nil || !res.TimedOut {
		t.Fatalf("timeout: %v %+v", err, res)
	}
	if took := time.Since(start); took > 8*time.Second {
		t.Fatalf("timeout took %s", took)
	}
	// Nothing the timed-out command started is left running.
	res, _ = d.Exec(ctx, inst, driver.ExecRequest{Args: []string{"sh", "-c", "ps | grep -c '[s]leep 30' || true"}})
	if strings.TrimSpace(string(res.Stdout)) != "0" {
		t.Fatalf("leftover processes: %s", res.Stdout)
	}
}
