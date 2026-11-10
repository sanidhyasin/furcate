// Package tine is the sandbox driver for tine, the Firecracker microVM sandbox
// that forks running VMs. It saves whole machines (level 3): memory, processes
// and files.
//
// It talks to tined's HTTP API: on its Unix socket when Furcate runs on the tine
// host, or through `ssh <host> tine dial-stdio` from another machine, as the
// tine CLI does. The agent loop and the model calls stay on the machine running
// Furcate; only tools execute in the VMs.
//
// Checkpoints are retained snapshots, so they outlive the VM they came from, and
// forks restore N VMs from a snapshot, offline when asked. Those three calls are
// the additions Furcate needed from tine.
package tine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/sanidhyasin/furcate/internal/driver"
)

// Options configures the driver. The zero value talks to the local socket.
type Options struct {
	// Host is "unix:///run/tine/tine.sock" (the default) or "ssh://<host>", where
	// <host> is anything ssh accepts, e.g. an entry in ~/.ssh/config.
	Host string
	// SSH is the ssh command for ssh:// hosts, split on spaces. Default "ssh".
	SSH string
	// VCPUs and MemoryMiB size each VM; 0 leaves tine's defaults.
	VCPUs     int
	MemoryMiB int
	// TTL bounds a VM's life, so a crashed Furcate can't leave VMs running.
	// Default 2 hours.
	TTL time.Duration
	// Strategy is how restored VMs get their memory: share (default), copy, uffd
	// or uffd-prefetch.
	Strategy string
	// MaxOutputBytes caps one command's output. Default 4 MiB.
	MaxOutputBytes int
	// HTTPClient and BaseURL replace the transport, for tests.
	HTTPClient *http.Client
	BaseURL    string
}

// Driver is safe for concurrent use.
type Driver struct {
	opts   Options
	client *http.Client
	base   string
	// Features tined reported on /healthz.
	Features []string
	Version  string
}

// New connects to tined and checks it answers.
func New(opts Options) (*Driver, error) {
	if opts.Host == "" {
		opts.Host = "unix:///run/tine/tine.sock"
	}
	if opts.SSH == "" {
		opts.SSH = "ssh"
	}
	if opts.TTL <= 0 {
		opts.TTL = 2 * time.Hour
	}
	if opts.MaxOutputBytes <= 0 {
		opts.MaxOutputBytes = 4 << 20
	}
	d := &Driver{opts: opts, client: opts.HTTPClient, base: opts.BaseURL}
	if d.client == nil {
		dial, err := dialer(opts)
		if err != nil {
			return nil, err
		}
		d.client = &http.Client{Transport: &http.Transport{
			DialContext:         dial,
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
		}}
		d.base = "http://tined"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var health struct {
		Status   string   `json:"status"`
		Version  string   `json:"version"`
		Features []string `json:"features"`
	}
	if err := d.call(ctx, http.MethodGet, "/healthz", nil, &health); err != nil {
		return nil, fmt.Errorf("tine: %s not reachable: %w", opts.Host, err)
	}
	d.Version, d.Features = health.Version, health.Features
	return d, nil
}

func dialer(opts Options) (func(ctx context.Context, network, addr string) (net.Conn, error), error) {
	u, err := url.Parse(opts.Host)
	if err != nil {
		return nil, fmt.Errorf("tine: host %q: %w", opts.Host, err)
	}
	switch u.Scheme {
	case "unix":
		path := u.Path
		return func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", path)
		}, nil
	case "ssh":
		host := u.Host
		if u.User != nil {
			host = u.User.String() + "@" + host
		}
		ssh := strings.Fields(opts.SSH)
		return func(ctx context.Context, _, _ string) (net.Conn, error) {
			args := append(ssh[1:], "-o", "BatchMode=yes", host, "tine", "dial-stdio")
			return dialStdio(ssh[0], args...)
		}, nil
	}
	return nil, fmt.Errorf("tine: host %q: want unix:// or ssh://", opts.Host)
}

// stdioConn is a connection over a child process's stdin and stdout: here, ssh
// running `tine dial-stdio`, which pipes them to tined's socket on the far side.
type stdioConn struct {
	cmd  *exec.Cmd
	r    io.ReadCloser
	w    io.WriteCloser
	once sync.Once
}

func dialStdio(name string, args ...string) (net.Conn, error) {
	cmd := exec.Command(name, args...)
	w, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	r, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	return &stdioConn{cmd: cmd, r: r, w: w}, nil
}

func (c *stdioConn) Read(b []byte) (int, error)  { return c.r.Read(b) }
func (c *stdioConn) Write(b []byte) (int, error) { return c.w.Write(b) }
func (c *stdioConn) Close() error {
	c.once.Do(func() {
		c.w.Close()
		c.cmd.Process.Kill()
		c.cmd.Wait()
	})
	return nil
}
func (c *stdioConn) LocalAddr() net.Addr              { return stdioAddr{} }
func (c *stdioConn) RemoteAddr() net.Addr             { return stdioAddr{} }
func (c *stdioConn) SetDeadline(time.Time) error      { return nil }
func (c *stdioConn) SetReadDeadline(time.Time) error  { return nil }
func (c *stdioConn) SetWriteDeadline(time.Time) error { return nil }

type stdioAddr struct{}

func (stdioAddr) Network() string { return "stdio" }
func (stdioAddr) String() string  { return "tine-dial-stdio" }

// APIError is an error tined answered with.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("tine: %s (%d): %s", e.Code, e.Status, e.Message)
}

func apiError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var e struct {
		Error struct{ Code, Message string } `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error.Code != "" {
		return &APIError{Status: resp.StatusCode, Code: e.Error.Code, Message: e.Error.Message}
	}
	return &APIError{Status: resp.StatusCode, Code: "http", Message: strings.TrimSpace(string(body))}
}

// IsCode reports whether err is a tine error with this code.
func IsCode(err error, code string) bool {
	var e *APIError
	return errors.As(err, &e) && e.Code == code
}

func (d *Driver) call(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, d.base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return apiError(resp)
	}
	if out == nil {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type vm struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Error string `json:"error"`
}

type snapshot struct {
	ID        string             `json:"id"`
	State     string             `json:"state"`
	SizeBytes int64              `json:"size_bytes"`
	Timings   map[string]float64 `json:"timings_ms"`
}

// Caps: whole machines, and forks run offline.
func (d *Driver) Caps() driver.Caps {
	return driver.Caps{Name: "tine", Level: driver.LevelMachine, OfflineForks: true}
}

// Create starts a VM from an image (restored from its template when tine has
// one of the right size). The first VM keeps the network for setup steps.
func (d *Driver) Create(ctx context.Context, template string) (driver.Instance, error) {
	req := map[string]any{"image": template, "ttl_seconds": int(d.opts.TTL.Seconds())}
	if d.opts.VCPUs > 0 {
		req["vcpus"] = d.opts.VCPUs
	}
	if d.opts.MemoryMiB > 0 {
		req["memory_mib"] = d.opts.MemoryMiB
	}
	if d.opts.Strategy != "" {
		req["strategy"] = d.opts.Strategy
	}
	var v vm
	if err := d.call(ctx, http.MethodPost, "/v1/vms", req, &v); err != nil {
		return "", err
	}
	if v.State != "running" {
		d.Destroy(context.WithoutCancel(ctx), driver.Instance(v.ID))
		return "", fmt.Errorf("tine: VM %s is %s: %s", v.ID, v.State, v.Error)
	}
	return driver.Instance(v.ID), nil
}

// Checkpoint takes a retained snapshot: it stays until DeleteCheckpoint, even
// after the VM is gone. tined refuses a snapshot while a command runs (busy);
// the controller never overlaps them, but a straggling exec is waited out.
func (d *Driver) Checkpoint(ctx context.Context, inst driver.Instance) (driver.CheckpointID, error) {
	backoff := 50 * time.Millisecond
	for {
		var s snapshot
		err := d.call(ctx, http.MethodPost, "/v1/vms/"+string(inst)+"/snapshots", map[string]any{"retain": true}, &s)
		if err == nil {
			return driver.CheckpointID(s.ID), nil
		}
		if !IsCode(err, "busy") {
			return "", err
		}
		select {
		case <-time.After(backoff):
			backoff = min(backoff*2, 2*time.Second)
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// Fork restores n VMs from a snapshot. Children that failed are reported in the
// error; the ones that started are returned too, so a caller can destroy them.
func (d *Driver) Fork(ctx context.Context, cp driver.CheckpointID, n int, offline bool) ([]driver.Instance, error) {
	req := map[string]any{"count": n, "ttl_seconds": int(d.opts.TTL.Seconds())}
	if d.opts.Strategy != "" {
		req["strategy"] = d.opts.Strategy
	}
	if offline {
		req["network"] = "none"
	}
	var res struct {
		Children []struct {
			VM    *vm `json:"vm"`
			Error *struct {
				Code, Message string
			} `json:"error"`
		} `json:"children"`
	}
	if err := d.call(ctx, http.MethodPost, "/v1/snapshots/"+string(cp)+"/fork", req, &res); err != nil {
		return nil, err
	}
	var out []driver.Instance
	var errs []error
	for _, c := range res.Children {
		switch {
		case c.VM != nil && c.VM.State == "running":
			out = append(out, driver.Instance(c.VM.ID))
		case c.Error != nil:
			errs = append(errs, &APIError{Code: c.Error.Code, Message: c.Error.Message})
		case c.VM != nil:
			errs = append(errs, fmt.Errorf("tine: child %s is %s: %s", c.VM.ID, c.VM.State, c.VM.Error))
		}
	}
	if len(out) < n && len(errs) == 0 {
		errs = append(errs, fmt.Errorf("tine: asked for %d children, got %d", n, len(out)))
	}
	if len(errs) > 0 {
		return out, fmt.Errorf("tine: fork: %d of %d failed: %w", n-len(out), n, errors.Join(errs...))
	}
	return out, nil
}

// Exec runs a command through tine's in-VM agent, reading the NDJSON stream of
// output frames and the exit frame.
func (d *Driver) Exec(ctx context.Context, inst driver.Instance, req driver.ExecRequest) (driver.ExecResult, error) {
	in := map[string]any{"args": req.Args, "max_output_bytes": d.opts.MaxOutputBytes}
	if req.Dir != "" {
		in["dir"] = req.Dir
	}
	if len(req.Env) > 0 {
		in["env"] = req.Env
	}
	if len(req.Stdin) > 0 {
		in["stdin"] = base64.StdEncoding.EncodeToString(req.Stdin)
	}
	if req.Timeout > 0 {
		in["timeout_ms"] = req.Timeout.Milliseconds()
	}
	body, err := json.Marshal(in)
	if err != nil {
		return driver.ExecResult{}, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, d.base+"/v1/vms/"+string(inst)+"/exec", bytes.NewReader(body))
	if err != nil {
		return driver.ExecResult{}, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := d.client.Do(hreq)
	if err != nil {
		return driver.ExecResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return driver.ExecResult{}, apiError(resp)
	}
	res, err := readExec(resp.Body)
	if res.Duration == 0 {
		res.Duration = time.Since(start)
	}
	return res, err
}

// readExec parses an exec stream. A stream without an exit frame was cut off.
func readExec(r io.Reader) (driver.ExecResult, error) {
	var res driver.ExecResult
	var stdout, stderr bytes.Buffer
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 64<<20)
	for sc.Scan() {
		var f struct {
			Stream string `json:"stream"`
			Data   string `json:"data"`
			Exit   *struct {
				Code       int     `json:"code"`
				Signal     string  `json:"signal"`
				TimedOut   bool    `json:"timed_out"`
				DurationMs float64 `json:"duration_ms"`
			} `json:"exit"`
		}
		if err := json.Unmarshal(sc.Bytes(), &f); err != nil {
			return res, fmt.Errorf("tine: bad exec frame: %w", err)
		}
		if f.Exit != nil {
			res.Stdout, res.Stderr = stdout.Bytes(), stderr.Bytes()
			res.ExitCode, res.TimedOut = f.Exit.Code, f.Exit.TimedOut
			if f.Exit.Signal != "" && res.ExitCode == 0 {
				res.ExitCode = -1
			}
			res.Duration = time.Duration(f.Exit.DurationMs * float64(time.Millisecond))
			return res, nil
		}
		data, err := base64.StdEncoding.DecodeString(f.Data)
		if err != nil {
			return res, fmt.Errorf("tine: bad exec output: %w", err)
		}
		if f.Stream == "stderr" {
			stderr.Write(data)
		} else {
			stdout.Write(data)
		}
	}
	res.Stdout, res.Stderr = stdout.Bytes(), stderr.Bytes()
	if err := sc.Err(); err != nil {
		return res, fmt.Errorf("tine: exec stream: %w", err)
	}
	return res, errors.New("tine: exec stream ended without an exit frame")
}

// Destroy destroys the VM; its retained snapshots stay.
func (d *Driver) Destroy(ctx context.Context, inst driver.Instance) error {
	err := d.call(ctx, http.MethodDelete, "/v1/vms/"+string(inst), nil, nil)
	if IsCode(err, "not_found") {
		return nil
	}
	return err
}

// DeleteCheckpoint deletes a snapshot. It fails while VMs run from it.
func (d *Driver) DeleteCheckpoint(ctx context.Context, cp driver.CheckpointID) error {
	err := d.call(ctx, http.MethodDelete, "/v1/snapshots/"+string(cp), nil, nil)
	if IsCode(err, "not_found") {
		return nil
	}
	return err
}
