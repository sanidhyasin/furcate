package tine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanidhyasin/furcate/internal/driver"
)

// fakeTined answers like tined for the calls the driver makes.
func fakeTined(t *testing.T) (*Driver, *fakeState) {
	t.Helper()
	st := &fakeState{}
	mux := http.NewServeMux()
	writeErr := func(w http.ResponseWriter, status int, code string) {
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"error":{"code":%q,"message":"fake %s"}}`, code, code)
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `{"status":"ok","version":"0.9.0","features":["diff-snapshots"]}`)
	})
	mux.HandleFunc("POST /v1/vms", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		json.NewDecoder(r.Body).Decode(&req)
		st.lastCreate = req
		w.WriteHeader(201)
		io.WriteString(w, `{"id":"vm-1","state":"running"}`)
	})
	mux.HandleFunc("POST /v1/vms/{id}/snapshots", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		json.NewDecoder(r.Body).Decode(&req)
		if req["retain"] != true {
			writeErr(w, 400, "invalid_request")
			return
		}
		if st.snapCalls.Add(1) == 1 {
			writeErr(w, 409, "busy") // an exec is still finishing
			return
		}
		w.WriteHeader(201)
		io.WriteString(w, `{"id":"snap-a","state":"ready","size_bytes":1024}`)
	})
	mux.HandleFunc("POST /v1/snapshots/{id}/fork", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Count   int    `json:"count"`
			Network string `json:"network"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		st.lastNetwork = req.Network
		var kids []string
		for i := range req.Count {
			if i == 2 {
				kids = append(kids, `{"error":{"code":"no_memory","message":"full"}}`)
				continue
			}
			kids = append(kids, fmt.Sprintf(`{"vm":{"id":"vm-c%d","state":"running"}}`, i))
		}
		w.WriteHeader(201)
		fmt.Fprintf(w, `{"snapshot":{"id":%q},"children":[%s]}`, r.PathValue("id"), strings.Join(kids, ","))
	})
	mux.HandleFunc("POST /v1/vms/{id}/exec", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Args  []string `json:"args"`
			Stdin string   `json:"stdin"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/x-ndjson")
		b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
		in, _ := base64.StdEncoding.DecodeString(req.Stdin)
		fmt.Fprintf(w, "{\"stream\":\"stdout\",\"data\":%q}\n", b64("hello "))
		fmt.Fprintf(w, "{\"stream\":\"stdout\",\"data\":%q}\n", b64(string(in)))
		fmt.Fprintf(w, "{\"stream\":\"stderr\",\"data\":%q}\n", b64("warn"))
		if req.Args[0] == "cut" {
			return // no exit frame
		}
		io.WriteString(w, `{"exit":{"code":3,"signal":"","timed_out":false,"duration_ms":12.5}}`+"\n")
	})
	mux.HandleFunc("DELETE /v1/vms/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") == "gone" {
			writeErr(w, 404, "not_found")
			return
		}
		io.WriteString(w, `{"id":"vm-1","state":"destroyed"}`)
	})
	mux.HandleFunc("DELETE /v1/snapshots/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") == "used" {
			writeErr(w, 409, "in_use")
			return
		}
		w.WriteHeader(204)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	d, err := New(Options{HTTPClient: srv.Client(), BaseURL: srv.URL, VCPUs: 2, MemoryMiB: 1024})
	if err != nil {
		t.Fatal(err)
	}
	return d, st
}

type fakeState struct {
	lastCreate  map[string]any
	lastNetwork string
	snapCalls   atomic.Int64
}

func TestDriverAgainstFakeTined(t *testing.T) {
	d, st := fakeTined(t)
	ctx := context.Background()
	if d.Version != "0.9.0" || d.Caps().Level != driver.LevelMachine || !d.Caps().OfflineForks {
		t.Fatalf("health/caps: %s %+v", d.Version, d.Caps())
	}

	inst, err := d.Create(ctx, "python")
	if err != nil || inst != "vm-1" {
		t.Fatalf("create: %v %s", err, inst)
	}
	if st.lastCreate["image"] != "python" || st.lastCreate["vcpus"] != float64(2) || st.lastCreate["ttl_seconds"] != float64(7200) {
		t.Fatalf("create request: %v", st.lastCreate)
	}

	cp, err := d.Checkpoint(ctx, inst)
	if err != nil || cp != "snap-a" || st.snapCalls.Load() != 2 {
		t.Fatalf("checkpoint should retry once on busy: %v %s %d", err, cp, st.snapCalls.Load())
	}

	kids, err := d.Fork(ctx, cp, 4, true)
	if st.lastNetwork != "none" {
		t.Fatalf("offline fork sent network=%q", st.lastNetwork)
	}
	if len(kids) != 3 || err == nil || !IsCode(err, "no_memory") {
		t.Fatalf("fork: %d children, err %v", len(kids), err)
	}

	res, err := d.Exec(ctx, inst, driver.ExecRequest{Args: []string{"cat"}, Stdin: []byte("world"), Timeout: time.Second})
	if err != nil || res.ExitCode != 3 || string(res.Stdout) != "hello world" || string(res.Stderr) != "warn" ||
		res.Duration != 12500*time.Microsecond {
		t.Fatalf("exec: %v %+v", err, res)
	}
	if _, err := d.Exec(ctx, inst, driver.ExecRequest{Args: []string{"cut"}}); err == nil || !strings.Contains(err.Error(), "exit frame") {
		t.Fatalf("cut-off stream: %v", err)
	}

	if err := d.Destroy(ctx, "gone"); err != nil {
		t.Fatalf("destroying a gone VM: %v", err)
	}
	if err := d.DeleteCheckpoint(ctx, "used"); !IsCode(err, "in_use") {
		t.Fatalf("delete in use: %v", err)
	}
	if err := d.DeleteCheckpoint(ctx, cp); err != nil {
		t.Fatal(err)
	}
}

func TestHostParsing(t *testing.T) {
	for _, h := range []string{"unix:///run/tine/tine.sock", "ssh://tine-dev", "ssh://ubuntu@1.2.3.4"} {
		if _, err := dialer(Options{Host: h, SSH: "ssh"}); err != nil {
			t.Errorf("%s: %v", h, err)
		}
	}
	if _, err := dialer(Options{Host: "http://x"}); err == nil {
		t.Error("http:// should be refused")
	}
}
