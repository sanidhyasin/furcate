package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapshot(t *testing.T) {
	traces, ms := fixture(t)
	dir := filepath.Join(t.TempDir(), "site")
	st, err := Snapshot(context.Background(), Config{Traces: traces, Runs: ms}, dir, []string{"r1"},
		SnapshotOptions{Note: "Test runs."})
	if err != nil {
		t.Fatal(err)
	}
	// r1 and its five branches; r1's spans carry two payloads.
	if st.Listed != 1 || st.Runs != 6 || st.Payloads != 2 {
		t.Fatalf("stats %+v, want 1 listed, 6 runs, 2 payloads", st)
	}
	read := func(rel string, v any) {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(dir, rel))
		if err != nil {
			t.Fatal(err)
		}
		if v != nil {
			if err := json.Unmarshal(b, v); err != nil {
				t.Fatalf("%s: %v", rel, err)
			}
		}
	}

	var list struct{ Runs []runItem }
	read("api/runs.json", &list)
	if len(list.Runs) != 1 || list.Runs[0].ID != "r1" || list.Runs[0].Branches != 5 || list.Runs[0].Spans != 4 ||
		list.Runs[0].Outcome != "fail" || list.Runs[0].Name != "invoke_agent keep-fixture" {
		t.Fatalf("runs list %+v", list.Runs)
	}
	var d runDetail
	read("api/runs/c1.json", &d)
	if d.Run == nil || d.Run.ParentRunID != "r1" || d.ForkEnabled {
		t.Fatalf("branch c1: %+v", d)
	}
	var br struct{ Total int }
	read("api/runs/r1/branches.json", &br)
	read("api/runs/c5/branches.json", nil)
	if br.Total != 5 {
		t.Fatalf("r1 has %d branches in the snapshot, want 5", br.Total)
	}
	var files map[string]string
	read("api/files/c1.json", &files)
	if files["new.txt"] != "h4" {
		t.Fatalf("c1's manifest %v", files)
	}
	read("api/payloads/"+hashReq+".txt", nil)
	var info map[string]any
	read("api/info.json", &info)
	if info["static"] != true || info["fork_enabled"] != false || info["featured"] != "r1" || info["note"] != "Test runs." {
		t.Fatalf("info %v", info)
	}
	idx, _ := os.ReadFile(filepath.Join(dir, "index.html"))
	if !strings.Contains(string(idx), `<meta name="furcate-static" content="1">`) {
		t.Fatal("index.html isn't marked as a snapshot")
	}
	read("app.js", nil)
	read("style.css", nil)

	if _, err := Snapshot(context.Background(), Config{Traces: traces, Runs: ms}, dir, []string{"r1"}, SnapshotOptions{}); err == nil {
		t.Fatal("a snapshot overwrote a directory that wasn't empty")
	}
	if _, err := Snapshot(context.Background(), Config{Traces: traces, Runs: ms}, t.TempDir(), []string{"lg1"}, SnapshotOptions{}); err == nil {
		t.Fatal("a snapshot took a trace with no run-store row")
	}
}

func TestSnapshotLeavesOutTaskFiles(t *testing.T) {
	got := withoutFiles(json.RawMessage(`{"id":"t","files":"/home/me/repo","prompt":"p"}`))
	var m map[string]any
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["files"]; ok || m["id"] != "t" || m["prompt"] != "p" {
		t.Fatalf("withoutFiles: %s", got)
	}
	if in := json.RawMessage(`{"id":"t"}`); string(withoutFiles(in)) != string(in) {
		t.Fatal("a task without files changed")
	}
}
