// Package task defines what an agent run does: where it starts, what it is told,
// and how its success is checked.
package task

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// Task is loaded from a JSON file and frozen into each run's row.
type Task struct {
	ID       string     `json:"id"`
	Template string     `json:"template"`        // the driver's template: an image name on tine
	Workdir  string     `json:"workdir"`         // where tools run inside the sandbox
	Files    string     `json:"files,omitempty"` // local directory copied into the workdir before setup
	Setup    [][]string `json:"setup,omitempty"` // commands run before step 1
	System   string     `json:"system"`
	Prompt   string     `json:"prompt"`
	Model    string     `json:"model,omitempty"`  // default claude-opus-5-5
	Effort   string     `json:"effort,omitempty"` // low, medium, high, xhigh, max; default high
	MaxSteps int        `json:"max_steps,omitempty"`
	// Check decides the outcome: exit code 0 is a pass. It runs in the sandbox
	// after the agent stops, e.g. ["bash", "-lc", "pytest -q"].
	Check []string `json:"check"`
	// ToolTimeout bounds one tool call. Default 2 minutes.
	ToolTimeout Duration `json:"tool_timeout,omitempty"`
}

// Duration reads "90s" style JSON strings.
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	d.Duration = v
	return err
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

// Defaults.
const (
	DefaultModel    = "claude-opus-5-5"
	DefaultEffort   = "high"
	DefaultMaxSteps = 40
	DefaultTimeout  = 2 * time.Minute
)

// Load reads and validates a task file. A relative Files path is resolved
// against the task file's directory.
func Load(path string) (*Task, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	t, err := Parse(b)
	if err != nil {
		return nil, err
	}
	if t.Files != "" && !filepath.IsAbs(t.Files) {
		t.Files = filepath.Join(filepath.Dir(path), t.Files)
	}
	if t.Files != "" {
		if abs, err := filepath.Abs(t.Files); err == nil {
			t.Files = abs
		}
	}
	return t, nil
}

// Archive returns Files as a gzipped tarball, ready to unpack in the workdir.
func (t *Task) Archive() ([]byte, error) {
	if t.Files == "" {
		return nil, nil
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	err := filepath.WalkDir(t.Files, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(t.Files, path)
		if err != nil || rel == "." {
			return err
		}
		if d.Name() == "__pycache__" || d.Name() == ".pytest_cache" || d.Name() == ".git" {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		hdr.Uid, hdr.Gid, hdr.Uname, hdr.Gname = 0, 0, "", ""
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if d.Type().IsRegular() {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			_, err = tw.Write(b)
			return err
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("task files: %w", err)
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Parse validates a task spec and fills in defaults.
func Parse(b []byte) (*Task, error) {
	var t Task
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, fmt.Errorf("task: %w", err)
	}
	if t.ID == "" || t.Prompt == "" || len(t.Check) == 0 {
		return nil, errors.New("task: id, prompt and check are required")
	}
	if t.Model == "" {
		t.Model = DefaultModel
	}
	if t.Effort == "" {
		t.Effort = DefaultEffort
	}
	if t.MaxSteps <= 0 {
		t.MaxSteps = DefaultMaxSteps
	}
	if t.ToolTimeout.Duration <= 0 {
		t.ToolTimeout.Duration = DefaultTimeout
	}
	return &t, nil
}

// JSON is the frozen form stored with a run.
func (t *Task) JSON() json.RawMessage {
	b, _ := json.Marshal(t)
	return b
}
