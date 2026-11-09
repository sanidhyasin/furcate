// Package task defines what an agent run does: where it starts, what it is told,
// and how its success is checked.
package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

// Task is loaded from a JSON file and frozen into each run's row.
type Task struct {
	ID       string     `json:"id"`
	Template string     `json:"template"`        // the driver's template: an image name on tine
	Workdir  string     `json:"workdir"`         // where tools run inside the sandbox
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

// Load reads and validates a task file.
func Load(path string) (*Task, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b)
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
