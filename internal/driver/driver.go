// Package driver is the seam between Furcate's core and a sandbox. The core
// (traces, replay, bisect, evals) never talks to a sandbox directly: it calls a
// Sandbox, and each sandbox gets a small driver.
//
// The interface holds only the calls the core makes. A driver declares the level
// it can save and whether its forks can run offline; a feature that needs more is
// switched off for that driver, never faked.
package driver

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Level is how much of a run's state a driver saves at each step.
type Level int

const (
	LevelRecording    Level = 0 // nothing; traces only
	LevelConversation Level = 1 // the chat history
	LevelFiles        Level = 2 // the files, like a commit per step
	LevelMachine      Level = 3 // memory, processes and files
)

func (l Level) String() string {
	switch l {
	case LevelRecording:
		return "recording"
	case LevelConversation:
		return "conversation"
	case LevelFiles:
		return "files"
	case LevelMachine:
		return "machine"
	}
	return fmt.Sprintf("level(%d)", int(l))
}

// Caps is what a driver can do.
type Caps struct {
	Name         string
	Level        Level
	OfflineForks bool // forks can start with no network at all
}

// Instance is a running sandbox, named by the driver.
type Instance string

// CheckpointID is the driver's own name for a saved state.
type CheckpointID string

// ExecRequest runs one command. Args are not passed through a shell.
type ExecRequest struct {
	Args    []string
	Stdin   []byte
	Dir     string   // working directory inside the sandbox; empty = the driver's default
	Env     []string // extra KEY=VALUE pairs
	Timeout time.Duration
}

// ExecResult is a finished command. A non-zero exit is a result, not an error.
type ExecResult struct {
	Stdout, Stderr []byte
	ExitCode       int
	TimedOut       bool
	Duration       time.Duration
}

// ErrUnsupported is returned by a call the driver can't do at its level.
var ErrUnsupported = errors.New("driver: not supported")

// Sandbox is one sandbox backend.
type Sandbox interface {
	// Caps reports the driver's level and whether its forks can run offline.
	Caps() Caps
	// Create starts a run's first sandbox from a template.
	Create(ctx context.Context, template string) (Instance, error)
	// Checkpoint saves the instance's state at a step boundary.
	Checkpoint(ctx context.Context, inst Instance) (CheckpointID, error)
	// Fork starts n instances from a checkpoint. With offline, they get no network.
	Fork(ctx context.Context, cp CheckpointID, n int, offline bool) ([]Instance, error)
	// Exec runs a command in the instance.
	Exec(ctx context.Context, inst Instance, req ExecRequest) (ExecResult, error)
	// Destroy stops the instance. Its checkpoints stay.
	Destroy(ctx context.Context, inst Instance) error
	// DeleteCheckpoint lets the core expire checkpoints it no longer needs.
	DeleteCheckpoint(ctx context.Context, cp CheckpointID) error
}
