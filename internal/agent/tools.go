package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go/v2"

	"github.com/sanidhyasin/furcate/internal/driver"
)

// MaxToolOutput caps what one tool call returns to the model.
const MaxToolOutput = 16 << 10

// Tools returns the tool definitions sent with every request. They never change
// during a run: a changed tool list would break the cached prefix and invalidate
// earlier thinking blocks.
func Tools() []anthropic.ToolUnionParam {
	bash := anthropic.ToolParam{
		Name:        "bash",
		Description: anthropic.String("Run a bash command in the sandbox's working directory. Returns the exit code, stdout and stderr."),
		InputSchema: anthropic.ToolInputSchemaParam{
			Properties: map[string]any{
				"command": map[string]any{"type": "string", "description": "The command to run."},
			},
			Required: []string{"command"},
		},
	}
	write := anthropic.ToolParam{
		Name:        "write_file",
		Description: anthropic.String("Write a file in the sandbox, creating parent directories. Overwrites an existing file."),
		InputSchema: anthropic.ToolInputSchemaParam{
			Properties: map[string]any{
				"path":    map[string]any{"type": "string", "description": "Path, relative to the working directory or absolute."},
				"content": map[string]any{"type": "string", "description": "The file's full contents."},
			},
			Required: []string{"path", "content"},
		},
	}
	return []anthropic.ToolUnionParam{{OfTool: &bash}, {OfTool: &write}}
}

// ToolCall is one tool_use block to run.
type ToolCall struct {
	ID    string
	Name  string
	Input json.RawMessage
}

// ToolResult is what the model gets back.
type ToolResult struct {
	Output  string
	IsError bool // the call itself failed: bad input, unknown tool, driver error
	Exit    int
}

// RunTool executes one call in the sandbox. Only a driver failure returns an
// error; a bad call or a failing command is a result the model sees.
func RunTool(ctx context.Context, sb driver.Sandbox, inst driver.Instance, workdir string, timeout time.Duration, call ToolCall) (ToolResult, error) {
	switch call.Name {
	case "bash":
		var in struct {
			Command string `json:"command"`
		}
		if err := json.Unmarshal(call.Input, &in); err != nil || in.Command == "" {
			return ToolResult{Output: "invalid input: want {\"command\": string}", IsError: true}, nil
		}
		res, err := sb.Exec(ctx, inst, driver.ExecRequest{
			Args: []string{"bash", "-lc", in.Command}, Dir: workdir, Timeout: timeout,
		})
		if err != nil {
			return ToolResult{}, err
		}
		return ToolResult{Output: formatExec(res), Exit: res.ExitCode}, nil

	case "write_file":
		var in struct {
			Path    string `json:"path"`
			Content string `json:"content"`
		}
		if err := json.Unmarshal(call.Input, &in); err != nil || in.Path == "" {
			return ToolResult{Output: "invalid input: want {\"path\": string, \"content\": string}", IsError: true}, nil
		}
		res, err := sb.Exec(ctx, inst, driver.ExecRequest{
			Args:  []string{"sh", "-c", `mkdir -p "$(dirname "$1")" && cat > "$1"`, "sh", in.Path},
			Stdin: []byte(in.Content), Dir: workdir, Timeout: timeout,
		})
		if err != nil {
			return ToolResult{}, err
		}
		if res.ExitCode != 0 {
			return ToolResult{Output: formatExec(res), IsError: true, Exit: res.ExitCode}, nil
		}
		return ToolResult{Output: fmt.Sprintf("wrote %d bytes to %s", len(in.Content), in.Path)}, nil
	}
	return ToolResult{Output: "unknown tool " + call.Name, IsError: true}, nil
}

func formatExec(r driver.ExecResult) string {
	var b strings.Builder
	if r.TimedOut {
		b.WriteString("timed out\n")
	}
	fmt.Fprintf(&b, "exit code %d\n", r.ExitCode)
	if len(r.Stdout) > 0 {
		b.WriteString("stdout:\n")
		b.Write(r.Stdout)
		if !strings.HasSuffix(string(r.Stdout), "\n") {
			b.WriteByte('\n')
		}
	}
	if len(r.Stderr) > 0 {
		b.WriteString("stderr:\n")
		b.Write(r.Stderr)
	}
	s := b.String()
	if len(s) > MaxToolOutput {
		s = s[:MaxToolOutput] + fmt.Sprintf("\n[truncated %d bytes]", len(s)-MaxToolOutput)
	}
	return s
}
