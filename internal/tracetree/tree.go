// Package tracetree rebuilds a trace's tree from flat span rows at read time.
// Spans arrive out of order (a child ends, and is sent, before its parent), so the
// store keeps them flat and the tree only exists here.
package tracetree

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/sanidhyasin/furcate/internal/model"
)

// Node is a span and its children, sorted by start time.
type Node struct {
	Span     model.Span
	Children []*Node
	Orphan   bool // its parent span never arrived
}

// Tree is one trace.
type Tree struct {
	Roots []*Node
	Spans int
	Start time.Time
	End   time.Time
}

// Build links spans into a tree. A span whose parent is missing becomes a root
// marked Orphan, so a partial trace still renders.
func Build(spans []model.Span) *Tree {
	t := &Tree{Spans: len(spans)}
	if len(spans) == 0 {
		return t
	}
	nodes := make(map[string]*Node, len(spans))
	for _, s := range spans {
		nodes[s.SpanID] = &Node{Span: s}
	}
	t.Start, t.End = spans[0].Start, spans[0].End
	for _, n := range nodes {
		s := n.Span
		t.Start = minTime(t.Start, s.Start)
		t.End = maxTime(t.End, s.End)
		if s.IsRoot() {
			t.Roots = append(t.Roots, n)
			continue
		}
		if p, ok := nodes[s.ParentSpanID]; ok && p != n {
			p.Children = append(p.Children, n)
		} else {
			n.Orphan = true
			t.Roots = append(t.Roots, n)
		}
	}
	sortNodes(t.Roots)
	for _, n := range nodes {
		sortNodes(n.Children)
	}
	return t
}

func sortNodes(ns []*Node) {
	sort.SliceStable(ns, func(i, j int) bool {
		if !ns[i].Span.Start.Equal(ns[j].Span.Start) {
			return ns[i].Span.Start.Before(ns[j].Span.Start)
		}
		return ns[i].Span.SpanID < ns[j].Span.SpanID
	})
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// Totals sums the spans in the tree.
type Totals struct {
	TokensIn, TokensOut uint64
	CostUSD             float64
	Errors              int
	LLMCalls, ToolCalls int
}

// Totals walks every node.
func (t *Tree) Totals() Totals {
	var tot Totals
	var walk func([]*Node)
	walk = func(ns []*Node) {
		for _, n := range ns {
			s := n.Span
			tot.TokensIn += s.TokensIn
			tot.TokensOut += s.TokensOut
			tot.CostUSD += s.CostUSD
			if s.Status == model.StatusError {
				tot.Errors++
			}
			switch s.Kind {
			case model.KindLLM:
				tot.LLMCalls++
			case model.KindTool:
				tot.ToolCalls++
			}
			walk(n.Children)
		}
	}
	walk(t.Roots)
	return tot
}

// Render writes the tree as text: offset from the trace start, duration, kind,
// name and the numbers that matter for that kind of span.
func Render(w io.Writer, traceID string, t *Tree) {
	tot := t.Totals()
	fmt.Fprintf(w, "trace %s  %d spans  %s  %d llm / %d tool calls  tokens %d in / %d out  $%.4f",
		traceID, t.Spans, fmtDur(t.End.Sub(t.Start)), tot.LLMCalls, tot.ToolCalls, tot.TokensIn, tot.TokensOut, tot.CostUSD)
	if tot.Errors > 0 {
		fmt.Fprintf(w, "  %d errors", tot.Errors)
	}
	fmt.Fprintln(w)
	for i, n := range t.Roots {
		renderNode(w, t.Start, n, "", i == len(t.Roots)-1)
	}
}

func renderNode(w io.Writer, origin time.Time, n *Node, prefix string, last bool) {
	branch, next := "├─ ", "│  "
	if last {
		branch, next = "└─ ", "   "
	}
	s := n.Span
	var b strings.Builder
	fmt.Fprintf(&b, "%s%s%-5s %s", prefix, branch, s.Kind, s.Name)
	if s.Model != "" && !strings.Contains(s.Name, s.Model) {
		fmt.Fprintf(&b, " [%s]", s.Model)
	}
	fmt.Fprintf(&b, "  +%s %s", fmtDur(s.Start.Sub(origin)), fmtDur(s.Duration()))
	if s.TokensIn+s.TokensOut > 0 {
		fmt.Fprintf(&b, "  tok %d/%d", s.TokensIn, s.TokensOut)
	}
	if s.CostUSD > 0 {
		fmt.Fprintf(&b, "  $%.4f", s.CostUSD)
	}
	if cp := s.Attributes["furcate.checkpoint_id"]; cp != "" {
		fmt.Fprintf(&b, "  cp=%s", cp)
	}
	if s.Status == model.StatusError {
		b.WriteString("  ERROR")
		if s.StatusMessage != "" {
			fmt.Fprintf(&b, ": %s", oneLine(s.StatusMessage, 80))
		}
	}
	if n.Orphan {
		b.WriteString("  (parent missing)")
	}
	fmt.Fprintln(w, b.String())
	for i, c := range n.Children {
		renderNode(w, origin, c, prefix+next, i == len(n.Children)-1)
	}
}

func fmtDur(d time.Duration) string {
	switch {
	case d < 0:
		return "0s"
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

func oneLine(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
