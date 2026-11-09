package tracetree

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/sanidhyasin/furcate/internal/model"
)

func TestBuildOutOfOrder(t *testing.T) {
	t0 := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	// Children arrive before their parent, and one span's parent never arrives.
	spans := []model.Span{
		{SpanID: "tool", ParentSpanID: "root", Kind: "tool", Name: "bash", Start: at(3), End: at(4)},
		{SpanID: "llm", ParentSpanID: "root", Kind: "llm", Name: "chat", Model: "m", Start: at(1), End: at(3), TokensIn: 10, TokensOut: 5, CostUSD: 0.5},
		{SpanID: "lost", ParentSpanID: "gone", Kind: "tool", Name: "orphan", Start: at(5), End: at(6), Status: model.StatusError},
		{SpanID: "root", Kind: "agent", Name: "run", Start: at(0), End: at(10)},
	}
	tr := Build(spans)
	if len(tr.Roots) != 2 || tr.Roots[0].Span.SpanID != "root" || !tr.Roots[1].Orphan {
		t.Fatalf("roots: %+v", tr.Roots)
	}
	kids := tr.Roots[0].Children
	if len(kids) != 2 || kids[0].Span.SpanID != "llm" || kids[1].Span.SpanID != "tool" {
		t.Fatal("children must be sorted by start time")
	}
	tot := tr.Totals()
	if tot.LLMCalls != 1 || tot.ToolCalls != 2 || tot.Errors != 1 || tot.TokensIn != 10 {
		t.Fatalf("totals %+v", tot)
	}
	if tr.End.Sub(tr.Start) != 10*time.Second {
		t.Fatalf("span of trace %s", tr.End.Sub(tr.Start))
	}

	var buf bytes.Buffer
	Render(&buf, "abc", tr)
	out := buf.String()
	for _, want := range []string{"trace abc  4 spans", "└─ tool  orphan", "(parent missing)", "llm   chat [m]  +1.00s 2.00s  tok 10/5  $0.5000"} {
		if !strings.Contains(out, want) {
			t.Errorf("render is missing %q:\n%s", want, out)
		}
	}
}
