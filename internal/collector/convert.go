package collector

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/sanidhyasin/furcate/internal/blob"
	"github.com/sanidhyasin/furcate/internal/model"
	"github.com/sanidhyasin/furcate/internal/pricing"
)

// Attribute keys the converter reads. The gen_ai.* ones follow the OpenTelemetry
// GenAI semantic conventions; furcate.* ones are set by our own SDK.
const (
	attrOperation     = "gen_ai.operation.name"
	attrRequestModel  = "gen_ai.request.model"
	attrResponseModel = "gen_ai.response.model"
	attrInputTokens   = "gen_ai.usage.input_tokens"
	attrOutputTokens  = "gen_ai.usage.output_tokens"
	attrPromptTokens  = "gen_ai.usage.prompt_tokens"     // older convention
	attrComplTokens   = "gen_ai.usage.completion_tokens" // older convention
	attrRunID         = "furcate.run_id"
	attrStepKind      = "furcate.step.kind"
	attrCost          = "furcate.cost_usd"
	attrServiceName   = "service.name"
)

// payloadKeys are always moved to the blob store, whatever their size: these are
// what replay needs byte for byte. furcate.llm.request is the full request body
// (messages, tools, parameters) the SDK records so a conversation can be rebuilt.
var payloadKeys = map[string]bool{
	"gen_ai.input.messages":      true,
	"gen_ai.output.messages":     true,
	"gen_ai.system_instructions": true,
	"gen_ai.tool.definitions":    true,
	"gen_ai.tool.call.arguments": true,
	"gen_ai.tool.call.result":    true,
	"gen_ai.prompt":              true, // older convention
	"gen_ai.completion":          true, // older convention
	"furcate.llm.request":        true,
	"furcate.llm.response":       true,
}

// Converter lowers OTLP spans into rows. Payloads become hashes on the row, and
// their bytes ride along in NewPayloads unless Seen says they are already stored.
type Converter struct {
	Seen      *blob.Seen
	Prices    pricing.Table
	Threshold int // string attributes longer than this become payloads too

	payloads, payloadBytes atomic.Uint64 // every payload seen, before dedupe
}

// Convert turns one OTLP export request into span rows tagged with walSeq.
func (c *Converter) Convert(req *coltracepb.ExportTraceServiceRequest, walSeq uint64) ([]model.Span, error) {
	var out []model.Span
	for _, rs := range req.GetResourceSpans() {
		service := ""
		for _, kv := range rs.GetResource().GetAttributes() {
			if kv.GetKey() == attrServiceName {
				service = kv.GetValue().GetStringValue()
			}
		}
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				row, err := c.convertSpan(sp, service)
				if err != nil {
					return nil, err
				}
				row.WALSeq = walSeq
				out = append(out, row)
			}
		}
	}
	return out, nil
}

func (c *Converter) convertSpan(sp *tracepb.Span, service string) (model.Span, error) {
	if len(sp.GetTraceId()) != 16 || len(sp.GetSpanId()) != 8 {
		return model.Span{}, fmt.Errorf("span %q: trace id must be 16 bytes and span id 8", sp.GetName())
	}
	row := model.Span{
		TraceID:    hex.EncodeToString(sp.GetTraceId()),
		SpanID:     hex.EncodeToString(sp.GetSpanId()),
		Service:    service,
		Name:       sp.GetName(),
		Start:      time.Unix(0, int64(sp.GetStartTimeUnixNano())).UTC(),
		End:        time.Unix(0, int64(sp.GetEndTimeUnixNano())).UTC(),
		Payloads:   map[string]string{},
		Attributes: map[string]string{},
	}
	if len(sp.GetParentSpanId()) == 8 {
		row.ParentSpanID = hex.EncodeToString(sp.GetParentSpanId())
	}

	switch sp.GetStatus().GetCode() {
	case tracepb.Status_STATUS_CODE_ERROR:
		row.Status = model.StatusError
	case tracepb.Status_STATUS_CODE_OK:
		row.Status = model.StatusOK
	default:
		row.Status = model.StatusUnset
	}
	row.StatusMessage = sp.GetStatus().GetMessage()

	var costSet bool
	for _, kv := range sp.GetAttributes() {
		key, val := kv.GetKey(), kv.GetValue()
		switch key {
		case attrRequestModel:
			if row.Model == "" {
				row.Model = val.GetStringValue()
			}
		case attrResponseModel:
			row.Model = val.GetStringValue() // the model that actually answered wins
		case attrInputTokens, attrPromptTokens:
			row.TokensIn = uint64(max(intValue(val), 0))
		case attrOutputTokens, attrComplTokens:
			row.TokensOut = uint64(max(intValue(val), 0))
		case attrRunID:
			row.RunID = val.GetStringValue()
		case attrCost:
			row.CostUSD, costSet = floatValue(val), true
		}
		c.putAttr(&row, key, val)
	}

	// Span events: exceptions are worth keeping as attributes; GenAI content
	// events (the older way to send prompts) become payloads.
	for i, ev := range sp.GetEvents() {
		for _, kv := range ev.GetAttributes() {
			key := fmt.Sprintf("event.%d.%s.%s", i, ev.GetName(), kv.GetKey())
			c.putAttr(&row, key, kv.GetValue())
		}
	}

	row.Kind = classify(row.Attributes)
	if row.RunID == "" {
		row.RunID = row.TraceID // each run is one trace
	}
	if !costSet && row.Kind == model.KindLLM {
		row.CostUSD = c.Prices.Cost(row.Model, row.TokensIn, row.TokensOut)
	}
	return row, nil
}

// putAttr keeps an attribute on the row, or makes it a payload if it is a known
// payload key or a string longer than the threshold.
func (c *Converter) putAttr(row *model.Span, key string, val *commonpb.AnyValue) {
	s := stringify(val)
	if !payloadKeys[key] && (c.Threshold <= 0 || len(s) <= c.Threshold) {
		row.Attributes[key] = s
		return
	}
	data := []byte(s)
	hash := blob.Hash(data)
	row.Payloads[key] = hash
	c.payloads.Add(1)
	c.payloadBytes.Add(uint64(len(data)))
	if c.Seen != nil && c.Seen.Has(hash) {
		return
	}
	for _, p := range row.NewPayloads {
		if p.Hash == hash {
			return
		}
	}
	row.NewPayloads = append(row.NewPayloads, model.Payload{Hash: hash, Data: data})
}

// classify maps a span to llm, tool, agent or other. furcate.step.kind wins;
// otherwise gen_ai.operation.name decides.
func classify(attrs map[string]string) string {
	if k := attrs[attrStepKind]; k != "" {
		return k
	}
	switch attrs[attrOperation] {
	case "chat", "text_completion", "generate_content", "embeddings":
		return model.KindLLM
	case "execute_tool":
		return model.KindTool
	case "invoke_agent", "create_agent":
		return model.KindAgent
	}
	return model.KindOther
}

func intValue(v *commonpb.AnyValue) int64 {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_IntValue:
		return x.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return int64(x.DoubleValue)
	case *commonpb.AnyValue_StringValue:
		n, _ := strconv.ParseInt(x.StringValue, 10, 64)
		return n
	}
	return 0
}

func floatValue(v *commonpb.AnyValue) float64 {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_DoubleValue:
		return x.DoubleValue
	case *commonpb.AnyValue_IntValue:
		return float64(x.IntValue)
	case *commonpb.AnyValue_StringValue:
		f, _ := strconv.ParseFloat(x.StringValue, 64)
		return f
	}
	return 0
}

// stringify renders any OTLP value as a string: scalars as themselves, arrays and
// maps as JSON.
func stringify(v *commonpb.AnyValue) string {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(x.BytesValue)
	case *commonpb.AnyValue_ArrayValue, *commonpb.AnyValue_KvlistValue:
		b, _ := json.Marshal(toGo(v))
		return string(b)
	}
	return ""
}

func toGo(v *commonpb.AnyValue) any {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		return x.BoolValue
	case *commonpb.AnyValue_IntValue:
		return x.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return x.DoubleValue
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(x.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		out := make([]any, 0, len(x.ArrayValue.GetValues()))
		for _, e := range x.ArrayValue.GetValues() {
			out = append(out, toGo(e))
		}
		return out
	case *commonpb.AnyValue_KvlistValue:
		out := make(map[string]any, len(x.KvlistValue.GetValues()))
		for _, kv := range x.KvlistValue.GetValues() {
			out[kv.GetKey()] = toGo(kv.GetValue())
		}
		return out
	}
	return nil
}
