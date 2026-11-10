// Package pricing turns token counts into dollars. Defaults cover the current
// Claude models at Anthropic's published first-party rates; pass a JSON file to
// override or extend them, or send furcate.cost_usd on a span and the collector
// uses that instead.
//
//	{"claude-sonnet-5-5": {"input_per_mtok": 2.0, "output_per_mtok": 10.0,
//	                       "cache_read_per_mtok": 0.1, "cache_write_per_mtok": 2.5}}
package pricing

import (
	"encoding/json"
	"os"
	"strings"
)

// Price is USD per million tokens. Cache writes are the 5-minute rate.
type Price struct {
	InputPerMTok      float64 `json:"input_per_mtok"`
	OutputPerMTok     float64 `json:"output_per_mtok"`
	CacheReadPerMTok  float64 `json:"cache_read_per_mtok,omitempty"`
	CacheWritePerMTok float64 `json:"cache_write_per_mtok,omitempty"`
}

// Table maps a model name, or a model-name prefix, to its price.
type Table map[string]Price

// Defaults are Anthropic's first-party rates for the current Claude models, as
// listed on 2026-11-03. 5-minute cache writes are 1.25x input; cache reads for
// Haiku 5.5 assume the usual 0.1x. Check current prices before publishing a
// dollar figure.
func Defaults() Table {
	return Table{
		"claude-fable-5-2":  {InputPerMTok: 8, OutputPerMTok: 40, CacheReadPerMTok: 0.20, CacheWritePerMTok: 10},
		"claude-fable-5-1":  {InputPerMTok: 10, OutputPerMTok: 50, CacheReadPerMTok: 0.25, CacheWritePerMTok: 12.5},
		"claude-opus-5-5":   {InputPerMTok: 4, OutputPerMTok: 20, CacheReadPerMTok: 0.20, CacheWritePerMTok: 5},
		"claude-sonnet-5-5": {InputPerMTok: 2, OutputPerMTok: 10, CacheReadPerMTok: 0.10, CacheWritePerMTok: 2.5},
		"claude-haiku-5-5":  {InputPerMTok: 0.10, OutputPerMTok: 0.50, CacheReadPerMTok: 0.01, CacheWritePerMTok: 0.125},
	}
}

// Load reads a price table on top of the defaults. An empty path returns the
// defaults.
func Load(path string) (Table, error) {
	t := Defaults()
	if path == "" {
		return t, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var extra Table
	if err := json.Unmarshal(b, &extra); err != nil {
		return nil, err
	}
	for k, v := range extra {
		t[k] = v
	}
	return t, nil
}

func (t Table) lookup(model string) (Price, bool) {
	if p, ok := t[model]; ok {
		return p, true
	}
	best, found := -1, Price{}
	for k, v := range t {
		if strings.HasPrefix(model, k) && len(k) > best {
			found, best = v, len(k)
		}
	}
	return found, best >= 0
}

// Cost prices a call from its uncached input and output tokens, matching the
// model exactly first, then by the longest prefix (so "claude-sonnet-5-5" covers
// a dated variant). An unknown model costs 0.
func (t Table) Cost(model string, in, out uint64) float64 {
	return t.CostWithCache(model, in, out, 0, 0)
}

// CostWithCache also prices cache reads and cache writes. in is the uncached
// input, as the API's usage.input_tokens reports it.
func (t Table) CostWithCache(model string, in, out, cacheRead, cacheWrite uint64) float64 {
	p, ok := t.lookup(model)
	if !ok {
		return 0
	}
	return (float64(in)*p.InputPerMTok + float64(out)*p.OutputPerMTok +
		float64(cacheRead)*p.CacheReadPerMTok + float64(cacheWrite)*p.CacheWritePerMTok) / 1e6
}
