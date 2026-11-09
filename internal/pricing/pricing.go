// Package pricing turns token counts into dollars. Prices change often and differ
// by account, so none are built in: pass a JSON file, or send furcate.cost_usd on
// the span and the collector uses that instead.
//
//	{"claude-sonnet-5-5": {"input_per_mtok": 3.0, "output_per_mtok": 15.0}}
package pricing

import (
	"encoding/json"
	"os"
	"strings"
)

// Price is USD per million tokens.
type Price struct {
	InputPerMTok  float64 `json:"input_per_mtok"`
	OutputPerMTok float64 `json:"output_per_mtok"`
}

// Table maps a model name, or a model-name prefix, to its price.
type Table map[string]Price

// Load reads a price table. An empty path returns an empty table.
func Load(path string) (Table, error) {
	if path == "" {
		return Table{}, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	t := Table{}
	return t, json.Unmarshal(b, &t)
}

// Cost prices a call, matching the model exactly first, then by the longest
// prefix (so "claude-sonnet-5-5" covers "claude-sonnet-5-5-20261001"). An unknown
// model costs 0.
func (t Table) Cost(model string, in, out uint64) float64 {
	p, ok := t[model]
	if !ok {
		best := -1
		for k, v := range t {
			if strings.HasPrefix(model, k) && len(k) > best {
				p, best = v, len(k)
			}
		}
		if best < 0 {
			return 0
		}
	}
	return (float64(in)*p.InputPerMTok + float64(out)*p.OutputPerMTok) / 1e6
}
