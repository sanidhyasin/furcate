// Package bisect finds the step where a run went wrong. It measures the success
// rate of continuations started from different steps: a sharp drop between two
// checkpointed steps marks the bad decision, and a binary search keeps this to
// about log2(steps) probes.
//
// Agents are random, so every probe is N continuations with a Wilson interval,
// and a step is only called good or bad when its interval separates from the
// other side's. An ambiguous probe gets more samples before the search moves on,
// and the whole search stops when its budget runs out.
package bisect

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sanidhyasin/furcate/internal/driver"
	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
	"github.com/sanidhyasin/furcate/internal/stats"
)

// Config bounds one bisect.
type Config struct {
	RunID   string
	N       int     // continuations per probe; default 10 (5 is too few: 4/5 and 0/5 overlap)
	MaxRuns int     // budget in continuations; default 100
	MaxUSD  float64 // budget in dollars; 0 = no dollar cap
	// Change applies to every continuation. Usually none: bisect asks where the
	// run went wrong as it was.
	Change replay.Change
	Log    func(format string, args ...any)
}

// Probe is the continuations run from one step.
type Probe struct {
	Step         int
	Passes, Done int
	Errors       int
	Lo, Hi       float64
	Usage        runstore.Usage
	BranchRunIDs []string
	rate         float64
}

// Rate is the pass rate.
func (p *Probe) Rate() float64 { return p.rate }

func (p *Probe) add(r *replay.Result) {
	for _, b := range r.Branches {
		p.BranchRunIDs = append(p.BranchRunIDs, b.RunID)
		if b.Status != runstore.StatusDone {
			p.Errors++
		}
	}
	p.Passes += r.Passes
	p.Done += r.Done
	p.Usage = p.Usage.Add(r.Usage)
	p.Lo, p.Hi = stats.Wilson(p.Passes, p.Done, stats.Z95)
	if p.Done > 0 {
		p.rate = float64(p.Passes) / float64(p.Done)
	}
}

// above reports that a's interval lies wholly above b's.
func above(a, b *Probe) bool { return a.Lo > b.Hi }

// Result is what bisect found and what it cost.
type Result struct {
	RunID     string
	Steps     []int // the run's checkpointed LLM steps
	Probes    []*Probe
	LastGood  *Probe // continuations from here mostly pass
	FirstBad  *Probe // from here they mostly fail
	Faulty    int    // the LLM step whose decision breaks the run; 0 if not found
	Confident bool   // every call along the way had separated intervals
	Verdict   string
	Runs      int
	Usage     runstore.Usage
	Duration  time.Duration
	Original  runstore.Usage // the run being bisected, for "how many reruns' worth"
}

// Run bisects cfg.RunID.
func Run(ctx context.Context, eng *replay.Engine, cfg Config) (*Result, error) {
	if cfg.N <= 0 {
		cfg.N = 10
	}
	if cfg.MaxRuns <= 0 {
		cfg.MaxRuns = 100
	}
	logf := cfg.Log
	if logf == nil {
		logf = func(string, ...any) {}
	}
	start := time.Now()
	run, err := eng.Store.Run(ctx, cfg.RunID)
	if err != nil {
		return nil, err
	}
	all, err := eng.Store.Steps(ctx, cfg.RunID)
	if err != nil {
		return nil, err
	}
	res := &Result{RunID: cfg.RunID, Original: run.Usage, Confident: true}
	for _, s := range all {
		if s.Kind == runstore.KindLLM && s.CheckpointID != "" {
			res.Steps = append(res.Steps, s.Index)
		}
	}
	if len(res.Steps) < 2 {
		return nil, fmt.Errorf("run %s has %d checkpointed steps; bisect needs at least 2", cfg.RunID, len(res.Steps))
	}

	probes := map[int]*Probe{}
	errBudget := errors.New("budget spent")
	probe := func(step, n int) (*Probe, error) {
		if left := cfg.MaxRuns - res.Runs; n > left {
			n = left
		}
		if n <= 0 || (cfg.MaxUSD > 0 && res.Usage.CostUSD >= cfg.MaxUSD) {
			return nil, errBudget
		}
		logf("probe step %d: %d continuations", step, n)
		r, err := eng.Fork(ctx, replay.Request{RunID: cfg.RunID, Step: step, N: n, Change: cfg.Change})
		if err != nil {
			return nil, err
		}
		p := probes[step]
		if p == nil {
			p = &Probe{Step: step}
			probes[step] = p
			res.Probes = append(res.Probes, p)
		}
		p.add(r)
		res.Runs += len(r.Branches)
		res.Usage = res.Usage.Add(r.Usage)
		logf("  step %d: %d/%d pass (%.2f-%.2f), $%.2f so far", step, p.Passes, p.Done, p.Lo, p.Hi, res.Usage.CostUSD)
		return p, nil
	}
	finish := func(verdict string) (*Result, error) {
		res.Verdict = verdict
		res.Duration = time.Since(start)
		return res, nil
	}

	first, last := res.Steps[0], res.Steps[len(res.Steps)-1]
	good, err := probe(first, cfg.N)
	if err != nil {
		return nil, err
	}
	bad, err := probe(last, cfg.N)
	if err != nil {
		return nil, err
	}
	if !above(good, bad) { // one more round on both ends before giving up
		if _, err := probe(first, cfg.N); err == nil {
			_, err = probe(last, cfg.N)
		}
		if err != nil && !errors.Is(err, errBudget) {
			return nil, err
		}
	}
	if !above(good, bad) {
		res.Confident = false
		if good.Hi < 0.5 {
			v := fmt.Sprintf("continuations fail even from step %d (%d/%d pass): the task or prompt is the problem, not one step",
				first, good.Passes, good.Done)
			if run.Status == runstore.StatusDone && eng.Controller.Driver.Caps().Level < driver.LevelMachine {
				v += fmt.Sprintf("; or the task needs running processes, which the %s driver doesn't restore in forks (it saves %s only)",
					eng.Controller.Driver.Caps().Name, eng.Controller.Driver.Caps().Level)
			}
			return finish(v)
		}
		return finish(fmt.Sprintf("no clear drop: from step %d %d/%d pass, from step %d %d/%d; the outcome looks random rather than decided by one step",
			first, good.Passes, good.Done, last, bad.Passes, bad.Done))
	}

	lo, hi := 0, len(res.Steps)-1
	for hi-lo > 1 {
		mid := (lo + hi) / 2
		p, err := probe(res.Steps[mid], cfg.N)
		if errors.Is(err, errBudget) {
			res.LastGood, res.FirstBad = good, bad
			res.Confident = false
			return finish(fmt.Sprintf("budget ran out: the faulty decision is between step %d and step %d", res.Steps[lo], res.Steps[hi]))
		}
		if err != nil {
			return nil, err
		}
		isGood, isBad := above(p, bad), above(good, p)
		if isGood == isBad { // ambiguous: sample more before deciding
			if _, err := probe(res.Steps[mid], cfg.N); err != nil && !errors.Is(err, errBudget) {
				return nil, err
			}
			isGood, isBad = above(p, bad), above(good, p)
		}
		if isGood == isBad { // still ambiguous: go by the point estimate
			res.Confident = false
			isGood = p.Rate() >= (good.Rate()+bad.Rate())/2
		}
		if isGood {
			lo, good = mid, p
		} else {
			hi, bad = mid, p
		}
	}
	res.LastGood, res.FirstBad, res.Faulty = good, bad, res.Steps[lo]
	return finish(fmt.Sprintf("the decision at step %d breaks the run: from step %d, %d/%d continuations pass (%.2f-%.2f); from step %d, %d/%d (%.2f-%.2f)",
		res.Steps[lo], good.Step, good.Passes, good.Done, good.Lo, good.Hi, bad.Step, bad.Passes, bad.Done, bad.Lo, bad.Hi))
}
