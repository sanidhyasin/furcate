package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/sanidhyasin/furcate/internal/replay"
	"github.com/sanidhyasin/furcate/internal/runstore"
)

// Job statuses.
const (
	jobRunning = "running"
	jobDone    = "done"
	jobError   = "error"
)

// job is one fork running in the background. Its fields are guarded by Server.mu.
type job struct {
	id       string
	req      replay.Request
	status   string
	err      string
	started  time.Time
	finished time.Time
	res      *replay.Result
}

type branchResultJSON struct {
	RunID      string  `json:"run_id"`
	Status     string  `json:"status"`
	Outcome    string  `json:"outcome,omitempty"`
	Error      string  `json:"error,omitempty"`
	Steps      int     `json:"steps"`
	CostUSD    float64 `json:"cost_usd"`
	DurationMs float64 `json:"duration_ms"`
}

type forkResultJSON struct {
	Passes   int                `json:"passes"`
	Done     int                `json:"done"`
	Errors   int                `json:"errors"`
	Rate     float64            `json:"rate"`
	Lo       float64            `json:"lo"`
	Hi       float64            `json:"hi"`
	Usage    usageJSON          `json:"usage"`
	Branches []branchResultJSON `json:"branches"`
}

type jobJSON struct {
	ID          string          `json:"id"`
	RunID       string          `json:"run_id"`
	Step        int             `json:"step"`
	N           int             `json:"n"`
	Change      replay.Change   `json:"change"`
	ChangeLabel string          `json:"change_label"`
	Status      string          `json:"status"`
	Error       string          `json:"error,omitempty"`
	Started     time.Time       `json:"started"`
	Finished    *time.Time      `json:"finished,omitempty"`
	ElapsedMs   float64         `json:"elapsed_ms"`
	Result      *forkResultJSON `json:"result,omitempty"`
}

// toJSON snapshots a job; the caller holds s.mu.
func (j *job) toJSON() jobJSON {
	out := jobJSON{ID: j.id, RunID: j.req.RunID, Step: j.req.Step, N: j.req.N, Change: j.req.Change,
		Status: j.status, Error: j.err, Started: j.started}
	_, out.ChangeLabel = changeKey(mustJSON(j.req.Change))
	end := time.Now()
	if !j.finished.IsZero() {
		f := j.finished
		out.Finished, end = &f, f
	}
	out.ElapsedMs = ms(end.Sub(j.started))
	if res := j.res; res != nil {
		fr := &forkResultJSON{Passes: res.Passes, Done: res.Done, Lo: res.Lo, Hi: res.Hi, Usage: toUsage(res.Usage),
			Branches: make([]branchResultJSON, 0, len(res.Branches))}
		if res.Done > 0 {
			fr.Rate = float64(res.Passes) / float64(res.Done)
		}
		for _, b := range res.Branches {
			br := branchResultJSON{RunID: b.RunID, Status: b.Status, Outcome: b.Outcome, Steps: b.Steps,
				CostUSD: b.Usage.CostUSD, DurationMs: ms(b.Duration)}
			if b.Err != nil {
				br.Error = b.Err.Error()
			}
			if b.Status == runstore.StatusError {
				fr.Errors++
			}
			fr.Branches = append(fr.Branches, br)
		}
		out.Result = fr
	}
	return out
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// forkBody is POST /api/runs/{id}/fork.
type forkBody struct {
	Step   int           `json:"step"`
	N      int           `json:"n"`
	Change replay.Change `json:"change"`
}

func (s *Server) fork(w http.ResponseWriter, r *http.Request) error {
	if s.forker == nil {
		return errorf(http.StatusNotImplemented,
			"this server was started without a sandbox driver, so it can't fork; restart it with furcate serve -driver docker (or tine)")
	}
	id, err := pathID(r)
	if err != nil {
		return err
	}
	// A JSON content type also means a cross-origin page can't send this
	// without a CORS preflight, which the server never grants.
	if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != "application/json" {
		return errorf(http.StatusUnsupportedMediaType, "send the fork as Content-Type: application/json")
	}
	var body forkBody
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		return errorf(http.StatusBadRequest, "bad fork request: %v", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errorf(http.StatusBadRequest, "bad fork request: one JSON object expected")
	}
	if body.Step <= 0 {
		return errorf(http.StatusBadRequest, "step must be a step index of 1 or more")
	}
	if body.N < 1 || body.N > s.maxN {
		return errorf(http.StatusBadRequest, "n must be between 1 and %d", s.maxN)
	}

	// The engine checks all of this too, but only once the job has started;
	// checking here gives the form an immediate answer.
	ctx := r.Context()
	if _, err := s.runs.Run(ctx, id); errors.Is(err, runstore.ErrNotFound) {
		return errorf(http.StatusNotFound, "run %s not found in the run store", id)
	} else if err != nil {
		return err
	}
	step, err := s.runs.Step(ctx, id, body.Step)
	if errors.Is(err, runstore.ErrNotFound) {
		return errorf(http.StatusNotFound, "run %s has no step %d", id, body.Step)
	} else if err != nil {
		return err
	}
	if step.Kind != runstore.KindLLM || step.CheckpointID == "" {
		return errorf(http.StatusBadRequest, "step %d is a %s step without a checkpoint; fork from an LLM step", body.Step, step.Kind)
	}
	cp, err := s.runs.Checkpoint(ctx, step.CheckpointID)
	if errors.Is(err, runstore.ErrNotFound) {
		return errorf(http.StatusConflict, "step %d's checkpoint %s is not in the run store", body.Step, step.CheckpointID)
	} else if err != nil {
		return err
	}
	if cp.Deleted {
		return errorf(http.StatusConflict, "step %d's checkpoint %s was garbage-collected; it can't be forked", body.Step, cp.CheckpointID)
	}

	j, err := s.startJob(replay.Request{RunID: id, Step: body.Step, N: body.N, Change: body.Change})
	if err != nil {
		return err
	}
	s.mu.Lock()
	out := j.toJSON()
	s.mu.Unlock()
	w.Header().Set("Location", "/api/jobs/"+j.id)
	writeJSON(w, http.StatusAccepted, out)
	return nil
}

// startJob runs a fork in the background, under the server's context rather
// than the request's: the fork outlives the POST.
func (s *Server) startJob(req replay.Request) (*job, error) {
	j := &job{id: newID("job_"), req: req, status: jobRunning, started: time.Now()}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errorf(http.StatusServiceUnavailable, "server is shutting down")
	}
	s.jobs[j.id] = j
	s.order = append(s.order, j.id)
	s.evictLocked()
	s.wg.Add(1)
	s.mu.Unlock()

	s.log.Info("fork started", "job", j.id, "run", req.RunID, "step", req.Step, "n", req.N, "change", req.Change.String())
	go func() {
		defer s.wg.Done()
		res, err := s.runFork(req)
		s.mu.Lock()
		defer s.mu.Unlock()
		j.finished = time.Now()
		if err != nil {
			j.status, j.err = jobError, err.Error()
			s.log.Warn("fork failed", "job", j.id, "err", err)
			return
		}
		j.status, j.res = jobDone, res
		s.log.Info("fork done", "job", j.id, "passes", res.Passes, "done", res.Done)
	}()
	return j, nil
}

func (s *Server) runFork(req replay.Request) (res *replay.Result, err error) {
	defer func() {
		if p := recover(); p != nil {
			res, err = nil, fmt.Errorf("fork panicked: %v", p)
		}
	}()
	return s.forker.Fork(s.ctx, req)
}

// evictLocked drops the oldest finished jobs beyond maxJobs. Running jobs stay.
func (s *Server) evictLocked() {
	for i := 0; len(s.order) > s.maxJobs && i < len(s.order); {
		if j := s.jobs[s.order[i]]; j != nil && j.status == jobRunning {
			i++
			continue
		}
		delete(s.jobs, s.order[i])
		s.order = append(s.order[:i], s.order[i+1:]...)
	}
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	j, ok := s.jobs[id]
	var out jobJSON
	if ok {
		out = j.toJSON()
	}
	s.mu.Unlock()
	if !ok {
		return errorf(http.StatusNotFound, "job %s not found (jobs live in the server's memory and end with it)", id)
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}
