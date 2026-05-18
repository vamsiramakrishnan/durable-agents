package chaos

import (
	"context"
	"fmt"
	"strings"

	tape "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go"
)

// ReplayReport — the result of one Replay call.
type ReplayReport struct {
	ScenarioName  string
	Seed          int64
	BitIdentical  bool
	SnapA, SnapB  *Snapshot
	DiffSummary   []string
	Notes         []string
}

func (r ReplayReport) String() string {
	verdict := "DRIFTED"
	if r.BitIdentical {
		verdict = "DETERMINISTIC"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "ReplayReport(%q: %s, seed=%d)", r.ScenarioName, verdict, r.Seed)
	if !r.BitIdentical {
		aLen := 0
		bLen := 0
		if r.SnapA != nil {
			aLen = len(r.SnapA.Lines)
		}
		if r.SnapB != nil {
			bLen = len(r.SnapB.Lines)
		}
		fmt.Fprintf(&b, "\n  journal lengths: %d vs %d", aLen, bLen)
		for i, ln := range r.DiffSummary {
			if i >= 10 {
				fmt.Fprintf(&b, "\n  ... and %d more", len(r.DiffSummary)-10)
				break
			}
			fmt.Fprintf(&b, "\n  - %s", ln)
		}
	}
	for _, n := range r.Notes {
		fmt.Fprintf(&b, "\n  ! %s", n)
	}
	return b.String()
}

func summarize(a, b Snapshot) []string {
	var out []string
	for _, d := range a.Diff(b) {
		switch d.Op {
		case "!=":
			out = append(out, fmt.Sprintf("[%d] %s differs:\n    A: %s\n    B: %s",
				d.Index, d.A.Kind, d.A.Payload, d.B.Payload))
		case ">":
			out = append(out, fmt.Sprintf("[%d] only in A: %s", d.Index, d.A.Kind))
		case "<":
			out = append(out, fmt.Sprintf("[%d] only in B: %s", d.Index, d.B.Kind))
		}
	}
	return out
}

// ReplayOpts — knobs for Replay.
type ReplayOpts struct {
	URL        string
	DeadlineMs int
}

// Replay — run `body(ctx, client, sess)` twice under `scen` with the
// same seed and check journal bit-identity. `body` must produce a run —
// either by returning its runID or calling sess.SetRunID.
//
// Never errors on divergence; reports it.
func Replay(ctx context.Context, scen Scenario,
	body func(ctx context.Context, client *tape.Client, sess *Session) (string, error),
	opts ReplayOpts,
) ReplayReport {
	url := opts.URL
	if url == "" {
		url = "tape://localhost:7878"
	}
	deadlineMs := opts.DeadlineMs
	if deadlineMs <= 0 {
		deadlineMs = 5000
	}
	rep := ReplayReport{ScenarioName: scen.Name, Seed: scen.Seed}
	snapshots := make([]Snapshot, 0, 2)

	for passIdx := 1; passIdx <= 2; passIdx++ {
		sess := NewSession(scen, SessionOpts{URL: url})
		if err := sess.Enter(ctx); err != nil {
			rep.Notes = append(rep.Notes, fmt.Sprintf("pass %d enter: %v", passIdx, err))
			return rep
		}
		client, err := tape.Dial(url)
		if err != nil {
			sess.Exit(ctx, err)
			rep.Notes = append(rep.Notes, fmt.Sprintf("pass %d dial: %v", passIdx, err))
			return rep
		}
		runID, bodyErr := body(ctx, client, sess)
		if runID == "" {
			runID = sess.RunID
		}
		if runID == "" {
			sess.Exit(ctx, bodyErr)
			client.Close()
			rep.Notes = append(rep.Notes,
				fmt.Sprintf("pass %d: body did not produce a runID", passIdx))
			return rep
		}
		snap, err := CaptureSnapshot(ctx, client, runID, CaptureSnapshotOpts{DeadlineMs: deadlineMs})
		client.Close()
		sess.Exit(ctx, bodyErr)
		if err != nil {
			rep.Notes = append(rep.Notes, fmt.Sprintf("pass %d capture: %v", passIdx, err))
			return rep
		}
		snapshots = append(snapshots, snap)
	}
	a, b := snapshots[0], snapshots[1]
	rep.SnapA, rep.SnapB = &a, &b
	rep.BitIdentical = a.Equals(b)
	if !rep.BitIdentical {
		rep.DiffSummary = summarize(a, b)
	}
	return rep
}
