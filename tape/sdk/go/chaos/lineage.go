package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	tape "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go"
	"github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
)

// LineageNode — one row of the journal, plus its lineage edge and the
// failpoint that breaks it.
type LineageNode struct {
	Seq               int64
	Kind              string // "run" | "decision" | "effect" | "obligation" | "gate" | "value"
	Payload           map[string]any
	ParentSeq         int64 // 0 = root
	BreakingFailpoint string
}

// LineageGraph — the DAG of journal entries for one run.
type LineageGraph struct {
	RunID string
	Nodes []LineageNode
}

// OfKind — every node of the given kind.
func (g LineageGraph) OfKind(kind string) []LineageNode {
	var out []LineageNode
	for _, n := range g.Nodes {
		if n.Kind == kind {
			out = append(out, n)
		}
	}
	return out
}

// Edges — (parent_seq, child_seq) for every node with a non-zero parent.
func (g LineageGraph) Edges() [][2]int64 {
	var out [][2]int64
	for _, n := range g.Nodes {
		if n.ParentSeq > 0 {
			out = append(out, [2]int64{n.ParentSeq, n.Seq})
		}
	}
	return out
}

// MinimalCutsOpts — knobs for MinimalCuts.
type MinimalCutsOpts struct {
	MaxSize int // default 1: singletons (every node with a breaking failpoint)
}

// MinimalCuts — enumerate cuts up to `MaxSize`. At MaxSize=1 (default)
// every node with a breaking failpoint is its own cut.
func (g LineageGraph) MinimalCuts(opts MinimalCutsOpts) [][]LineageNode {
	maxSize := opts.MaxSize
	if maxSize == 0 {
		maxSize = 1
	}
	var candidates []LineageNode
	for _, n := range g.Nodes {
		if n.BreakingFailpoint != "" {
			candidates = append(candidates, n)
		}
	}
	cuts := make([][]LineageNode, 0, len(candidates))
	for _, n := range candidates {
		cuts = append(cuts, []LineageNode{n})
	}
	if maxSize >= 2 {
		for i := 0; i < len(candidates); i++ {
			for j := i + 1; j < len(candidates); j++ {
				if candidates[i].BreakingFailpoint == candidates[j].BreakingFailpoint {
					continue
				}
				cuts = append(cuts, []LineageNode{candidates[i], candidates[j]})
			}
		}
	}
	return cuts
}

// LineageFromRunOpts — knobs for LineageFromRun.
type LineageFromRunOpts struct {
	DeadlineMs int
}

// LineageFromRun — walk the run's journal via SubscribeRun and build
// the lineage DAG. Stops at the first terminal `run` entry (or deadline).
func LineageFromRun(ctx context.Context, client *tape.Client, runID string, opts LineageFromRunOpts) (LineageGraph, error) {
	deadlineMs := opts.DeadlineMs
	if deadlineMs <= 0 {
		deadlineMs = 5000
	}
	subCtx, cancel := context.WithTimeout(ctx, time.Duration(deadlineMs)*time.Millisecond)
	defer cancel()

	stream, err := client.PB().SubscribeRun(subCtx, &tapepb.SubscribeRunRequest{RunId: runID, FromSeq: 0})
	if err != nil {
		return LineageGraph{RunID: runID}, err
	}

	decisionSeqs := map[int64]int64{}
	effectSeqs := map[string]int64{}
	gateSeqs := map[string]int64{}
	var nodes []LineageNode

	for {
		entry, err := stream.Recv()
		if err != nil {
			break
		}
		var payload map[string]any
		_ = json.Unmarshal([]byte(entry.PayloadJson), &payload)
		var parent int64
		var bp string

		switch entry.Kind {
		case "run":
			status, _ := payload["status"].(string)
			if status == "running" {
				bp = "tape::begin_run::post_db"
			} else {
				bp = "tape::end_run::post_db"
			}
		case "decision":
			idxF, _ := payload["decision_index"].(float64)
			idx := int64(idxF)
			decisionSeqs[idx] = entry.Seq
			parent = decisionSeqs[idx-1]
			bp = "tape::record_decision::post_db"
		case "effect":
			idxF, _ := payload["decision_index"].(float64)
			parent = decisionSeqs[int64(idxF)]
			status, _ := payload["status"].(string)
			key, _ := payload["idempotency_key"].(string)
			if key != "" {
				switch strings.ToLower(status) {
				case "pending":
					if _, ok := effectSeqs[key]; !ok {
						effectSeqs[key] = entry.Seq
					}
					bp = "tape::begin_effect::post_db"
				case "confirmed":
					bp = "tape::complete_effect::post_db"
				case "failed", "unknown", "reconciled":
					bp = "tape::reconcile_effect::post_db"
				default:
					bp = "tape::begin_effect::post_db"
				}
			}
		case "obligation":
			effectKey, _ := payload["effect_key"].(string)
			parent = effectSeqs[effectKey]
			status, _ := payload["status"].(string)
			switch strings.ToLower(status) {
			case "compensated", "stuck":
				bp = "tape::resolve_obligation::post_db"
			default:
				bp = "tape::register_compensation::post_db"
			}
		case "gate":
			gate, _ := payload["gate"].(string)
			if gate != "" {
				if _, ok := gateSeqs[gate]; !ok {
					gateSeqs[gate] = entry.Seq
				}
			}
			status, _ := payload["status"].(string)
			switch strings.ToLower(status) {
			case "delivered", "resolved":
				bp = "tape::send_signal::post_db"
			default:
				bp = "tape::await_signal::post_db"
			}
		case "value":
			deleted, _ := payload["deleted"].(bool)
			if deleted {
				bp = "tape::delete_value::post_db"
			} else {
				bp = "tape::write_value::post_db"
			}
		}

		nodes = append(nodes, LineageNode{
			Seq: entry.Seq, Kind: entry.Kind,
			Payload: payload, ParentSeq: parent,
			BreakingFailpoint: bp,
		})

		if entry.Kind == "run" {
			status, _ := payload["status"].(string)
			if _, term := terminalRunStatuses[strings.ToLower(status)]; term {
				break
			}
		}
	}
	return LineageGraph{RunID: runID, Nodes: nodes}, nil
}

// DeriveScenariosOpts — knobs for DeriveScenarios.
type DeriveScenariosOpts struct {
	Invariants []Invariant
	MaxCutSize int
	BaseName   string // default: "ldfi"
}

// DeriveScenarios — translate every minimal cut of `g` into a `Scenario`
// whose `Crash(...)` targets the breaking failpoint with `AfterN: 1`.
func DeriveScenarios(g LineageGraph, opts DeriveScenariosOpts) []Scenario {
	baseName := opts.BaseName
	if baseName == "" {
		baseName = "ldfi"
	}
	cuts := g.MinimalCuts(MinimalCutsOpts{MaxSize: opts.MaxCutSize})
	out := make([]Scenario, 0, len(cuts))
	for _, cut := range cuts {
		var faults []Fault
		var names []string
		for _, node := range cut {
			faults = append(faults, Crash(node.BreakingFailpoint, WithAfterN(1)))
			names = append(names, fmt.Sprintf("%s@%d", node.Kind, node.Seq))
		}
		out = append(out, Scenario{
			Name:       fmt.Sprintf("%s::cut::%s", baseName, strings.Join(names, "+")),
			Faults:     faults,
			Invariants: opts.Invariants,
		})
	}
	return out
}

// LDFIReport — aggregate outcome of running every derived scenario.
type LDFIReport struct {
	BaselineRunID    string
	DerivedCount     int
	SurvivedCount    int
	BrokenScenarios  []BrokenScenario
}

// BrokenScenario — one scenario plus the invariants that failed under it.
type BrokenScenario struct {
	Name   string
	Failed []InvariantResult
}

// SurvivalRate — fraction of derived scenarios under which every invariant passed.
func (r LDFIReport) SurvivalRate() float64 {
	if r.DerivedCount == 0 {
		return 1.0
	}
	return float64(r.SurvivedCount) / float64(r.DerivedCount)
}

// LDFIRunAll — drive `runner(scen)` once per derived scenario; aggregate.
func LDFIRunAll(ctx context.Context, derived []Scenario,
	runner func(context.Context, Scenario) ([]InvariantResult, error),
	baselineRunID string,
) (LDFIReport, error) {
	rep := LDFIReport{BaselineRunID: baselineRunID, DerivedCount: len(derived)}
	for _, scen := range derived {
		results, err := runner(ctx, scen)
		if err != nil {
			rep.BrokenScenarios = append(rep.BrokenScenarios, BrokenScenario{
				Name:   scen.Name,
				Failed: []InvariantResult{{Name: "runner", Passed: false, Detail: err.Error()}},
			})
			continue
		}
		passed := true
		var failed []InvariantResult
		for _, ir := range results {
			if !ir.Passed {
				passed = false
				failed = append(failed, ir)
			}
		}
		if passed {
			rep.SurvivedCount++
		} else {
			rep.BrokenScenarios = append(rep.BrokenScenarios, BrokenScenario{Name: scen.Name, Failed: failed})
		}
	}
	return rep, nil
}
