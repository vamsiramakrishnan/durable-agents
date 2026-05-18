// Package chaos — TapeChaos for Go. Mirrors `tape.chaos` from the
// Python SDK and `tape-ts/chaos` from the TS SDK. The shape: scenarios
// are declarative bundles of (faults, invariants, seed); faults target
// either the server's failpoint catalogue (Phase 0) or a registered
// connector; invariants are predicates over Tape's journal projections.
//
// See `design-principles/chaos.md` for the full design.
//
// Headline pattern:
//
//	import "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/chaos"
//
//	scen := chaos.Scenario{
//	    Name: "bank-wire-survives-crash",
//	    Seed: 42,
//	    Faults: []chaos.Fault{
//	        chaos.Crash("tape::begin_effect::post_db", chaos.WithAfterN(1)),
//	        chaos.LoseAck("bank.wire", 0.3),
//	    },
//	    Invariants: []chaos.Invariant{chaos.NoStuckObligations},
//	}
//	sess := chaos.NewSession(scen, chaos.SessionOpts{URL: "tape://127.0.0.1:7878"})
//	if err := sess.Enter(ctx); err != nil { ... }
//	defer sess.Exit(ctx, nil)
//	if err := runMyAgent(ctx, sess); err != nil { sess.Exit(ctx, err); ... }
//	fmt.Println(sess.Report)
package chaos

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"

	"github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/connectors"
	tape "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go"
)

// FaultLayer — the layer a Fault targets.
type FaultLayer string

const (
	LayerServer    FaultLayer = "server"
	LayerConnector FaultLayer = "connector"
)

// Fault — one declared chaos rule. Two layers: server (a named failpoint
// in the Rust catalogue) and connector (a wrap around a registered
// connector).
type Fault struct {
	Layer       FaultLayer
	Target      string // failpoint name OR connector name
	Action      string // panic/sleep/return (server) OR lose_ack/duplicate/delay (connector)
	Probability float64
	AfterN      int     // server only: skip first N hits
	Ms          int     // delay length
	Jitter      float64 // ± fraction
	When        string  // selector (Phase 2 CEL); recorded for the report
	ActionMsg   string  // for `error(msg=...)`
}

// FaultOpt — option for fault constructors.
type FaultOpt func(*Fault)

// WithProbability — set the per-hit firing probability (default 1.0).
func WithProbability(p float64) FaultOpt { return func(f *Fault) { f.Probability = p } }

// WithAfterN — fail-crate's `n*off->...` idiom: skip the first n hits.
func WithAfterN(n int) FaultOpt { return func(f *Fault) { f.AfterN = n } }

// WithJitter — ± fraction of the delay (only meaningful for delay/Delay).
func WithJitter(j float64) FaultOpt { return func(f *Fault) { f.Jitter = j } }

// WithWhen — free-form selector recorded for the report (Phase 2 CEL).
func WithWhen(w string) FaultOpt { return func(f *Fault) { f.When = w } }

// WithMsg — message for error(...) faults; ignored elsewhere.
func WithMsg(m string) FaultOpt { return func(f *Fault) { f.ActionMsg = m } }

// ── server-layer constructors ────────────────────────────────────────────

// Crash — a server failpoint configured to panic.
func Crash(failpoint string, opts ...FaultOpt) Fault {
	f := Fault{Layer: LayerServer, Target: failpoint, Action: "panic", Probability: 1.0}
	for _, o := range opts {
		o(&f)
	}
	return f
}

// Delay — a server failpoint configured to sleep(ms).
func Delay(failpoint string, ms int, opts ...FaultOpt) Fault {
	f := Fault{Layer: LayerServer, Target: failpoint, Action: "sleep", Probability: 1.0, Ms: ms}
	for _, o := range opts {
		o(&f)
	}
	return f
}

// Error — a server failpoint configured to return(msg).
func Error(failpoint string, opts ...FaultOpt) Fault {
	f := Fault{Layer: LayerServer, Target: failpoint, Action: "return", Probability: 1.0, ActionMsg: "chaos"}
	for _, o := range opts {
		o(&f)
	}
	return f
}

// ── connector-layer constructors ─────────────────────────────────────────

// LoseAck — the connector's Dispatch returns confirmed, but the wrapper
// flips it to unknown so the reconciler resolves via Observe.
func LoseAck(connector string, probability float64) Fault {
	return Fault{Layer: LayerConnector, Target: connector, Action: "lose_ack", Probability: probability}
}

// Duplicate — the connector's Observe returns duplicate instead of confirmed.
func Duplicate(connector string, probability float64) Fault {
	return Fault{Layer: LayerConnector, Target: connector, Action: "duplicate", Probability: probability}
}

// DelayConnector — sleep `ms ± jitter` before the connector's Dispatch.
func DelayConnector(connector string, ms int, opts ...FaultOpt) Fault {
	f := Fault{Layer: LayerConnector, Target: connector, Action: "delay", Probability: 1.0, Ms: ms}
	for _, o := range opts {
		o(&f)
	}
	return f
}

// ── Scenario + types ─────────────────────────────────────────────────────

// Scenario — a named bundle of faults + invariants + seed.
type Scenario struct {
	Name       string
	Faults     []Fault
	Invariants []Invariant
	Seed       int64
}

// InvariantResult — one invariant's outcome.
type InvariantResult struct {
	Name   string
	Passed bool
	Detail string
}

func (r InvariantResult) String() string {
	mark := "OK "
	if !r.Passed {
		mark = "FAIL"
	}
	if r.Detail != "" {
		return fmt.Sprintf("[%s] %s: %s", mark, r.Name, r.Detail)
	}
	return fmt.Sprintf("[%s] %s", mark, r.Name)
}

// Invariant — predicate over Tape's journal projections. The journal is
// the oracle.
type Invariant interface {
	Name() string
	Check(ctx context.Context, client *tape.Client, runID string) InvariantResult
}

// ── FAILPOINTS env rendering ─────────────────────────────────────────────

func toFailSpec(f Fault) string {
	action := f.Action
	switch action {
	case "sleep":
		action = fmt.Sprintf("sleep(%d)", f.Ms)
	case "return":
		msg := f.ActionMsg
		if msg == "" {
			msg = "chaos"
		}
		action = fmt.Sprintf("return(%s)", msg)
	case "print":
		msg := f.ActionMsg
		if msg == "" {
			msg = "chaos"
		}
		action = fmt.Sprintf("print(%s)", msg)
	}
	var parts []string
	if f.AfterN > 0 {
		parts = append(parts, fmt.Sprintf("%d*off", f.AfterN))
	}
	if f.Probability > 0 && f.Probability < 1.0 {
		parts = append(parts, fmt.Sprintf("%g*%s", f.Probability, action))
	} else {
		parts = append(parts, action)
	}
	return fmt.Sprintf("%s=%s", f.Target, strings.Join(parts, "->"))
}

// FailpointsEnv — render the server-layer faults to the FAILPOINTS env-var
// spec the chaos-feature tape-server parses at startup. Connector-layer
// faults are applied in-process via Session.Enter.
func FailpointsEnv(scen Scenario) string {
	var specs []string
	for _, f := range scen.Faults {
		if f.Layer == LayerServer {
			specs = append(specs, toFailSpec(f))
		}
	}
	return strings.Join(specs, ";")
}

// ── ChaosReport + Session ────────────────────────────────────────────────

// ChaosReport — the outcome of one scenario run.
type ChaosReport struct {
	ScenarioName     string
	Seed             int64
	FailpointsSpec   string
	Passed           bool
	InvariantResults []InvariantResult
	Notes            []string
}

func (r ChaosReport) String() string {
	verdict := "PASS"
	if !r.Passed {
		verdict = "FAIL"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "ChaosReport(%q: %s, seed=%d)", r.ScenarioName, verdict, r.Seed)
	for _, ir := range r.InvariantResults {
		fmt.Fprintf(&b, "\n  - %s", ir)
	}
	for _, n := range r.Notes {
		fmt.Fprintf(&b, "\n  ! %s", n)
	}
	return b.String()
}

// SessionOpts — Session configuration.
type SessionOpts struct {
	URL   string
	RunID string
	// Registry — the connector registry the session will wrap. Defaults
	// to `connectors.Default` if nil.
	Registry *connectors.Registry
}

// Session — the context: applies connector wraps on Enter, restores them
// on Exit, and runs invariants on Exit against the live journal.
type Session struct {
	Scenario       Scenario
	URL            string
	RunID          string
	FailpointsSpec string
	Report         ChaosReport
	rng            *rand.Rand
	registry       *connectors.Registry
	restores       []func()
	mu             sync.Mutex
}

// NewSession — construct (do not Enter).
func NewSession(scen Scenario, opts SessionOpts) *Session {
	url := opts.URL
	if url == "" {
		url = "tape://localhost:7878"
	}
	reg := opts.Registry
	if reg == nil {
		reg = connectors.Default
	}
	var r *rand.Rand
	if scen.Seed != 0 {
		r = rand.New(rand.NewPCG(uint64(scen.Seed), uint64(scen.Seed)^0xdead))
	} else {
		r = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	return &Session{
		Scenario:       scen,
		URL:            url,
		RunID:          opts.RunID,
		FailpointsSpec: FailpointsEnv(scen),
		Report: ChaosReport{
			ScenarioName:   scen.Name,
			Seed:           scen.Seed,
			FailpointsSpec: FailpointsEnv(scen),
			Passed:         true,
		},
		rng:      r,
		registry: reg,
	}
}

// SetRunID — late-bind the run identifier (for per-run invariants).
func (s *Session) SetRunID(id string) *Session {
	s.RunID = id
	return s
}

// Enter — apply connector wraps. Call before driving the agent.
func (s *Session) Enter(ctx context.Context) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	byTarget := map[string][]Fault{}
	for _, f := range s.Scenario.Faults {
		if f.Layer == LayerConnector {
			byTarget[f.Target] = append(byTarget[f.Target], f)
		}
	}
	for name, faults := range byTarget {
		real, err := s.registry.Get(name)
		if err != nil {
			s.Report.Notes = append(s.Report.Notes,
				fmt.Sprintf("connector fault for %q skipped: not registered", name))
			continue
		}
		wrapped := WrapConnector(real, faults, s.rng)
		s.registry.Replace(name, wrapped)
		realCopy := real
		nameCopy := name
		s.restores = append(s.restores, func() {
			s.registry.Replace(nameCopy, realCopy)
		})
	}
	return nil
}

// Exit — restore connectors and check invariants. Pass any error the
// scenario body raised; it's recorded in the report and the report's
// `Passed` flag goes false.
func (s *Session) Exit(ctx context.Context, thrown error) {
	s.mu.Lock()
	for i := len(s.restores) - 1; i >= 0; i-- {
		func() {
			defer func() { _ = recover() }()
			s.restores[i]()
		}()
	}
	s.restores = nil
	s.mu.Unlock()

	if thrown != nil {
		s.Report.Passed = false
		s.Report.Notes = append(s.Report.Notes, fmt.Sprintf("body raised: %v", thrown))
	}

	client, err := tape.Dial(s.URL)
	if err != nil {
		s.Report.Notes = append(s.Report.Notes, fmt.Sprintf("invariant client connect: %v", err))
		return
	}
	defer client.Close()

	for _, inv := range s.Scenario.Invariants {
		ir := safeCheck(ctx, inv, client, s.RunID)
		s.Report.InvariantResults = append(s.Report.InvariantResults, ir)
		if !ir.Passed {
			s.Report.Passed = false
		}
	}
}

func safeCheck(ctx context.Context, inv Invariant, client *tape.Client, runID string) (out InvariantResult) {
	defer func() {
		if r := recover(); r != nil {
			out = InvariantResult{Name: inv.Name(), Passed: false,
				Detail: fmt.Sprintf("check panicked: %v", r)}
		}
	}()
	return inv.Check(ctx, client, runID)
}

// RunScenario — drive `body(ctx, sess)` under `scen` and return the
// report. Convenience wrapper for the most common shape.
func RunScenario(ctx context.Context, scen Scenario,
	body func(context.Context, *Session) error, opts SessionOpts,
) ChaosReport {
	sess := NewSession(scen, opts)
	if err := sess.Enter(ctx); err != nil {
		sess.Report.Notes = append(sess.Report.Notes, fmt.Sprintf("enter: %v", err))
		return sess.Report
	}
	err := body(ctx, sess)
	sess.Exit(ctx, err)
	return sess.Report
}
