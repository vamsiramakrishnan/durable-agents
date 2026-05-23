package embedded

// chaos.go — Chaos / fault-injection for the embedded (tape-adk-go) tier.
//
// Mirrors `tape_adk/chaos.py`: a connector-layer wrapper that decorates a
// `Connector` with declarative faults plus invariants that read the
// embedded SQL tables directly. No gRPC, no failpoints — the embedded
// tier has no separate server, so server-layer faults are not applicable.
//
// Single mechanism, three composable layers:
//
//   * `Fault`              — data describing one fault. Same shape as the
//                            language-portable wire format.
//   * `LoseAck` / `Duplicate` / `DelayConnector` — fault constructors.
//   * `ChaosConnector`     — the actual wrapper: implements `Connector`,
//                            decorates `Inner` with `Faults`. One fault
//                            → one decision point.
//   * `Invariant`          — predicate over the embedded journal.
//   * `NoStuckObligations` / `NoBlindNonIdempotentRetry` / `ExactlyOne`
//                          — read the tables, build a boolean answer.
//   * `Scenario`           — `(Name, Faults, Invariants, Seed, StrictFaults)`.
//   * `RunScenario(...)`   — one-shot orchestration: wrap connectors, run
//                            `body`, then check invariants against the live
//                            store and build a `ChaosReport`.
//
// Same logical schema as `tape_adk.chaos`; the wire format is the
// embedded SQL store. The orchestration ATOMICALLY validates that every
// declared connector-targeted fault has a connector to attach to
// (`StrictFaults=true`, the default), wraps them, runs `body`, and on
// exit runs every invariant. No silent-skip false positives.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"
)

// ── data: Fault + Scenario ────────────────────────────────────────────────

// FaultLayer — the layer a Fault targets. Only `connector` is meaningful
// in the embedded tier (the server layer requires the gRPC tier).
type FaultLayer string

const (
	// FaultLayerConnector — connector-wrap level fault.
	FaultLayerConnector FaultLayer = "connector"
)

// Fault — one declarative fault. The embedded module only consumes the
// connector layer.
type Fault struct {
	Layer       FaultLayer
	Target      string  // connector name when target-scoped
	Tool        string  // tool name when tool-scoped
	Action      string  // "lose_ack" | "duplicate" | "delay"
	Probability float64 // 1.0 → always; (0, 1) → rng-gated
	Ms          int     // delay length (action="delay")
	Jitter      float64 // ± fraction of Ms
}

// LoseAck — dispatch returns CONFIRMED → flipped to UNKNOWN. Pass
// `connector=` OR `tool=`, not both.
func LoseAck(connector, tool string, probability float64) (Fault, error) {
	if connector != "" && tool != "" {
		return Fault{}, errors.New("LoseAck: pass connector or tool, not both")
	}
	if connector == "" && tool == "" {
		return Fault{}, errors.New("LoseAck requires connector or tool")
	}
	if probability == 0 {
		probability = 0.3
	}
	return Fault{Layer: FaultLayerConnector, Target: connector, Tool: tool,
		Action: "lose_ack", Probability: probability}, nil
}

// MustLoseAck — panic-on-config helper for static scenario bundles.
func MustLoseAck(connector, tool string, probability float64) Fault {
	f, err := LoseAck(connector, tool, probability)
	if err != nil {
		panic(err)
	}
	return f
}

// Duplicate — observe() returns DUPLICATE — the reconciler should
// register a compensation.
func Duplicate(connector, tool string, probability float64) (Fault, error) {
	if connector != "" && tool != "" {
		return Fault{}, errors.New("Duplicate: pass connector or tool, not both")
	}
	if connector == "" && tool == "" {
		return Fault{}, errors.New("Duplicate requires connector or tool")
	}
	if probability == 0 {
		probability = 0.05
	}
	return Fault{Layer: FaultLayerConnector, Target: connector, Tool: tool,
		Action: "duplicate", Probability: probability}, nil
}

// MustDuplicate — panic-on-config helper.
func MustDuplicate(connector, tool string, probability float64) Fault {
	f, err := Duplicate(connector, tool, probability)
	if err != nil {
		panic(err)
	}
	return f
}

// DelayConnector — sleep `ms` (± `jitter` as a fraction) before
// dispatch.
func DelayConnector(connector string, ms int, jitter float64) Fault {
	return Fault{Layer: FaultLayerConnector, Target: connector,
		Action: "delay", Probability: 1.0, Ms: ms, Jitter: jitter}
}

// ── wrapper: ChaosConnector ───────────────────────────────────────────────

// ChaosConnector — a `Connector` that decorates `Inner` with `Faults`.
//
//   - `lose_ack`  — dispatch's CONFIRMED becomes UNKNOWN. The inner call
//     already landed; the wrapper hides the ack.
//   - `duplicate` — observe()'s result becomes DUPLICATE.
//   - `delay`     — dispatch sleeps `Ms` (± `Jitter`) before the inner call.
//
// A seeded `*rand.Rand` is the only mutable thread of nondeterminism;
// a seeded scenario is reproducible.
type ChaosConnector struct {
	Inner  Connector
	Faults []Fault

	mu  sync.Mutex
	rng *rand.Rand
}

// NewChaosConnector — wrap `inner` with `faults`. Pass a seeded rng for
// reproducibility, or nil for a random one.
func NewChaosConnector(inner Connector, faults []Fault, rng *rand.Rand) *ChaosConnector {
	if rng == nil {
		rng = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	return &ChaosConnector{Inner: inner, Faults: faults, rng: rng}
}

// Name — delegates to the inner connector.
func (c *ChaosConnector) Name() string {
	if c.Inner == nil {
		return ""
	}
	return c.Inner.Name()
}

// matching — pick the first matching fault for `kind`, possibly
// gated by probability. Tool-scoped faults match only when the effect's
// tool name matches.
func (c *ChaosConnector) matching(kind string, effect *EffectRecord) *Fault {
	for i := range c.Faults {
		f := &c.Faults[i]
		if f.Action != kind {
			continue
		}
		if f.Tool != "" && effect != nil {
			if effect.ToolName != f.Tool {
				continue
			}
		}
		if f.Probability >= 1.0 {
			return f
		}
		c.mu.Lock()
		r := c.rng.Float64()
		c.mu.Unlock()
		if r < f.Probability {
			return f
		}
	}
	return nil
}

// Dispatch — apply `delay` before inner; rewrite the result on `lose_ack`.
func (c *ChaosConnector) Dispatch(ctx context.Context, effect EffectRecord) (DispatchResult, error) {
	// delay → before inner.
	if d := c.matching("delay", &effect); d != nil && d.Ms > 0 {
		jitterFactor := 1.0
		if d.Jitter > 0 {
			c.mu.Lock()
			delta := (c.rng.Float64()*2 - 1) * d.Jitter
			c.mu.Unlock()
			jitterFactor = 1.0 + delta
		}
		wait := time.Duration(float64(d.Ms) * jitterFactor * float64(time.Millisecond))
		if wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return DispatchResult{}, ctx.Err()
			}
		}
	}

	result, err := c.Inner.Dispatch(ctx, effect)
	if err != nil {
		return result, err
	}

	// lose_ack → CONFIRMED → UNKNOWN (inner already wrote to the upstream).
	if result.Status == "confirmed" && c.matching("lose_ack", &effect) != nil {
		return DispatchResult{
			Status:      "unknown",
			ExternalRef: result.ExternalRef,
			Response:    result.Response,
			Error:       map[string]any{"reason": "embedded.chaos: simulated lost ack"},
		}, nil
	}
	return result, nil
}

// Observe — rewrite CONFIRMED → DUPLICATE on the `duplicate` fault.
func (c *ChaosConnector) Observe(ctx context.Context, effect EffectRecord) (ObservationResult, error) {
	result, err := c.Inner.Observe(ctx, effect)
	if err != nil {
		return result, err
	}
	if result.Status == "confirmed" && c.matching("duplicate", &effect) != nil {
		return ObservationResult{
			Status:         "duplicate",
			ExternalRef:    result.ExternalRef,
			Response:       result.Response,
			CompensateKind: result.CompensateKind,
		}, nil
	}
	return result, nil
}

// Compensate — passthrough; compensate() is the cleanup path; we don't
// decorate it.
func (c *ChaosConnector) Compensate(ctx context.Context, ob ObligationRecord) (CompensationResult, error) {
	return c.Inner.Compensate(ctx, ob)
}

// ── invariants ────────────────────────────────────────────────────────────

// InvariantResult — one invariant's outcome.
type InvariantResult struct {
	Name   string
	Passed bool
	Detail string
}

// String — readable form for reports.
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

// Invariant — predicate over the embedded journal.
type Invariant interface {
	InvariantName() string
	Check(ctx context.Context, svc *TapeSessionService) InvariantResult
}

// noStuckObligationsInv — singleton zero-arg invariant.
type noStuckObligationsInv struct{}

func (noStuckObligationsInv) InvariantName() string { return "no_stuck_obligations" }

func (noStuckObligationsInv) Check(ctx context.Context, svc *TapeSessionService) InvariantResult {
	q := svc.rew(`SELECT seq, kind FROM tape_obligations WHERE status = ?`)
	rows, err := svc.db.QueryContext(ctx, q, ObligationStatusStuck)
	if err != nil {
		return InvariantResult{Name: "no_stuck_obligations", Passed: false,
			Detail: fmt.Sprintf("query: %v", err)}
	}
	defer rows.Close()
	var hits []string
	var n int
	for rows.Next() {
		var seq int64
		var kind string
		if err := rows.Scan(&seq, &kind); err != nil {
			return InvariantResult{Name: "no_stuck_obligations", Passed: false,
				Detail: fmt.Sprintf("scan: %v", err)}
		}
		n++
		if len(hits) < 5 {
			hits = append(hits, fmt.Sprintf("seq=%d kind=%s", seq, kind))
		}
	}
	if err := rows.Err(); err != nil {
		return InvariantResult{Name: "no_stuck_obligations", Passed: false,
			Detail: fmt.Sprintf("rows: %v", err)}
	}
	if n == 0 {
		return InvariantResult{Name: "no_stuck_obligations", Passed: true,
			Detail: "0 stuck"}
	}
	return InvariantResult{Name: "no_stuck_obligations", Passed: false,
		Detail: fmt.Sprintf("%d stuck: %s", n, strings.Join(hits, ", "))}
}

// NoStuckObligations — invariant: no obligation is in STUCK status.
var NoStuckObligations Invariant = noStuckObligationsInv{}

// noBlindNonIdempotentRetryInv — proves the safety contract for outbox dispatch.
type noBlindNonIdempotentRetryInv struct{}

func (noBlindNonIdempotentRetryInv) InvariantName() string { return "no_blind_non_idempotent_retry" }

func (noBlindNonIdempotentRetryInv) Check(ctx context.Context, svc *TapeSessionService) InvariantResult {
	q := svc.rew(`SELECT COUNT(*) FROM tape_effects
WHERE semantics = ? AND status = ? AND dispatch_attempts > 1`)
	var n int
	if err := svc.db.QueryRowContext(ctx, q,
		EffectSemanticsNonIdempotent, EffectStatusPending).Scan(&n); err != nil {
		return InvariantResult{Name: "no_blind_non_idempotent_retry", Passed: false,
			Detail: fmt.Sprintf("query: %v", err)}
	}
	if n == 0 {
		return InvariantResult{Name: "no_blind_non_idempotent_retry", Passed: true,
			Detail: "0 violators"}
	}
	return InvariantResult{Name: "no_blind_non_idempotent_retry", Passed: false,
		Detail: fmt.Sprintf("%d NON_IDEMPOTENT effects retried while PENDING", n)}
}

// NoBlindNonIdempotentRetry — invariant: a NON_IDEMPOTENT + OUTBOX
// effect should never reach `dispatch_attempts > 1` while still
// PENDING — the contract says `next_dispatch_at_ms = 0` flips it to
// UNKNOWN for the reconciler instead.
var NoBlindNonIdempotentRetry Invariant = noBlindNonIdempotentRetryInv{}

// exactlyOneInv — parameterised: exactly one CONFIRMED effect matches.
type exactlyOneInv struct {
	connector string
	tool      string
}

func (e exactlyOneInv) InvariantName() string {
	if e.connector != "" {
		return fmt.Sprintf("exactly_one(connector=%q)", e.connector)
	}
	if e.tool != "" {
		return fmt.Sprintf("exactly_one(tool=%q)", e.tool)
	}
	return "exactly_one"
}

func (e exactlyOneInv) Check(ctx context.Context, svc *TapeSessionService) InvariantResult {
	name := e.InvariantName()
	q := `SELECT COUNT(*) FROM tape_effects WHERE status = ?`
	args := []any{EffectStatusConfirmed}
	if e.connector != "" {
		q += ` AND connector = ?`
		args = append(args, e.connector)
	}
	if e.tool != "" {
		q += ` AND tool_name = ?`
		args = append(args, e.tool)
	}
	var n int
	if err := svc.db.QueryRowContext(ctx, svc.rew(q), args...).Scan(&n); err != nil {
		return InvariantResult{Name: name, Passed: false,
			Detail: fmt.Sprintf("query: %v", err)}
	}
	if n == 1 {
		return InvariantResult{Name: name, Passed: true, Detail: "1 confirmed"}
	}
	return InvariantResult{Name: name, Passed: false,
		Detail: fmt.Sprintf("%d confirmed (expected 1)", n)}
}

// ExactlyOne — invariant factory: exactly one CONFIRMED effect matches
// the filter. Pass `connector=` OR `tool=`, not both.
func ExactlyOne(connector, tool string) (Invariant, error) {
	if connector != "" && tool != "" {
		return nil, errors.New("ExactlyOne: pass connector or tool, not both")
	}
	if connector == "" && tool == "" {
		return nil, errors.New("ExactlyOne requires connector or tool")
	}
	return exactlyOneInv{connector: connector, tool: tool}, nil
}

// MustExactlyOne — panic-on-config helper.
func MustExactlyOne(connector, tool string) Invariant {
	inv, err := ExactlyOne(connector, tool)
	if err != nil {
		panic(err)
	}
	return inv
}

// ── scenario + session ────────────────────────────────────────────────────

// Scenario — a named bundle of faults + invariants + seed.
//
// `StrictFaults=true` (default): a connector-targeted fault whose target
// isn't in the `Connectors` map FAILS the scenario instead of silently
// passing — same mechanism as the gRPC SDK. The silent-skip false
// positive is the bug both versions share until this guard fires.
type Scenario struct {
	Name         string
	Faults       []Fault
	Invariants   []Invariant
	Seed         uint64
	StrictFaults *bool // pointer so the zero-value default is `true`
}

// strict — resolve StrictFaults to its effective value (default true).
func (s Scenario) strict() bool {
	if s.StrictFaults == nil {
		return true
	}
	return *s.StrictFaults
}

// StrictFaultsOff — convenience helper for `StrictFaults: chaos.StrictFaultsOff()`.
func StrictFaultsOff() *bool { v := false; return &v }

// StrictFaultsOn — convenience helper, the default.
func StrictFaultsOn() *bool { v := true; return &v }

// ChaosReport — outcome of one scenario run.
type ChaosReport struct {
	ScenarioName     string
	Seed             uint64
	Passed           bool
	InvariantResults []InvariantResult
	Notes            []string
}

// String — readable form.
func (r ChaosReport) String() string {
	verdict := "pass"
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

// ChaosSession — the wrapped connectors + the report shell that
// `RunScenario` / `OpenChaosSession` produces. Yielding a small struct
// instead of a bare map lets the caller read the report after `Close`.
type ChaosSession struct {
	Connectors map[string]Connector
	Report     *ChaosReport

	svc        *TapeSessionService
	scenario   Scenario
	ownedSvc   bool
	ownedDB    *sql.DB
	finalized  bool
	finalizeMu sync.Mutex
}

// OpenChaosSessionOpts — `OpenChaosSession` options.
type OpenChaosSessionOpts struct {
	// Connectors — the dict of connectors the reactor loop will dispatch
	// through. Required. The returned `ChaosSession.Connectors` map
	// contains wrapped versions where faults targeted them.
	Connectors map[string]Connector

	// Svc — reuse a caller-owned `*TapeSessionService`. Set this OR
	// SvcDBURL — not both.
	Svc *TapeSessionService

	// SvcDBURL — when non-empty, open a fresh SQLite DB at this DSN for
	// the invariant checks. Use for one-shot scenario runs where the
	// caller doesn't already own a service.
	//
	// Pass `:memory:` for an ephemeral in-memory DB.
	SvcDBURL string
}

// OpenChaosSession — wrap `connectors` with the scenario's faults; return
// a `*ChaosSession`. Callers MUST call `Close` to run invariants and
// finalise the report.
//
// The mechanism:
//
//  1. validate every connector-targeted fault has a target in
//     `connectors` — under StrictFaults=true (default) a missing target
//     FAILS the scenario at this point, not silently;
//  2. wrap each targeted connector in a `ChaosConnector` with the
//     relevant faults bound to a seeded `*rand.Rand`;
//  3. return the wrapped map + a report shell;
//  4. on `Close`, run every invariant against the live store and
//     finalize the report.
func OpenChaosSession(ctx context.Context, scen Scenario, opts OpenChaosSessionOpts) (*ChaosSession, error) {
	if opts.Svc == nil && opts.SvcDBURL == "" {
		return nil, errors.New("OpenChaosSession: pass Svc or SvcDBURL")
	}
	if opts.Svc != nil && opts.SvcDBURL != "" {
		return nil, errors.New("OpenChaosSession: pass Svc OR SvcDBURL, not both")
	}
	report := &ChaosReport{
		ScenarioName: scen.Name,
		Seed:         scen.Seed,
		Passed:       true,
	}
	wrapped := map[string]Connector{}
	for k, v := range opts.Connectors {
		wrapped[k] = v
	}

	rng := rand.New(rand.NewPCG(scen.Seed, scen.Seed^0xdeadbeef))

	byConnector := map[string][]Fault{}
	var toolScoped []Fault
	for _, f := range scen.Faults {
		if f.Layer != FaultLayerConnector {
			recordSkip(report, scen,
				fmt.Sprintf("fault layer %q not supported in embedded tier "+
					"(server failpoints require the gRPC tier)", f.Layer))
			continue
		}
		switch {
		case f.Target != "":
			byConnector[f.Target] = append(byConnector[f.Target], f)
		case f.Tool != "":
			toolScoped = append(toolScoped, f)
		default:
			recordSkip(report, scen,
				"connector fault skipped: neither target nor tool set")
		}
	}

	for name, faults := range byConnector {
		inner, ok := opts.Connectors[name]
		if !ok {
			recordSkip(report, scen,
				fmt.Sprintf("connector fault for %q skipped: connector not in `connectors` dict", name))
			continue
		}
		combined := append([]Fault{}, faults...)
		combined = append(combined, toolScoped...)
		wrapped[name] = NewChaosConnector(inner, combined, rng)
	}
	if len(toolScoped) > 0 {
		if len(opts.Connectors) == 0 {
			recordSkip(report, scen,
				"tool-scoped fault(s) skipped: empty `connectors` dict")
		}
		for name, inner := range opts.Connectors {
			if _, already := byConnector[name]; already {
				continue
			}
			wrapped[name] = NewChaosConnector(inner, toolScoped, rng)
		}
	}

	// Resolve svc — either the caller's or a fresh one against SvcDBURL.
	svc := opts.Svc
	var ownedDB *sql.DB
	owned := false
	if svc == nil {
		dsn := opts.SvcDBURL
		if dsn == ":memory:" {
			dsn = "file:chaos?mode=memory&cache=shared"
		}
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			return nil, fmt.Errorf("OpenChaosSession: open %q: %w", dsn, err)
		}
		if err := CreateAllTables(ctx, db, CreateAllOpts{
			Dialect:          DialectSQLite,
			WithoutSessionFK: true,
		}); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("OpenChaosSession: create tables: %w", err)
		}
		svc = NewTapeSessionService(ctx, db, DialectSQLite)
		owned = true
		ownedDB = db
	} else {
		// Ensure tables exist on the caller's svc — invariant queries
		// would fail on an empty DB otherwise.
		if err := CreateAllTables(ctx, svc.db, CreateAllOpts{
			Dialect:          svc.dialect,
			WithoutSessionFK: true,
		}); err != nil {
			return nil, fmt.Errorf("OpenChaosSession: ensure tables: %w", err)
		}
	}

	return &ChaosSession{
		Connectors: wrapped,
		Report:     report,
		svc:        svc,
		scenario:   scen,
		ownedSvc:   owned,
		ownedDB:    ownedDB,
	}, nil
}

// Close — run every invariant against the live store, finalize the
// report. Idempotent — calling twice is safe. If the session owns its
// `*sql.DB`, the underlying connection is closed.
func (s *ChaosSession) Close(ctx context.Context) {
	s.finalizeMu.Lock()
	if s.finalized {
		s.finalizeMu.Unlock()
		return
	}
	s.finalized = true
	s.finalizeMu.Unlock()

	for _, inv := range s.scenario.Invariants {
		ir := safeInvariantCheck(ctx, inv, s.svc)
		s.Report.InvariantResults = append(s.Report.InvariantResults, ir)
		if !ir.Passed {
			s.Report.Passed = false
		}
	}
	if s.ownedSvc && s.ownedDB != nil {
		_ = s.ownedDB.Close()
	}
}

func safeInvariantCheck(ctx context.Context, inv Invariant, svc *TapeSessionService) (out InvariantResult) {
	defer func() {
		if r := recover(); r != nil {
			out = InvariantResult{Name: inv.InvariantName(), Passed: false,
				Detail: fmt.Sprintf("check panicked: %v", r)}
		}
	}()
	return inv.Check(ctx, svc)
}

// RunScenario — one-shot orchestration: open a session, run `body` with
// the wrapped connectors, close the session, return the report.
//
// `body` may be nil — for pure-invariant scenarios that don't drive
// anything.
func RunScenario(
	ctx context.Context,
	scen Scenario,
	body func(ctx context.Context, connectors map[string]Connector) error,
	opts OpenChaosSessionOpts,
) (*ChaosReport, error) {
	sess, err := OpenChaosSession(ctx, scen, opts)
	if err != nil {
		return nil, err
	}
	defer sess.Close(ctx)
	if body != nil {
		if err := body(ctx, sess.Connectors); err != nil {
			sess.Report.Passed = false
			sess.Report.Notes = append(sess.Report.Notes,
				fmt.Sprintf("body raised: %v", err))
		}
	}
	// Force Close NOW so the report has invariant results populated
	// before we return it; the deferred Close is then a no-op via the
	// idempotent guard.
	sess.Close(ctx)
	return sess.Report, nil
}

// recordSkip — a declared fault couldn't be applied. Always notes; under
// StrictFaults, also fails the scenario via a synthetic `strict_faults`
// invariant result. Same mechanism as the SDK fix.
func recordSkip(report *ChaosReport, scen Scenario, message string) {
	report.Notes = append(report.Notes, message)
	if scen.strict() {
		report.InvariantResults = append(report.InvariantResults, InvariantResult{
			Name: "strict_faults", Passed: false, Detail: message,
		})
		report.Passed = false
	}
}
