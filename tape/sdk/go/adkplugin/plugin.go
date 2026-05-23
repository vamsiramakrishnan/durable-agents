// Package adkplugin wires Tape's embedded effect ledger
// (`tape/sdk/go/embedded`) into a real ADK-Go runner via ADK-Go's plugin
// callbacks. It is the Go counterpart of the Python
// `tape_adk.NonIdempotentSafetyPlugin`.
//
// The contract this plugin adds to a vanilla ADK-Go runner:
//
//   - For tools built with `embedded.Effect` (IDEMPOTENT + INLINE): before
//     the tool runs, journal an intent (`BeginEffect`). After the tool
//     returns, complete the effect CONFIRMED with the result. On error:
//     FAILED, or UNKNOWN if the error is an `AckLost` (a recognised
//     "the upstream call landed but we couldn't confirm it" signal). On
//     replay — same `(invocation, decision, tool, call)` key — the intent
//     already exists; if it is CONFIRMED the recorded response is returned
//     and the tool body never runs.
//
//   - For tools built with `embedded.OutboxTool` (NON_IDEMPOTENT + OUTBOX):
//     never let the body run inline. Journal an OUTBOX intent and return a
//     `{"status":"pending", ...}` map that ADK records as the
//     function_response. The outbox dispatcher reactor resolves it later.
//
//   - For tools NOT registered with Tape: pass through untouched. The
//     plugin is opt-in per-tool.
//
// ADK-Go's `tool.Tool` is a bare interface (Name/Description/IsLongRunning)
// with no slot for arbitrary metadata, so the Tape decorators cannot stamp
// metadata onto the tool value the way the Python `@effect` decorator does.
// Instead `adkplugin` keeps a registry keyed by tool name: `Tool(...)`
// converts an `*embedded.EffectTool` into an ADK `tool.Tool` AND records its
// `embedded.EffectMeta` in a registry the plugin's callbacks read back at
// call time.
//
// This package is a SEPARATE Go module — see go.mod — so the ADK-Go
// dependency is optional for embedded-only users.
package adkplugin

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/genai"

	"google.golang.org/adk/agent"
	"google.golang.org/adk/agent/llmagent"
	"google.golang.org/adk/plugin"
	"google.golang.org/adk/tool"

	"github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/embedded"
)

// AckLost — return this (or an error that wraps it) from an inline
// `embedded.Effect` tool body to signal "the upstream call landed, but we
// could not confirm it". The plugin maps `AckLost` to
// `EffectStatusUnknown` on the effect ledger, which is what kicks the
// reconciler in. Tools that cannot distinguish UNKNOWN from FAILED don't
// need it — a plain error lands as FAILED.
//
// This mirrors `tape_adk.AckLost` in the Python SDK.
type AckLost struct {
	// Msg — human-readable detail journaled into the effect's error_json.
	Msg string
}

func (e *AckLost) Error() string {
	if e.Msg == "" {
		return "ack-lost: upstream call landed but could not be confirmed"
	}
	return "ack-lost: " + e.Msg
}

// IsAckLost — true if `err` is, or wraps, an `*AckLost`.
func IsAckLost(err error) bool {
	var a *AckLost
	return errors.As(err, &a)
}

// ── tool metadata registry ────────────────────────────────────────────────

// registry — process-wide map from ADK tool name → Tape effect metadata.
// Populated by `Tool` / `Register`, read by the plugin callbacks. ADK-Go's
// `tool.Tool` interface has no metadata slot, so a name-keyed registry is
// how the decorator's intent reaches the plugin.
var registry sync.Map // map[string]embedded.EffectMeta

// Register — record `meta` for the tool named `name`. Callers that build
// their own ADK `tool.Tool` (rather than going through `Tool`) use this to
// opt the tool into Tape journaling. Last write wins.
func Register(name string, meta embedded.EffectMeta) {
	registry.Store(name, meta)
}

// metaFor — look up the registered metadata for a tool name. The second
// return is false when the tool is not Tape-tracked (pass-through).
func metaFor(name string) (embedded.EffectMeta, bool) {
	v, ok := registry.Load(name)
	if !ok {
		return embedded.EffectMeta{}, false
	}
	return v.(embedded.EffectMeta), true
}

// ── plugin construction ───────────────────────────────────────────────────

// Option — functional option for `NewTapePlugin`.
type Option func(*config)

type config struct {
	name string
}

// WithName — override the plugin's name (default
// "tape_non_idempotent_safety"). ADK-Go uses the name only for logging /
// disambiguation.
func WithName(name string) Option {
	return func(c *config) { c.name = name }
}

// NewTapePlugin — build an ADK-Go `*plugin.Plugin` that journals effects
// into `svc`. Pass the returned plugin in `runner.Config.PluginConfig`.
//
// The plugin wires four ADK-Go callbacks:
//
//   - BeforeRunCallback  — per-invocation decision-index bookkeeping.
//   - BeforeToolCallback — journal an intent; short-circuit on a CONFIRMED
//     replay; return a pending map for OUTBOX tools.
//   - AfterToolCallback  — complete inline effects CONFIRMED; register a
//     compensation obligation if the tool declares one.
//   - OnToolErrorCallback — complete inline effects FAILED, or UNKNOWN for
//     an `AckLost` error.
func NewTapePlugin(svc *embedded.TapeSessionService, opts ...Option) (*plugin.Plugin, error) {
	if svc == nil {
		return nil, errors.New("adkplugin.NewTapePlugin: svc must not be nil")
	}
	cfg := config{name: "tape_non_idempotent_safety"}
	for _, o := range opts {
		o(&cfg)
	}
	tp := &tapePlugin{svc: svc}
	return plugin.New(plugin.Config{
		Name:                cfg.name,
		BeforeRunCallback:   tp.beforeRun,
		BeforeToolCallback:  tp.beforeTool,
		AfterToolCallback:   tp.afterTool,
		OnToolErrorCallback: tp.onToolError,
	})
}

// tapePlugin — holds the service handle and the per-invocation
// decision/call-index bookkeeping that keys effects deterministically.
//
// Effect key shape (identical to Python):
//
//	<invocationID>/decision-<decisionIndex>/<toolName>/<callIndex>
//
// `dnext` is the next decision index to hand out; `dlast` is the index of
// the decision currently authorising tool calls; `tcount` counts calls per
// (invocation, decision, tool) so two calls to the same tool in one
// decision get distinct keys. Mirrors Python's `_dnext`/`_dlast`/`_tcount`.
type tapePlugin struct {
	svc *embedded.TapeSessionService

	mu     sync.Mutex
	dnext  map[string]int    // invocationID → next decision index
	dlast  map[string]int    // invocationID → current decision index
	tcount map[callKey]int   // (inv, decision, tool) → next call index
	keys   map[string]string // effectStateKey → idempotency_key (for after/error)
}

type callKey struct {
	inv      string
	decision int
	tool     string
}

func (p *tapePlugin) ensureMaps() {
	if p.dnext == nil {
		p.dnext = map[string]int{}
		p.dlast = map[string]int{}
		p.tcount = map[callKey]int{}
		p.keys = map[string]string{}
	}
}

// ── before-run: decision-index bookkeeping ────────────────────────────────

// beforeRun — initialise per-invocation counters. ADK-Go calls this once
// before any tool callback for the same invocation.
//
// Unlike Python — which has a per-model-response `after_model_callback` to
// bump the decision index — ADK-Go's plugin surface gives us no equivalent
// hook that fires *between* a model response and its tool calls. So a
// single invocation is treated as one decision (index 0): every tool call
// in the invocation is keyed `decision-0`, and `callIndex` (per tool)
// disambiguates. A multi-decision invocation still produces distinct keys
// because the tool/callIndex tuple differs. This is the honest, documented
// limitation versus the Python plugin — see the package test.
func (p *tapePlugin) beforeRun(ictx agent.InvocationContext) (*genai.Content, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureMaps()
	inv := ictx.InvocationID()
	p.dnext[inv] = 1
	p.dlast[inv] = 0
	return nil, nil // nil content ⇒ do not short-circuit the run
}

// ── before-tool: journal the intent ───────────────────────────────────────

// beforeTool — journal an effect intent before the tool runs.
//
//   - Not Tape-tracked          → return (nil,nil): let the tool run.
//   - Existing CONFIRMED effect → return the recorded response: replay.
//   - Existing FAILED effect    → return a failure map: replay.
//   - Existing UNKNOWN effect   → return an "unknown" map; agent must not
//     re-act, the reconciler closes the loop.
//   - OUTBOX intent             → return a "pending" map: never run inline.
//   - PENDING + INLINE          → return (nil,nil): ADK runs the body,
//     `afterTool` completes the effect.
func (p *tapePlugin) beforeTool(
	ctx tool.Context, t tool.Tool, args map[string]any,
) (map[string]any, error) {
	meta, ok := metaFor(t.Name())
	if !ok {
		return nil, nil // not Tape-tracked — pass through
	}

	inv := ctx.InvocationID()

	p.mu.Lock()
	p.ensureMaps()
	decision := p.dlast[inv]
	ck := callKey{inv: inv, decision: decision, tool: t.Name()}
	callIndex := p.tcount[ck]
	p.tcount[ck] = callIndex + 1
	p.mu.Unlock()

	appName := ctx.AppName()
	userID := ctx.UserID()
	sessionID := ctx.SessionID()

	// Resolve the cross-run dedup key (OUTBOX tools only).
	businessKey, err := meta.ResolveBusinessKey(args)
	if err != nil {
		return map[string]any{
			"status": "failed",
			"error":  fmt.Sprintf("tape: business-key derivation failed: %v", err),
		}, nil
	}

	bctx := context.Background()
	if c, ok := ctx.(context.Context); ok {
		bctx = c
	}

	// Apply a custom idempotency key if the decorator declared one. A
	// custom key overrides the default `<inv>/decision-<n>/<tool>/<call>`
	// derivation — the deliberate escape hatch for tools whose effect
	// identity is an argument-derived business fact, not a position in the
	// journal (this is what makes cross-run replay deterministic).
	customKey := ""
	if meta.CustomKeyFn != nil {
		customKey = meta.CustomKeyFn(args)
	}

	eff, err := p.svc.BeginEffect(bctx, embedded.BeginEffectOpts{
		AppName:       appName,
		UserID:        userID,
		SessionID:     sessionID,
		InvocationID:  inv,
		DecisionIndex: decision,
		ToolName:      t.Name(),
		CallIndex:     callIndex,
		RequestJSON:   args,
		CustomKey:     customKey,
		Semantics:     meta.Semantics,
		DispatchMode:  meta.DispatchMode,
		BusinessKey:   businessKey,
		Connector:     meta.Connector,
	})
	if err != nil {
		// Server-side safety refusal (NI+inline, outbox-without-connector,
		// duplicate business_key). We cannot let the tool run, so surface
		// a failure map — ADK records it as the function_response.
		return map[string]any{
			"status": "failed",
			"error":  fmt.Sprintf("tape refused effect: %v", err),
		}, nil
	}

	// Stash the resolved idempotency key so afterTool / onToolError can
	// complete the right row. Keyed by (inv, tool, callIndex).
	p.mu.Lock()
	p.keys[stateKey(inv, t.Name(), callIndex)] = eff.IdempotencyKey
	p.mu.Unlock()
	// FunctionCallID is unique per call within an invocation — also stash
	// under it so a concurrent same-tool call can't collide.
	if fcid := ctx.FunctionCallID(); fcid != "" {
		p.mu.Lock()
		p.keys["fc:"+fcid] = eff.IdempotencyKey
		p.mu.Unlock()
	}

	switch eff.Status {
	case embedded.EffectStatusConfirmed:
		// Replay path — return the recorded response, skip the body.
		if m, ok := eff.ResponseJSON.(map[string]any); ok {
			return m, nil
		}
		return map[string]any{"tape_replay": eff.ResponseJSON}, nil
	case embedded.EffectStatusFailed:
		return map[string]any{
			"status":      "failed",
			"error":       eff.ErrorJSON,
			"tape_replay": true,
		}, nil
	case embedded.EffectStatusUnknown:
		// The reconciler has not resolved this yet. The agent must not
		// re-act; hand back a pending sentinel.
		return map[string]any{
			"status":          "unknown",
			"tape_replay":     true,
			"idempotency_key": eff.IdempotencyKey,
		}, nil
	}

	// OUTBOX intent — never run inline. The outbox dispatcher reactor will
	// execute the connector call and complete the effect out-of-band.
	if eff.DispatchMode == embedded.EffectDispatchOutbox {
		return map[string]any{
			"status":          "pending",
			"idempotency_key": eff.IdempotencyKey,
			"business_key":    businessKey,
			"note": "the outbox dispatcher will execute this call; the " +
				"result lands via a separate function_response",
		}, nil
	}

	// PENDING + INLINE — let ADK call the body; afterTool completes it.
	return nil, nil
}

// ── after-tool: complete the inline effect ─────────────────────────────────

// afterTool — record the tool's result onto the journaled effect and, if
// the tool declares a compensate kind, register the obligation.
//
// `err != nil` is handled by `onToolError`, not here (ADK-Go routes a tool
// error to OnToolErrorCallback). When `err == nil` we complete CONFIRMED.
func (p *tapePlugin) afterTool(
	ctx tool.Context, t tool.Tool, args, result map[string]any, err error,
) (map[string]any, error) {
	meta, ok := metaFor(t.Name())
	if !ok {
		return nil, nil
	}
	if meta.DispatchMode == embedded.EffectDispatchOutbox {
		// OUTBOX effects have no inline call — `result` is the pending map
		// from beforeTool. The dispatcher completes the effect.
		return nil, nil
	}
	if err != nil {
		// Routed via onToolError; nothing to do here.
		return nil, nil
	}

	effectKey := p.lookupKey(ctx, t.Name())
	if effectKey == "" {
		return nil, nil // no stashed key — beforeTool never journaled it
	}

	bctx := asContext(ctx)
	if _, cerr := p.svc.CompleteEffect(
		bctx, ctx.AppName(), ctx.UserID(), ctx.SessionID(), effectKey,
		embedded.EffectStatusConfirmed, result, nil,
	); cerr != nil {
		return nil, fmt.Errorf("tape: CompleteEffect failed: %w", cerr)
	}

	// Register a compensation obligation if the tool declares one.
	// Idempotent on (session, effect_key, kind).
	if meta.Compensate != "" {
		if _, rerr := p.svc.RegisterCompensation(bctx, embedded.RegisterCompensationOpts{
			AppName:      ctx.AppName(),
			UserID:       ctx.UserID(),
			SessionID:    ctx.SessionID(),
			InvocationID: ctx.InvocationID(),
			EffectKey:    effectKey,
			Kind:         meta.Compensate,
			PayloadJSON:  map[string]any{"args": args, "result": result},
		}); rerr != nil {
			return nil, fmt.Errorf("tape: RegisterCompensation failed: %w", rerr)
		}
	}
	return nil, nil
}

// ── on-tool-error: map the error to a terminal status ─────────────────────

// onToolError — map a tool error onto the effect ledger. An `AckLost`
// error (or one wrapping it) → UNKNOWN, which hands the effect to the
// reconciler. Any other error → FAILED.
func (p *tapePlugin) onToolError(
	ctx tool.Context, t tool.Tool, args map[string]any, err error,
) (map[string]any, error) {
	meta, ok := metaFor(t.Name())
	if !ok {
		return nil, nil
	}
	if meta.DispatchMode == embedded.EffectDispatchOutbox {
		return nil, nil
	}

	effectKey := p.lookupKey(ctx, t.Name())
	if effectKey == "" {
		return nil, nil
	}

	bctx := asContext(ctx)
	status := embedded.EffectStatusFailed
	errJSON := map[string]any{"type": "error", "message": errString(err)}
	if IsAckLost(err) {
		status = embedded.EffectStatusUnknown
		errJSON = map[string]any{"type": "AckLost", "message": errString(err)}
	}
	if _, cerr := p.svc.CompleteEffect(
		bctx, ctx.AppName(), ctx.UserID(), ctx.SessionID(), effectKey,
		status, nil, errJSON,
	); cerr != nil {
		return nil, fmt.Errorf("tape: CompleteEffect failed: %w", cerr)
	}
	return nil, nil
}

// ── helpers ────────────────────────────────────────────────────────────────

// lookupKey — recover the idempotency key beforeTool stashed. Prefers the
// FunctionCallID (collision-proof within an invocation) and falls back to
// the (inv, tool, callIndex) state key.
func (p *tapePlugin) lookupKey(ctx tool.Context, toolName string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if fcid := ctx.FunctionCallID(); fcid != "" {
		if k, ok := p.keys["fc:"+fcid]; ok {
			return k
		}
	}
	inv := ctx.InvocationID()
	// Without per-call FunctionCallID, take the most recent call for this
	// (inv, tool). tcount holds the *next* index, so the last is count-1.
	decision := p.dlast[inv]
	ck := callKey{inv: inv, decision: decision, tool: toolName}
	n := p.tcount[ck]
	if n == 0 {
		return ""
	}
	return p.keys[stateKey(inv, toolName, n-1)]
}

func stateKey(inv, tool string, callIndex int) string {
	return fmt.Sprintf("k:%s:%s:%d", inv, tool, callIndex)
}

// asContext — ADK-Go's tool.Context embeds context.Context; recover it for
// the embedded service's `ctx`-first method signatures.
func asContext(ctx tool.Context) context.Context {
	if c, ok := ctx.(context.Context); ok {
		return c
	}
	return context.Background()
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

var _ = llmagent.BeforeToolCallback(nil) // compile-time pin of the callback type
