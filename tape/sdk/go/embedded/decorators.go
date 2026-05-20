package embedded

// decorators.go — Go's answer to Python's `@effect` and `@outbox_tool`.
// We don't have decorators; we have higher-order constructors. The
// returned value (`*EffectTool` / `*OutboxTool`) carries the metadata
// the (future) ADK-Go plugin would read at call time.
//
// Construction-time refusal is the load-bearing property: if the
// caller asks for the NON-IDEMPOTENT + OUTBOX shape without a way to
// recover, we fail HERE — before any agent ever runs the tool.

import (
	"errors"
	"fmt"
)

// ToolFn — the user-supplied tool body. Args are JSON-coercible
// (typically the model's parsed function-call arguments). The first
// return value is the tool result; the second is the journaled error.
type ToolFn func(args map[string]any) (any, error)

// CustomKeyFn — derive an idempotency key from the tool args (overrides
// the default `<invocation>/decision-<n>/<tool>/<call>` shape).
type CustomKeyFn func(args map[string]any) string

// BusinessKeyFn — derive the cross-run dedup key for the OUTBOX path.
type BusinessKeyFn func(args map[string]any) (string, error)

// EffectMeta — metadata stamped on a tool by `Effect` / `OutboxTool`.
// Read it back via `MetaOf(tool)`.
type EffectMeta struct {
	Semantics         string
	DispatchMode      string
	BusinessKeyFn     BusinessKeyFn
	BusinessKeyStatic string
	Connector         string
	Compensate        string
	CustomKeyFn       CustomKeyFn
}

// ResolveBusinessKey — apply BusinessKeyStatic if set, else BusinessKeyFn.
// Returns ("", nil) when neither is configured.
func (m EffectMeta) ResolveBusinessKey(args map[string]any) (string, error) {
	if m.BusinessKeyStatic != "" {
		return m.BusinessKeyStatic, nil
	}
	if m.BusinessKeyFn != nil {
		return m.BusinessKeyFn(args)
	}
	return "", nil
}

// EffectTool — the value returned by `Effect(...)`. Carries the tool
// function plus its metadata; pass to your plugin / runner.
type EffectTool struct {
	Fn   ToolFn
	Meta EffectMeta
}

// Call — invoke the wrapped function. Convenience for callers that just
// want to run the body directly (replay / testing).
func (t *EffectTool) Call(args map[string]any) (any, error) { return t.Fn(args) }

// EffectOpts — options for `Effect`.
type EffectOpts struct {
	CustomKey CustomKeyFn
}

// Effect — wrap `fn` as an IDEMPOTENT + INLINE tool. The plugin records
// the intent before the call, the result after, and short-circuits on
// replay. The body MUST be safe to call multiple times (the upstream
// dedupes via the request's idempotency key, or the body is a no-op on
// repeat).
func Effect(fn ToolFn, opts ...EffectOpts) *EffectTool {
	var o EffectOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	return &EffectTool{
		Fn: fn,
		Meta: EffectMeta{
			Semantics:    EffectSemanticsIdempotent,
			DispatchMode: EffectDispatchInline,
			CustomKeyFn:  o.CustomKey,
		},
	}
}

// OutboxToolOpts — options for `OutboxTool`. `Connector` is required;
// exactly one of `BusinessKeyFn` / `BusinessKeyStatic` must be set.
type OutboxToolOpts struct {
	// BusinessKeyFn — derive the cross-run dedup key from the tool
	// args. Mutually exclusive with BusinessKeyStatic.
	BusinessKeyFn BusinessKeyFn

	// BusinessKeyStatic — for cases where the business key is constant
	// (rare). Mutually exclusive with BusinessKeyFn.
	BusinessKeyStatic string

	// Connector — the registry name the outbox dispatcher resolves at
	// runtime. Must match the `Name()` on a registered Connector.
	Connector string

	// Compensate — the obligation `kind` to register on duplicate
	// observation. Mirrors `compensate_on_duplicate_kind` in the proto.
	Compensate string

	// CustomKey — overrides the default idempotency-key derivation.
	CustomKey CustomKeyFn
}

// ErrOutboxToolConfig — returned by `OutboxTool` when the configuration
// is unsafe. Matches `Python tape_adk.decorators.outbox_tool`'s
// construction-time `ValueError`s.
var ErrOutboxToolConfig = errors.New("OutboxTool: invalid configuration")

// OutboxTool — wrap `fn` as a NON_IDEMPOTENT + OUTBOX tool. The plugin
// never lets the body run inline; it journals an intent and the outbox
// dispatcher (a separate reactor) makes the upstream call.
//
// Construction-time refusal — returns an error wrapping
// `ErrOutboxToolConfig` if:
//
//   - `Connector` is empty;
//   - both `BusinessKeyFn` and `BusinessKeyStatic` are zero;
//   - both `BusinessKeyFn` and `BusinessKeyStatic` are set
//     (ambiguous — pick one).
//
// The bug never makes it past compile-time-adjacent validation.
func OutboxTool(fn ToolFn, opts OutboxToolOpts) (*EffectTool, error) {
	if opts.Connector == "" {
		return nil, fmt.Errorf("%w: `Connector` is required — the outbox dispatcher "+
			"needs to know which connector to dispatch through", ErrOutboxToolConfig)
	}
	if opts.BusinessKeyFn == nil && opts.BusinessKeyStatic == "" {
		return nil, fmt.Errorf("%w: `BusinessKeyFn` or `BusinessKeyStatic` is required — "+
			"non-idempotent operations must declare the key the upstream uses to dedupe",
			ErrOutboxToolConfig)
	}
	if opts.BusinessKeyFn != nil && opts.BusinessKeyStatic != "" {
		return nil, fmt.Errorf("%w: set exactly one of `BusinessKeyFn` or `BusinessKeyStatic`",
			ErrOutboxToolConfig)
	}
	return &EffectTool{
		Fn: fn,
		Meta: EffectMeta{
			Semantics:         EffectSemanticsNonIdempotent,
			DispatchMode:      EffectDispatchOutbox,
			BusinessKeyFn:     opts.BusinessKeyFn,
			BusinessKeyStatic: opts.BusinessKeyStatic,
			Connector:         opts.Connector,
			Compensate:        opts.Compensate,
			CustomKeyFn:       opts.CustomKey,
		},
	}, nil
}

// MustOutboxTool — panic-on-misconfiguration helper for `init()` blocks
// where an unsafe configuration should crash the process immediately.
func MustOutboxTool(fn ToolFn, opts OutboxToolOpts) *EffectTool {
	t, err := OutboxTool(fn, opts)
	if err != nil {
		panic(err)
	}
	return t
}

// MetaOf — read the metadata off an `*EffectTool`. Returns the zero
// value if `t` is nil.
func MetaOf(t *EffectTool) EffectMeta {
	if t == nil {
		return EffectMeta{}
	}
	return t.Meta
}
