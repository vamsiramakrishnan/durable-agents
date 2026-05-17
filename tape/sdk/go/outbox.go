package tape

// Outbox — the high-level surface for *non-idempotent* upstreams in Go.
//
// In Python this is `@tape.outbox_tool(...)`; in Go we expose a builder
// (`NewOutboxTool`) that returns an `*OutboxTool`. The user calls
// `tool.Envelope(payload)` to produce the JSON-serialisable intent the
// outbox reactor will dispatch via the named connector.
//
// Rules — enforced at construction time, not at runtime:
//
//   * `Semantics == OutboxNonIdempotent` MUST declare at least one of
//     `BusinessKey`, `StatusCheck`, `Compensate`, or `HumanGate=true`.
//
//   * The body builds an intent value only; it MUST NOT perform IO. The
//     Go type system can't enforce "no IO", but the Envelope() shape
//     pushes you toward pure assembly.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// OutboxSemantics — the semantics tag on an outbox tool.
type OutboxSemantics string

const (
	OutboxIdempotent    OutboxSemantics = "idempotent"
	OutboxAtLeastOnce   OutboxSemantics = "at_least_once"
	OutboxNonIdempotent OutboxSemantics = "non_idempotent"
)

// OutboxConfigError — returned by NewOutboxTool when the configuration is
// unsafe (typically: non_idempotent without any recovery path).
type OutboxConfigError struct{ Reason string }

func (e *OutboxConfigError) Error() string { return "tape.outbox: " + e.Reason }

// IsOutboxConfigError — type-check helper.
func IsOutboxConfigError(err error) bool {
	var t *OutboxConfigError
	return errors.As(err, &t)
}

// OutboxToolOpts — configuration for `NewOutboxTool`.
type OutboxToolOpts struct {
	// Name — the tool name as it appears in the journal / model API.
	Name string

	// Connector — the registered capability connector name (see
	// `tape/connectors`). E.g. "bank.wire".
	Connector string

	// Semantics — defaults to OutboxIdempotent.
	Semantics OutboxSemantics

	// BusinessKey — given the intent payload, derive a stable key the
	// counterparty can dedup on (typically a hash of the irreducible
	// identifying fields).
	BusinessKey func(payload map[string]any) (string, error)

	// StatusCheck — registered with `RegisterStatusCheck(Name, fn)`; the
	// reconciler invokes it to resolve UNKNOWN effects.
	StatusCheck func(ctx context.Context, idempotencyKey string) (StatusCheckResult, error)

	// Compensate — registered with `RegisterCompensator(<name>, fn)`; the
	// compensation reactor runs it for duplicate dispatches.
	Compensate func(ctx context.Context, payload []byte) error

	// WaitForResult — when true, the ADK runner parks the call until the
	// dispatch resolves (the Go adapter consumes this hint).
	WaitForResult bool

	// HumanGate — when true, the run parks on a gate before the outbox
	// dispatches. Required for non_idempotent tools that have no other
	// safety net.
	HumanGate bool

	// DispatchTimeoutMs — soft deadline the connector should honour (0 =
	// connector default).
	DispatchTimeoutMs int64

	// MaxAttempts — outbox-reactor retry budget (server default 5 when 0).
	MaxAttempts int
}

// OutboxTool — the value returned by `NewOutboxTool`. Use `Envelope(...)`
// from a tool body to produce the intent payload the runner will return.
type OutboxTool struct {
	Opts OutboxToolOpts
}

// OutboxEnvelope — the JSON-serialisable intent the runner returns. The
// outbox reactor recognises envelopes by the `__outbox__: true` sentinel.
type OutboxEnvelope struct {
	Outbox            bool           `json:"__outbox__"`
	Connector         string         `json:"connector"`
	Tool              string         `json:"tool"`
	Semantics         string         `json:"semantics"`
	WaitForResult     bool           `json:"wait_for_result"`
	HumanGate         bool           `json:"human_gate"`
	DispatchTimeoutMs int64          `json:"dispatch_timeout_ms,omitempty"`
	BusinessKey       string         `json:"business_key,omitempty"`
	Payload           map[string]any `json:"payload"`
}

// NewOutboxTool — build a validated outbox tool. Returns
// `*OutboxConfigError` for unsafe configurations.
func NewOutboxTool(opts OutboxToolOpts) (*OutboxTool, error) {
	if opts.Name == "" {
		return nil, &OutboxConfigError{Reason: "Name is required"}
	}
	if opts.Connector == "" {
		return nil, &OutboxConfigError{Reason: "Connector is required"}
	}
	if opts.Semantics == "" {
		opts.Semantics = OutboxIdempotent
	}
	switch opts.Semantics {
	case OutboxIdempotent, OutboxAtLeastOnce, OutboxNonIdempotent:
		// ok
	default:
		return nil, &OutboxConfigError{Reason: fmt.Sprintf("unknown semantics %q", opts.Semantics)}
	}
	if opts.Semantics == OutboxNonIdempotent {
		if opts.BusinessKey == nil && opts.StatusCheck == nil && opts.Compensate == nil && !opts.HumanGate {
			return nil, &OutboxConfigError{Reason: "non_idempotent tools must declare at least one of " +
				"BusinessKey, StatusCheck, Compensate, or HumanGate=true — otherwise an UNKNOWN " +
				"dispatch could be blindly retried"}
		}
	}
	// Side-effect: register any provided StatusCheck/Compensate so the
	// reactor processes can resolve them via the shared registries.
	if opts.StatusCheck != nil {
		RegisterStatusCheck(opts.Name, opts.StatusCheck)
	}
	if opts.Compensate != nil {
		RegisterCompensator(opts.Name, opts.Compensate)
	}
	return &OutboxTool{Opts: opts}, nil
}

// MustOutboxTool — like NewOutboxTool but panics on configuration errors.
// Use in init() blocks where a misconfiguration should crash early.
func MustOutboxTool(opts OutboxToolOpts) *OutboxTool {
	t, err := NewOutboxTool(opts)
	if err != nil {
		panic(err)
	}
	return t
}

// Envelope — build the JSON envelope the outbox reactor will dispatch.
// `payload` MUST be JSON-serialisable; it is wrapped, not consumed.
func (t *OutboxTool) Envelope(payload map[string]any) (map[string]any, error) {
	env := map[string]any{
		"__outbox__":     true,
		"connector":      t.Opts.Connector,
		"tool":           t.Opts.Name,
		"semantics":      string(t.Opts.Semantics),
		"wait_for_result": t.Opts.WaitForResult,
		"human_gate":     t.Opts.HumanGate,
		"payload":        payload,
	}
	if t.Opts.DispatchTimeoutMs > 0 {
		env["dispatch_timeout_ms"] = t.Opts.DispatchTimeoutMs
	}
	if t.Opts.BusinessKey != nil {
		bk, err := t.Opts.BusinessKey(payload)
		if err != nil {
			return nil, fmt.Errorf("tape.outbox %q: BusinessKey: %w", t.Opts.Name, err)
		}
		env["business_key"] = bk
	}
	return env, nil
}

// EnvelopeJSON — convenience wrapper that returns the encoded envelope.
func (t *OutboxTool) EnvelopeJSON(payload map[string]any) ([]byte, error) {
	env, err := t.Envelope(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(env)
}

// IsOutboxEnvelope — given an arbitrary tool result, report whether it is
// an outbox envelope. Used by the Go ADK adapter (and by hand-written
// dispatchers) to recognise an intent vs a direct result.
func IsOutboxEnvelope(v any) bool {
	switch m := v.(type) {
	case map[string]any:
		b, ok := m["__outbox__"].(bool)
		return ok && b
	case *OutboxEnvelope:
		return m != nil && m.Outbox
	}
	return false
}
