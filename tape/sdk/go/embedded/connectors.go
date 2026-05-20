package embedded

// connectors.go — the three-method protocol every connector implements,
// plus the three result shapes. Mirrors `tape_adk/connectors.py`. A
// connector wraps ONE upstream (`bank.wire`, `payment.charge`) and is
// the only place in the system allowed to call it — the agent's tool
// body never does.

import (
	"context"
	"fmt"
	"sync"
)

// DispatchResult — what a single dispatch attempt produced. `Status` is
// "confirmed" | "unknown" | "failed". `RetryAfterMs` is a hint honored
// only when status="failed" (0 → use the dispatcher's default
// exponential backoff; negative → terminal FAILED).
type DispatchResult struct {
	Status       string
	ExternalRef  string
	Response     any
	Error        any
	RetryAfterMs int64
}

// ObservationResult — what `observe(business_key)` found upstream.
// `Status` is "confirmed" | "failed" | "absent" | "duplicate".
// `CompensateKind` is the obligation kind the reconciler should register
// atomically when status="duplicate".
type ObservationResult struct {
	Status         string
	ExternalRef    string
	Response       any
	Error          any
	CompensateKind string
}

// CompensationResult — what running the inverse produced. `Status` is
// "compensated" | "failed".
type CompensationResult struct {
	Status       string
	Response     any
	Error        any
	RetryAfterMs int64
}

// Connector — implement three methods and you can ride the reactor
// loops. `Name` is the registry key matching `effect.connector` (the
// same string an outbox tool declares at construction).
type Connector interface {
	Name() string
	Dispatch(ctx context.Context, effect EffectRecord) (DispatchResult, error)
	Observe(ctx context.Context, effect EffectRecord) (ObservationResult, error)
	Compensate(ctx context.Context, obligation ObligationRecord) (CompensationResult, error)
}

// LogConnector — a no-op connector that records every call. Useful for
// tests and demos. Always confirms on dispatch, reports absent on
// observe, and compensates successfully.
type LogConnector struct {
	NameStr string

	mu            sync.Mutex
	Dispatches    []EffectRecord
	Observations  []EffectRecord
	Compensations []ObligationRecord
}

// NewLogConnector — build a LogConnector with the given registry name
// (defaults to "log").
func NewLogConnector(name string) *LogConnector {
	if name == "" {
		name = "log"
	}
	return &LogConnector{NameStr: name}
}

func (l *LogConnector) Name() string { return l.NameStr }

func (l *LogConnector) Dispatch(_ context.Context, e EffectRecord) (DispatchResult, error) {
	l.mu.Lock()
	l.Dispatches = append(l.Dispatches, e)
	l.mu.Unlock()
	short := e.IdempotencyKey
	if len(short) > 8 {
		short = short[:8]
	}
	return DispatchResult{Status: "confirmed", ExternalRef: fmt.Sprintf("log-%s", short)}, nil
}

func (l *LogConnector) Observe(_ context.Context, e EffectRecord) (ObservationResult, error) {
	l.mu.Lock()
	l.Observations = append(l.Observations, e)
	l.mu.Unlock()
	return ObservationResult{Status: "absent"}, nil
}

func (l *LogConnector) Compensate(_ context.Context, o ObligationRecord) (CompensationResult, error) {
	l.mu.Lock()
	l.Compensations = append(l.Compensations, o)
	l.mu.Unlock()
	return CompensationResult{Status: "compensated"}, nil
}
