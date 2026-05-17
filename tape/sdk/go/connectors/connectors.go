// Package connectors — capability connector registry + protocol.
//
// A `Connector` is what the outbox reactor calls to actually perform a
// side effect. Three operations, all idempotent on
// (RunID, IdempotencyKey):
//
//   * Dispatch     — perform / enqueue the effect
//   * Observe      — ask the counterparty about an UNKNOWN
//   * Compensate   — reverse a duplicate (or any registered obligation)
//
// Built-in connectors: LogConnector (tests / demos), HttpConnector,
// PubSubConnector, CloudTasksConnector. Register your own:
//
//   import "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/connectors"
//
//   connectors.Default.Register("bank.wire", connectors.NewHttpConnector(
//       connectors.HttpOpts{URL: "https://bank.example/wires"},
//   ))
package connectors

import (
	"context"
	"errors"
	"sync"
)

// DispatchOutcome — one of CONFIRMED / PENDING / UNKNOWN / FAILED.
type DispatchOutcome string

const (
	DispatchConfirmed DispatchOutcome = "confirmed"
	DispatchPending   DispatchOutcome = "pending"
	DispatchUnknown   DispatchOutcome = "unknown"
	DispatchFailed    DispatchOutcome = "failed"
)

// ObservationOutcome — what `Observe()` resolved.
type ObservationOutcome string

const (
	ObservationConfirmed ObservationOutcome = "confirmed"
	ObservationAbsent    ObservationOutcome = "absent"
	ObservationDuplicate ObservationOutcome = "duplicate"
	ObservationStuck     ObservationOutcome = "stuck"
	ObservationUnknown   ObservationOutcome = "unknown"
)

// CompensationOutcome — what `Compensate()` resolved.
type CompensationOutcome string

const (
	CompensationCompensated CompensationOutcome = "compensated"
	CompensationPending     CompensationOutcome = "pending"
	CompensationStuck       CompensationOutcome = "stuck"
	CompensationFailed      CompensationOutcome = "failed"
)

// Effect — the intent the outbox reactor wants dispatched.
type Effect struct {
	RunID           string
	IdempotencyKey  string
	ToolName        string
	Connector       string
	Payload         any
	BusinessKey     string
	Attempt         int
	Semantics       string
	TenantID        string
	AppName         string
	Metadata        map[string]any
}

// Obligation — the compensation obligation registered after a forward
// effect confirmed.
type Obligation struct {
	RunID          string
	EffectKey      string
	Kind           string
	Payload        any
	Attempt        int
	CompensatorRef string
	TenantID       string
}

// DispatchResult / ObservationResult / CompensationResult — what each op
// returns. Idiomatic Go: `Outcome` is the discriminator; `Response`
// carries the structured detail.
type DispatchResult struct {
	Outcome      DispatchOutcome
	Response     any
	Error        string
	DispatchID   string
	RetryAfterMs int
}

type ObservationResult struct {
	Outcome  ObservationOutcome
	Response any
	Error    string
	Count    int
}

type CompensationResult struct {
	Outcome  CompensationOutcome
	Response any
	Error    string
}

// Connector — the interface every capability connector implements. All
// three methods MUST be idempotent on (RunID, IdempotencyKey).
type Connector interface {
	Name() string
	Dispatch(ctx context.Context, effect Effect) (DispatchResult, error)
	Observe(ctx context.Context, effect Effect) (ObservationResult, error)
	Compensate(ctx context.Context, obligation Obligation) (CompensationResult, error)
}

// ── registry ────────────────────────────────────────────────────────────

// ErrUnknownConnector — registry.Get returned no match.
var ErrUnknownConnector = errors.New("connectors: unknown connector")

// ErrAlreadyRegistered — registry.Register saw a name collision.
var ErrAlreadyRegistered = errors.New("connectors: already registered")

// Registry — a process-local registry of connectors keyed by name.
type Registry struct {
	mu    sync.RWMutex
	items map[string]Connector
}

// NewRegistry — fresh, empty registry.
func NewRegistry() *Registry {
	return &Registry{items: map[string]Connector{}}
}

// Register — add a connector under `name`. Returns ErrAlreadyRegistered
// if `name` is taken.
func (r *Registry) Register(name string, c Connector) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[name]; ok {
		return ErrAlreadyRegistered
	}
	r.items[name] = c
	return nil
}

// Replace — install `c` under `name`, overwriting any prior value.
func (r *Registry) Replace(name string, c Connector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items[name] = c
}

// Get — fetch by name. Returns ErrUnknownConnector if absent.
func (r *Registry) Get(name string) (Connector, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.items[name]
	if !ok {
		return nil, ErrUnknownConnector
	}
	return c, nil
}

// Names — sorted-ish snapshot of registered names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.items))
	for k := range r.items {
		out = append(out, k)
	}
	return out
}

// Default — the process-global registry. Most projects register at init
// time and consume from here.
var Default = NewRegistry()
