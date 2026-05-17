// Reactions — the event-bus user surface for Go.
//
// This file is the Go-side companion to `design-principles/tape-event-bus.md`
// and a mirror of `tape/sdk/python/tape/reactions.py`. The shape:
//
//   * `On(...)` and the `OnXxx(...)` convenience wrappers collect a
//     process-global registry of `ReactionDef`s with their handler closures.
//     The registration calls are DECLARATIONS only — they do not contact the
//     server. Push them to the server with `RegisterAll(ctx, c, prefix)`.
//
//   * `RunDispatcher(ctx, c, opts)` is the in-process reference dispatcher: for
//     every TASK-kind registered reaction, claim a bounded batch from the
//     server, dispatch handlers with backpressure (max_concurrency /
//     rate_limit_per_s / debounce_ms), and complete/nack each task. The
//     server enforces retry & DLQ — the dispatcher just calls
//     `NackTask(permanent=…)` once `attempts >= dlq_after_n`. AGENT reactions
//     are handled entirely on the server; PUBLISH reactions are pulled by the
//     Pub/Sub bridge (`RunPubSubBridge`).
//
//   * Subject path segments are URL-encoded except for the wildcards `*` and
//     `**`. A `key=""` argument is treated as `**` (rest wildcard).
//
// This module is intentionally optional: nothing else in the SDK depends on
// it. You can use the Tape journal/values/effects perfectly well without ever
// importing the reactions surface.
package tape

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"golang.org/x/time/rate"

	pb "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
)

// ReactionHandler is the user callback the in-proc dispatcher runs for every
// claimed task. Returning a non-nil error triggers a NackTask (with
// permanent=true once attempts >= dlq_after_n).
type ReactionHandler func(ctx context.Context, env *Envelope) error

// Envelope is the per-task payload handed to the user handler. It mirrors the
// CEL envelope on the server: a handler can use the same field names that
// server-side predicates use.
type Envelope struct {
	Task    *pb.Task
	Payload map[string]any // parsed payload_json; nil if not JSON-decodable
}

// ReactionDef is one `On(...)` declaration before it's registered on the
// server. Field semantics match the proto / Python types one-for-one. Leave
// fields zero-valued to inherit defaults; `On(...)` normalises Min(1, …)
// where appropriate.
type ReactionDef struct {
	SubjectPattern    string
	Predicate         string
	Agent             string // kind=AGENT; if set, kind defaults to AGENT
	Publish           string // kind=PUBLISH; if set, kind defaults to PUBLISH
	Name              string
	ReactionID        string
	MaxConcurrency    int
	RateLimitPerS     int
	DebounceMs        int
	RetryMax          int
	RetryBackoffMs    int
	DLQAfterN         int
	NumShards         int
	BootstrapFromHead bool
	Handler           ReactionHandler

	// Filled in by RegisterAll once the server returns the canonical id.
	serverReactionID string
}

// ── registry ────────────────────────────────────────────────────────────────

var (
	registryMu sync.Mutex
	registry   []*ReactionDef
)

// On appends `def` to the process-global reaction registry. The decoration is
// declarative — the server is not contacted until RegisterAll. If `def.Agent`
// is set the reaction kind defaults to AGENT; if `def.Publish` is set it
// defaults to PUBLISH; otherwise it's TASK. It is an error to set both.
func On(def ReactionDef) {
	if def.Agent != "" && def.Publish != "" {
		panic("tape.On: pass either Agent= OR Publish=, not both")
	}
	if def.MaxConcurrency < 1 {
		def.MaxConcurrency = 1
	}
	if def.NumShards < 1 {
		def.NumShards = 1
	}
	if def.RetryMax == 0 {
		def.RetryMax = 5
	}
	if def.RetryBackoffMs == 0 {
		def.RetryBackoffMs = 1000
	}
	if def.DLQAfterN == 0 {
		def.DLQAfterN = 5
	}
	if def.Name == "" {
		def.Name = def.SubjectPattern
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = append(registry, &def)
}

// GetRegistry returns a snapshot of the registered reactions. Mostly useful
// for tests / introspection.
func GetRegistry() []*ReactionDef {
	registryMu.Lock()
	defer registryMu.Unlock()
	out := make([]*ReactionDef, len(registry))
	copy(out, registry)
	return out
}

// ClearRegistry drops every registered reaction. Test-only helper.
func ClearRegistry() {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = nil
}

// kindOf resolves a ReactionDef's handler kind from Agent/Publish.
func (d *ReactionDef) kindOf() pb.HandlerKind {
	if d.Agent != "" {
		return pb.HandlerKind_HANDLER_KIND_AGENT
	}
	if d.Publish != "" {
		return pb.HandlerKind_HANDLER_KIND_PUBLISH
	}
	return pb.HandlerKind_HANDLER_KIND_TASK
}

// ── subject helpers / convenience wrappers ─────────────────────────────────

// seg URL-encodes one subject segment. `*` and `**` pass through unchanged
// (they're the grammar's wildcards). Anything else — slashes, spaces, colons —
// is percent-encoded so user-chosen keys can't break the grammar.
func seg(s string) string {
	if s == "*" || s == "**" {
		return s
	}
	return url.QueryEscape(s)
}

// OnValueChange fires when a value in `(namespace, key)` is written. `key=""`
// matches any key (rest wildcard `**`); `key="*"` matches one segment.
func OnValueChange(namespace, key string, h ReactionHandler, opts ReactionDef) {
	if key == "" {
		key = "**"
	}
	opts.SubjectPattern = fmt.Sprintf("/tape/value/changed/%s/%s", seg(namespace), seg(key))
	opts.Handler = h
	On(opts)
}

// OnValueDeleted is the deletion counterpart of OnValueChange.
func OnValueDeleted(namespace, key string, h ReactionHandler, opts ReactionDef) {
	if key == "" {
		key = "**"
	}
	opts.SubjectPattern = fmt.Sprintf("/tape/value/deleted/%s/%s", seg(namespace), seg(key))
	opts.Handler = h
	On(opts)
}

// OnEffectConfirmed fires on `/tape/effect/confirmed/<tool>/**`. Pass tool=""
// or tool="*" to match any tool.
func OnEffectConfirmed(tool string, h ReactionHandler, opts ReactionDef) {
	if tool == "" {
		tool = "*"
	}
	opts.SubjectPattern = fmt.Sprintf("/tape/effect/confirmed/%s/**", seg(tool))
	opts.Handler = h
	On(opts)
}

// OnEffectFailed fires on `/tape/effect/failed/<tool>/**`.
func OnEffectFailed(tool string, h ReactionHandler, opts ReactionDef) {
	if tool == "" {
		tool = "*"
	}
	opts.SubjectPattern = fmt.Sprintf("/tape/effect/failed/%s/**", seg(tool))
	opts.Handler = h
	On(opts)
}

// OnEffectUnknown fires on `/tape/effect/unknown/<tool>/**`.
func OnEffectUnknown(tool string, h ReactionHandler, opts ReactionDef) {
	if tool == "" {
		tool = "*"
	}
	opts.SubjectPattern = fmt.Sprintf("/tape/effect/unknown/%s/**", seg(tool))
	opts.Handler = h
	On(opts)
}

// OnDecisionRecorded fires on every decision write.
func OnDecisionRecorded(h ReactionHandler, opts ReactionDef) {
	opts.SubjectPattern = "/tape/decision/recorded/**"
	opts.Handler = h
	On(opts)
}

// OnGate fires on a gate lifecycle event. `verb=""` defaults to `"released"`;
// pass `"waiting"` for the park event.
func OnGate(gate, verb string, h ReactionHandler, opts ReactionDef) {
	if verb == "" {
		verb = "released"
	}
	opts.SubjectPattern = fmt.Sprintf("/tape/gate/%s/%s/**", seg(verb), seg(gate))
	opts.Handler = h
	On(opts)
}

// OnRun fires on run lifecycle events. `status=""` defaults to `"terminal"`.
func OnRun(status string, h ReactionHandler, opts ReactionDef) {
	if status == "" {
		status = "terminal"
	}
	opts.SubjectPattern = fmt.Sprintf("/tape/run/%s/**", seg(status))
	opts.Handler = h
	On(opts)
}

// ── registration ───────────────────────────────────────────────────────────

// RegisterAll pushes every registered reaction to the server via
// RegisterReaction. The returned slice is the persisted `Reaction` rows with
// the canonical `reaction_id` filled in. `prefix` is prepended to each
// reaction's `name` so concurrent test runs don't collide.
//
// Idempotent on `ReactionID` — the server upserts by id. Reactions that
// declared `ReactionID=""` get a stable server-minted id which we cache on
// the ReactionDef so RunDispatcher can claim by it.
func RegisterAll(ctx context.Context, c *Client, prefix string) ([]*pb.Reaction, error) {
	defs := GetRegistry()
	out := make([]*pb.Reaction, 0, len(defs))
	for _, d := range defs {
		name := d.Name
		if prefix != "" {
			name = prefix + name
		}
		r, err := c.RegisterReaction(ctx, RegisterReactionOpts{
			ReactionID:        d.ReactionID,
			Name:              name,
			SubjectPattern:    d.SubjectPattern,
			PredicateCEL:      d.Predicate,
			HandlerKind:       d.kindOf(),
			AgentApp:          d.Agent,
			PublishTarget:     d.Publish,
			MaxConcurrency:    int32(d.MaxConcurrency),
			RateLimitPerS:     int32(d.RateLimitPerS),
			DebounceMs:        int32(d.DebounceMs),
			RetryMax:          int32(d.RetryMax),
			RetryBackoffMs:    int32(d.RetryBackoffMs),
			DLQAfterN:         int32(d.DLQAfterN),
			NumShards:         int32(d.NumShards),
			BootstrapFromHead: d.BootstrapFromHead,
		})
		if err != nil {
			return out, fmt.Errorf("register reaction %q: %w", d.Name, err)
		}
		d.serverReactionID = r.GetReactionId()
		out = append(out, r)
	}
	return out, nil
}

// ── backpressure primitives ────────────────────────────────────────────────

// debouncer coalesces repeated `(reaction, subject)` triggers within the
// configured window. Returns true the first time a subject is allowed; false
// for subsequent hits inside the window. The window resets on each True.
type debouncer struct {
	window time.Duration
	mu     sync.Mutex
	last   map[string]time.Time
}

func newDebouncer(ms int) *debouncer {
	if ms < 0 {
		ms = 0
	}
	return &debouncer{
		window: time.Duration(ms) * time.Millisecond,
		last:   map[string]time.Time{},
	}
}

func (d *debouncer) allow(subject string) bool {
	if d.window <= 0 {
		return true
	}
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	if last, ok := d.last[subject]; ok && now.Sub(last) < d.window {
		return false
	}
	d.last[subject] = now
	return true
}

// ── OTel propagation (lazy / optional) ─────────────────────────────────────
//
// Go's OTel SDK has a stable surface; pulling it in for every consumer would
// be heavy-handed. We expose two hooks the dispatcher calls — a default no-op
// pair that callers can swap for a real OTel implementation via SetOTelHooks.

// OTelHooks lets callers plug in real OTel context propagation without the
// SDK depending on `go.opentelemetry.io/otel`. If the start hook returns a
// non-nil func, the dispatcher calls it after the handler returns.
type OTelHooks struct {
	// Start opens a child span from the (trace_id, parent_span_id) on the
	// task and returns the span-bearing context plus an `end` func. The
	// dispatcher calls `end()` after the handler returns. A nil Start means
	// "no propagation".
	Start func(ctx context.Context, traceIDHex, parentSpanIDHex, name string) (context.Context, func())
}

var (
	otelHooksMu sync.RWMutex
	otelHooks   OTelHooks
)

// SetOTelHooks installs OTel propagation hooks for the dispatcher. Calling
// with an empty struct restores the no-op default.
func SetOTelHooks(h OTelHooks) {
	otelHooksMu.Lock()
	defer otelHooksMu.Unlock()
	otelHooks = h
}

func currentOTelHooks() OTelHooks {
	otelHooksMu.RLock()
	defer otelHooksMu.RUnlock()
	return otelHooks
}

// ── dispatcher ─────────────────────────────────────────────────────────────

func defaultOwner() string {
	if v := os.Getenv("TAPE_DISPATCHER_OWNER"); v != "" {
		return v
	}
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s:%d:%d", host, os.Getpid(), time.Now().UnixNano())
}

// envelopeOf decodes a Task into the user-facing Envelope. payload_json
// failures fall back to a nil Payload — the handler can still consult
// task.PayloadJson directly.
func envelopeOf(t *pb.Task) *Envelope {
	env := &Envelope{Task: t}
	if t.GetPayloadJson() != "" {
		var m map[string]any
		if err := json.Unmarshal([]byte(t.GetPayloadJson()), &m); err == nil {
			env.Payload = m
		}
	}
	return env
}

// reactionState bundles the per-reaction runtime: handler, concurrency
// semaphore, rate limiter and debouncer.
type reactionState struct {
	def       *ReactionDef
	limiter   *rate.Limiter
	debouncer *debouncer
	sem       chan struct{}
}

func newReactionState(d *ReactionDef) *reactionState {
	var lim *rate.Limiter
	if d.RateLimitPerS > 0 {
		lim = rate.NewLimiter(rate.Limit(d.RateLimitPerS), d.RateLimitPerS)
	}
	cap := d.MaxConcurrency
	if cap < 1 {
		cap = 1
	}
	return &reactionState{
		def:       d,
		limiter:   lim,
		debouncer: newDebouncer(d.DebounceMs),
		sem:       make(chan struct{}, cap),
	}
}

// RunDispatcherOpts is the configuration block for the in-proc dispatcher.
type RunDispatcherOpts struct {
	Owner        string
	PollInterval time.Duration
	Once         bool
	ClaimMax     int32
	LeaseMs      int64
}

// RunDispatcher is the in-proc dispatcher loop. For every TASK-kind reaction
// in the registry, it:
//
//  1. Calls ClaimTasks(reaction_id, shard=-1, ...) in a tight outer loop.
//  2. Submits each claimed Task to the reaction's semaphore-bounded worker
//     pool. Inside the worker: rate-limit via token bucket, drop a re-trigger
//     of the same subject inside the debounce window (complete it as a
//     no-op), run the handler, CompleteTask on success or
//     NackTask(permanent=…) on failure.
//
// AGENT and PUBLISH reactions are NOT driven here — the server creates
// matching runs for AGENT, and the Pub/Sub bridge pulls PUBLISH tasks.
//
// `Once=true` returns after one pass over every reaction (handy for tests).
func RunDispatcher(ctx context.Context, c *Client, opts RunDispatcherOpts) error {
	if opts.Owner == "" {
		opts.Owner = defaultOwner()
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 500 * time.Millisecond
	}
	if opts.ClaimMax <= 0 {
		opts.ClaimMax = 16
	}
	if opts.LeaseMs <= 0 {
		opts.LeaseMs = 60_000
	}

	// Build per-reaction state once. Only TASK reactions are dispatched here.
	states := []*reactionState{}
	for _, d := range GetRegistry() {
		if d.kindOf() != pb.HandlerKind_HANDLER_KIND_TASK {
			continue
		}
		if d.serverReactionID == "" {
			// Not yet registered — caller forgot to call RegisterAll.
			continue
		}
		states = append(states, newReactionState(d))
	}

	// Worker wait-group so we can drain in-flight handlers before returning.
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		didAny := false
		for _, st := range states {
			tasks, err := c.ClaimTasks(ctx, ClaimTasksOpts{
				ReactionID: st.def.serverReactionID,
				Shard:      -1,
				Owner:      opts.Owner,
				LeaseMs:    opts.LeaseMs,
				Max:        opts.ClaimMax,
			})
			if err != nil {
				// Surface ctx cancellation; ignore transient claim errors.
				if ctx.Err() != nil {
					return ctx.Err()
				}
				continue
			}
			for _, t := range tasks {
				didAny = true
				st := st
				task := t
				// Debounce decision: COMPLETE the task as a no-op rather than
				// nack-permanent. A debounced trigger is the handler choosing
				// to skip — not an error — and we don't want clean trims in
				// the DLQ. status=DONE, attempts unchanged.
				if !st.debouncer.allow(task.GetSubject()) {
					if _, err := c.CompleteTask(ctx, task.GetTaskId(), opts.Owner); err != nil && ctx.Err() != nil {
						return ctx.Err()
					}
					continue
				}
				// Acquire the per-reaction concurrency slot before starting a goroutine.
				select {
				case st.sem <- struct{}{}:
				case <-ctx.Done():
					return ctx.Err()
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-st.sem }()
					if st.limiter != nil {
						if err := st.limiter.Wait(ctx); err != nil {
							return
						}
					}
					runOne(ctx, c, st, task, opts.Owner)
				}()
			}
		}
		if opts.Once {
			return nil
		}
		if !didAny {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(opts.PollInterval):
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// runOne is the per-task worker body: open the OTel child span, invoke the
// user handler, ack with CompleteTask / NackTask.
func runOne(ctx context.Context, c *Client, st *reactionState, t *pb.Task, owner string) {
	env := envelopeOf(t)
	handlerCtx := ctx
	var end func()
	if h := currentOTelHooks(); h.Start != nil && t.GetTraceId() != "" && t.GetParentSpanId() != "" {
		handlerCtx, end = h.Start(ctx, t.GetTraceId(), t.GetParentSpanId(), "tape.task")
	}
	var handlerErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				handlerErr = fmt.Errorf("handler panic: %v", r)
			}
		}()
		handlerErr = st.def.Handler(handlerCtx, env)
	}()
	if end != nil {
		end()
	}
	if handlerErr == nil {
		_, _ = c.CompleteTask(ctx, t.GetTaskId(), owner)
		return
	}
	// The server tracks attempts; ClaimTasks already bumped the count for
	// the in-flight claim. Promote to DLQ once attempts cross the threshold.
	permanent := int(t.GetAttempts()) >= st.def.DLQAfterN
	_, _ = c.NackTask(ctx, t.GetTaskId(), owner, handlerErr.Error(), permanent)
}

// ── Pub/Sub bridge ─────────────────────────────────────────────────────────

// RunPubSubBridgeOpts is the configuration block for the Pub/Sub bridge.
type RunPubSubBridgeOpts struct {
	Project      string
	Topic        string
	ReactionID   string
	Owner        string
	Once         bool
	PollInterval time.Duration
	ClaimMax     int32
	LeaseMs      int64
}

// ErrPubSubNotBuilt is returned by RunPubSubBridge when the Pub/Sub backend
// is not compiled in. Build with `-tags pubsub` after adding
// `cloud.google.com/go/pubsub` to your module to enable it.
var ErrPubSubNotBuilt = errors.New("tape: RunPubSubBridge requires building with `-tags pubsub` and adding cloud.google.com/go/pubsub to your module")

// pubsubReactionIDs returns the candidate reaction ids for the bridge to
// claim from. If `explicit` is non-empty it's the only entry; otherwise every
// PUBLISH-kind registered reaction (after registration) is returned.
func pubsubReactionIDs(ctx context.Context, c *Client, explicit string) ([]string, error) {
	if explicit != "" {
		return []string{explicit}, nil
	}
	out := []string{}
	needsRegister := false
	for _, d := range GetRegistry() {
		if d.kindOf() != pb.HandlerKind_HANDLER_KIND_PUBLISH {
			continue
		}
		if d.serverReactionID == "" {
			needsRegister = true
			break
		}
	}
	if needsRegister {
		if _, err := RegisterAll(ctx, c, ""); err != nil {
			return nil, err
		}
	}
	for _, d := range GetRegistry() {
		if d.kindOf() != pb.HandlerKind_HANDLER_KIND_PUBLISH {
			continue
		}
		if d.serverReactionID != "" {
			out = append(out, d.serverReactionID)
		}
	}
	return out, nil
}
