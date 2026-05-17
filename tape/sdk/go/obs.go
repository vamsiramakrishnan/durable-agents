package tape

// Observability — structured-log helpers and OpenTelemetry span-name
// constants. Mirrors `tape/sdk/python/tape/obs.py`.

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Span-name constants — use these instead of stringly-typed magic.
const (
	SpanBeginRun         = "tape.begin_run"
	SpanResumeRun        = "tape.resume_run"
	SpanRecordDecision   = "tape.record_decision"
	SpanBeginEffect      = "tape.begin_effect"
	SpanCompleteEffect   = "tape.complete_effect"
	SpanReconcileEffect  = "tape.reconcile_effect"
	SpanDispatchEffect   = "tape.dispatch_effect"
	SpanCompensate       = "tape.compensate"
	SpanRedrive          = "tape.redrive"
	SpanAwaitSignal      = "tape.await_signal"
	SpanSendSignal       = "tape.send_signal"
)

// AllSpans — the canonical span-name list.
var AllSpans = []string{
	SpanBeginRun, SpanResumeRun, SpanRecordDecision,
	SpanBeginEffect, SpanCompleteEffect,
	SpanReconcileEffect, SpanDispatchEffect,
	SpanCompensate, SpanRedrive,
	SpanAwaitSignal, SpanSendSignal,
}

// StructuredFields — the canonical field order for structured log records.
// `LogJSON` emits keys in this order when present.
var StructuredFields = []string{
	"ts", "level", "msg",
	"tenant_id", "app_name", "run_id", "invocation_id", "session_id",
	"seq", "effect_key", "decision_index", "reactor", "lease_owner",
}

var logMu sync.Mutex

// LogJSON — emit one JSON line to stderr, ordered by `StructuredFields`.
// Extra fields are appended after the canonical block.
func LogJSON(msg string, fields map[string]any) {
	rec := map[string]any{
		"ts":    float64(time.Now().UnixNano()) / 1e9,
		"level": "INFO",
		"msg":   msg,
	}
	for k, v := range fields {
		if v == nil || v == "" {
			continue
		}
		rec[k] = v
	}
	// Re-serialise with stable key order.
	ordered := make(map[string]any, len(rec))
	for _, k := range StructuredFields {
		if v, ok := rec[k]; ok {
			ordered[k] = v
			delete(rec, k)
		}
	}
	for k, v := range rec {
		ordered[k] = v
	}
	enc, _ := json.Marshal(ordered)
	logMu.Lock()
	defer logMu.Unlock()
	fmt.Fprintln(os.Stderr, string(enc))
}

// LogLevel — convenience for log_json at a specific level.
func LogLevel(level, msg string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["level"] = level
	LogJSON(msg, fields)
}

// SpanHook — set by the host to receive Tape span events. Default is nil
// (no-op). A future opentelemetry-go integration would set this from
// `init`.
type SpanHook func(name string, attrs map[string]any) (end func(err error))

var (
	spanHookMu sync.RWMutex
	spanHook   SpanHook
)

// SetSpanHook — install a span hook. Used by tracing adapters; safe to
// call multiple times (last wins).
func SetSpanHook(h SpanHook) {
	spanHookMu.Lock()
	defer spanHookMu.Unlock()
	spanHook = h
}

// Span — open a span via the installed hook (or a no-op). Returns the
// end function the caller defers.
func Span(name string, attrs map[string]any) func(err error) {
	spanHookMu.RLock()
	h := spanHook
	spanHookMu.RUnlock()
	if h == nil {
		return func(error) {}
	}
	return h(name, attrs)
}
