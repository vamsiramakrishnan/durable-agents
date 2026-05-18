package chaos

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	tape "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go"
	"github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
)

// Keys stripped before comparing canonical journals. Kept in lockstep
// with `tape.chaos.snapshot._STRIP_KEYS` (Python).
var stripKeys = map[string]struct{}{
	"ts_ms": {}, "started_at_ms": {}, "ended_at_ms": {}, "last_update_time_ms": {},
	"lease_expires_at_ms": {}, "claim_expires_at_ms": {}, "dispatch_claim_expires_at_ms": {},
	"next_dispatch_at_ms": {}, "next_attempt_at_ms": {}, "fire_at_ms": {},
	"lease_owner": {}, "claimed_by": {}, "dispatch_claimed_by": {},
	"trace_id": {}, "span_id": {}, "parent_span_id": {},
	"seq": {}, "global_seq": {},
	"invocation_id": {},
}

var terminalRunStatuses = map[string]struct{}{
	"terminal": {}, "failed": {}, "cancelled": {}, "stuck": {},
}

func canonical(value any, runIDMap map[string]string) any {
	switch v := value.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, vv := range v {
			if _, drop := stripKeys[k]; drop {
				continue
			}
			out[k] = canonical(vv, runIDMap)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, vv := range v {
			out[i] = canonical(vv, runIDMap)
		}
		return out
	case string:
		s := v
		for raw, repl := range runIDMap {
			if raw != "" && strings.Contains(s, raw) {
				s = strings.ReplaceAll(s, raw, repl)
			}
		}
		return s
	default:
		return value
	}
}

func stableJSON(v any) string {
	// Sort map keys for deterministic encoding.
	b, _ := stableEncode(v)
	return string(b)
}

func stableEncode(v any) ([]byte, error) {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			kj, _ := json.Marshal(k)
			b.Write(kj)
			b.WriteByte(':')
			ej, _ := stableEncode(t[k])
			b.Write(ej)
		}
		b.WriteByte('}')
		return []byte(b.String()), nil
	case []any:
		var b strings.Builder
		b.WriteByte('[')
		for i, vv := range t {
			if i > 0 {
				b.WriteByte(',')
			}
			ej, _ := stableEncode(vv)
			b.Write(ej)
		}
		b.WriteByte(']')
		return []byte(b.String()), nil
	default:
		return json.Marshal(v)
	}
}

// JournalLine — one canonical journal entry.
type JournalLine struct {
	Kind    string
	Payload string // stable-key-encoded JSON of canonical payload
}

// Snapshot — a run's journal, canonicalised. Position encodes seq.
type Snapshot struct {
	RunID string
	Lines []JournalLine
}

// Equals — two snapshots are equal iff every (kind, payload) matches in order.
func (s Snapshot) Equals(o Snapshot) bool {
	if len(s.Lines) != len(o.Lines) {
		return false
	}
	for i := range s.Lines {
		if s.Lines[i] != o.Lines[i] {
			return false
		}
	}
	return true
}

// DiffEntry — one per-position difference.
type DiffEntry struct {
	Index int
	Op    string // "==", "!=", "<" (only in B), ">" (only in A)
	A, B  *JournalLine
}

// Diff — per-position diff list, omitting equal entries.
func (s Snapshot) Diff(o Snapshot) []DiffEntry {
	var out []DiffEntry
	n := len(s.Lines)
	if len(o.Lines) > n {
		n = len(o.Lines)
	}
	for i := 0; i < n; i++ {
		var a, b *JournalLine
		if i < len(s.Lines) {
			a = &s.Lines[i]
		}
		if i < len(o.Lines) {
			b = &o.Lines[i]
		}
		if a == nil {
			out = append(out, DiffEntry{Index: i, Op: "<", B: b})
		} else if b == nil {
			out = append(out, DiffEntry{Index: i, Op: ">", A: a})
		} else if *a != *b {
			out = append(out, DiffEntry{Index: i, Op: "!=", A: a, B: b})
		}
	}
	return out
}

// CaptureSnapshotOpts — knobs for CaptureSnapshot.
type CaptureSnapshotOpts struct {
	DeadlineMs       int
	CanonicalRunID   string
}

// CaptureSnapshot — stream a run's journal via SubscribeRun, canonicalise,
// and stop at the first terminal `run` entry (or deadline). Mirrors
// `tape.chaos.snapshot.capture` (Python).
func CaptureSnapshot(ctx context.Context, client *tape.Client, runID string, opts CaptureSnapshotOpts) (Snapshot, error) {
	deadlineMs := opts.DeadlineMs
	if deadlineMs <= 0 {
		deadlineMs = 5000
	}
	canonRid := opts.CanonicalRunID
	if canonRid == "" {
		canonRid = "run-1"
	}
	runIDMap := map[string]string{runID: canonRid}

	subCtx, cancel := context.WithTimeout(ctx, time.Duration(deadlineMs)*time.Millisecond)
	defer cancel()

	stream, err := client.PB().SubscribeRun(subCtx, &tapepb.SubscribeRunRequest{RunId: runID, FromSeq: 0})
	if err != nil {
		return Snapshot{RunID: runID}, err
	}
	var lines []JournalLine
	for {
		entry, err := stream.Recv()
		if err != nil {
			break
		}
		var payload any
		if err := json.Unmarshal([]byte(entry.PayloadJson), &payload); err != nil {
			payload = map[string]any{"_raw": entry.PayloadJson}
		}
		canon := canonical(payload, runIDMap)
		lines = append(lines, JournalLine{Kind: entry.Kind, Payload: stableJSON(canon)})
		if entry.Kind == "run" {
			if m, ok := payload.(map[string]any); ok {
				status, _ := m["status"].(string)
				if _, term := terminalRunStatuses[strings.ToLower(status)]; term {
					break
				}
			}
		}
	}
	return Snapshot{RunID: runID, Lines: lines}, nil
}

// ── DeepSnapshot ─────────────────────────────────────────────────────────

// DeepSnapshot — full-projection walk; catches body-drift the journal
// summary in Snapshot misses.
type DeepSnapshot struct {
	RunID       string
	Decisions   []string
	Effects     []string
	Obligations []string
}

// Equals — pairwise equality on all three projections.
func (a DeepSnapshot) Equals(b DeepSnapshot) bool {
	eq := func(x, y []string) bool {
		if len(x) != len(y) {
			return false
		}
		for i := range x {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}
	return eq(a.Decisions, b.Decisions) && eq(a.Effects, b.Effects) && eq(a.Obligations, b.Obligations)
}

// CaptureDeepOpts — knobs for CaptureDeep.
type CaptureDeepOpts struct {
	CanonicalRunID string
	MaxDecisions   int
}

// CaptureDeep — walks decisions by index, effects via the journal +
// GetEffect, obligations via ListObligations. Higher cost than
// CaptureSnapshot; use when body-level drift matters.
func CaptureDeep(ctx context.Context, client *tape.Client, runID string, opts CaptureDeepOpts) (DeepSnapshot, error) {
	canonRid := opts.CanonicalRunID
	if canonRid == "" {
		canonRid = "run-1"
	}
	maxDec := opts.MaxDecisions
	if maxDec <= 0 {
		maxDec = 1000
	}
	runIDMap := map[string]string{runID: canonRid}
	canonField := func(d map[string]any) string {
		return stableJSON(canonical(d, runIDMap))
	}

	out := DeepSnapshot{RunID: runID}

	for i := int64(0); i < int64(maxDec); i++ {
		got, err := client.GetDecision(ctx, runID, i)
		if err != nil || !got.Found {
			break
		}
		d := got.Decision
		out.Decisions = append(out.Decisions, canonField(map[string]any{
			"decision_index": d.DecisionIndex, "model": d.Model,
			"request_json": d.RequestJson, "response_json": d.ResponseJson,
			"policy_version": d.PolicyVersion, "rationale": d.Rationale,
		}))
	}

	// Effects: walk journal once to collect keys, then GetEffect each.
	seen := map[string]struct{}{}
	subCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	stream, err := client.PB().SubscribeRun(subCtx, &tapepb.SubscribeRunRequest{RunId: runID, FromSeq: 0})
	if err == nil {
		for {
			entry, err := stream.Recv()
			if err != nil {
				break
			}
			if entry.Kind == "effect" {
				var p map[string]any
				if json.Unmarshal([]byte(entry.PayloadJson), &p) == nil {
					if k, _ := p["idempotency_key"].(string); k != "" {
						seen[k] = struct{}{}
					}
				}
			}
			if entry.Kind == "run" {
				var p map[string]any
				_ = json.Unmarshal([]byte(entry.PayloadJson), &p)
				status, _ := p["status"].(string)
				if _, term := terminalRunStatuses[strings.ToLower(status)]; term {
					break
				}
			}
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		got, err := client.GetEffect(ctx, runID, k)
		if err != nil || !got.Found {
			continue
		}
		e := got.Effect
		out.Effects = append(out.Effects, canonField(map[string]any{
			"tool_name": e.ToolName, "idempotency_key": e.IdempotencyKey,
			"status": e.Status, "request_json": e.RequestJson,
			"response_json": e.ResponseJson, "error_json": e.ErrorJson,
			"semantics": e.Semantics, "dispatch_mode": e.DispatchMode,
			"business_key": e.BusinessKey, "connector": e.Connector,
			"external_ref": e.ExternalRef, "decision_index": e.DecisionIndex,
		}))
	}

	// Obligations
	if resp, err := client.ListObligations(ctx, runID, false); err == nil {
		for _, o := range resp.Obligations {
			out.Obligations = append(out.Obligations, canonField(map[string]any{
				"kind": o.Kind, "effect_key": o.EffectKey, "status": o.Status,
				"payload_json": o.PayloadJson, "attempts": o.Attempts,
				"max_attempts": o.MaxAttempts, "last_error": o.LastError,
				"result_json": o.ResultJson, "compensator_ref": o.CompensatorRef,
			}))
		}
	}
	return out, nil
}
