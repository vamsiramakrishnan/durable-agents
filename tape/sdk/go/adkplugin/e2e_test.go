package adkplugin_test

// e2e_test.go — drives a REAL ADK-Go runner (`google.golang.org/adk`)
// through the Tape plugin, with a hand-built scripted LLM (no API key, no
// network). It is the Go counterpart of
// `tape/sdk/python-adk/tests/test_e2e_runner.py`.
//
// What it proves:
//
//   - An `embedded.Effect` (INLINE) tool: the plugin journals an intent
//     before the call and completes it CONFIRMED with the result; the
//     effect ledger ends with exactly one CONFIRMED row carrying the
//     recorded response.
//   - Replay short-circuits: pre-seeding the journal with a CONFIRMED row
//     for the same `(invocation, decision, tool, call)` key makes the
//     plugin return the recorded response WITHOUT running the tool body.
//   - An `embedded.OutboxTool` (NON_IDEMPOTENT + OUTBOX) tool: the body
//     NEVER runs inline; the effect ledger ends with one PENDING + OUTBOX
//     + NON_IDEMPOTENT row carrying the resolved business key.
//   - The construction-time safety contract still fires.
//
// The path taken: REAL ADK-Go runner — `google.golang.org/adk v1.3.0` is
// fetched as a module dependency. No fallback was needed.

import (
	"context"
	"database/sql"
	"iter"
	"strconv"
	"sync/atomic"
	"testing"

	"google.golang.org/genai"

	"google.golang.org/adk/agent"
	"google.golang.org/adk/agent/llmagent"
	"google.golang.org/adk/model"
	"google.golang.org/adk/plugin"
	"google.golang.org/adk/runner"
	"google.golang.org/adk/session"
	"google.golang.org/adk/tool"

	_ "modernc.org/sqlite"

	"github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/adkplugin"
	"github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/embedded"
)

// ── scripted LLM ──────────────────────────────────────────────────────────

// scriptedLLM — a `model.LLM` whose responses are scripted by the test.
// Each `GenerateContent` call yields the next scripted response; once the
// script is exhausted it yields a plain-text "done." so the agent stops.
type scriptedLLM struct {
	responses []*model.LLMResponse
	idx       int
}

func (m *scriptedLLM) Name() string { return "stub/scripted" }

func (m *scriptedLLM) GenerateContent(
	_ context.Context, _ *model.LLMRequest, _ bool,
) iter.Seq2[*model.LLMResponse, error] {
	return func(yield func(*model.LLMResponse, error) bool) {
		if m.idx >= len(m.responses) {
			yield(&model.LLMResponse{
				Content: &genai.Content{
					Role:  "model",
					Parts: []*genai.Part{genai.NewPartFromText("done.")},
				},
			}, nil)
			return
		}
		resp := m.responses[m.idx]
		m.idx++
		yield(resp, nil)
	}
}

var callSeq int64

func callResponse(name string, args map[string]any) *model.LLMResponse {
	id := "call_" + strconv.FormatInt(atomic.AddInt64(&callSeq, 1), 10)
	return &model.LLMResponse{
		Content: &genai.Content{
			Role: "model",
			Parts: []*genai.Part{{
				FunctionCall: &genai.FunctionCall{ID: id, Name: name, Args: args},
			}},
		},
	}
}

func textResponse(text string) *model.LLMResponse {
	return &model.LLMResponse{
		Content: &genai.Content{
			Role:  "model",
			Parts: []*genai.Part{genai.NewPartFromText(text)},
		},
	}
}

// ── test fixtures ──────────────────────────────────────────────────────────

// newSvc — an in-memory SQLite-backed TapeSessionService with the four
// tables provisioned.
func newSvc(t *testing.T) *embedded.TapeSessionService {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1) // shared in-memory DB — single connection
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if err := embedded.CreateAllTables(ctx, db, embedded.CreateAllOpts{
		Dialect: embedded.DialectSQLite,
	}); err != nil {
		t.Fatalf("CreateAllTables: %v", err)
	}
	return embedded.NewTapeSessionService(ctx, db, embedded.DialectSQLite)
}

// drain — run the agent to completion, discarding events.
func drain(t *testing.T, r *runner.Runner, msg string) {
	t.Helper()
	stream := r.Run(context.Background(), "u", "s-1",
		&genai.Content{Role: "user", Parts: []*genai.Part{genai.NewPartFromText(msg)}},
		agent.RunConfig{})
	for _, err := range stream {
		if err != nil {
			t.Fatalf("runner.Run yielded error: %v", err)
		}
	}
}

// ── e2e: inline effect is journaled CONFIRMED ─────────────────────────────

func TestE2E_InlineEffectJournaledConfirmed(t *testing.T) {
	svc := newSvc(t)

	var calls int64
	payTool, err := adkplugin.Tool(
		adkplugin.ToolConfig{Name: "record_payment", Description: "Records a payment."},
		embedded.Effect(func(args map[string]any) (any, error) {
			n := atomic.AddInt64(&calls, 1)
			return map[string]any{
				"payment_id": "pmt-0001",
				"amount":     args["amount"],
				"customer":   args["customer"],
				"call_num":   n,
			}, nil
		}),
	)
	if err != nil {
		t.Fatalf("adkplugin.Tool: %v", err)
	}

	llm := &scriptedLLM{responses: []*model.LLMResponse{
		callResponse("record_payment", map[string]any{
			"amount": float64(100), "customer": "alice"}),
		textResponse("OK"),
	}}
	agt, err := llmagent.New(llmagent.Config{
		Name:        "payments",
		Model:       llm,
		Instruction: "Use record_payment when asked.",
		Tools:       []tool.Tool{payTool},
	})
	if err != nil {
		t.Fatalf("llmagent.New: %v", err)
	}

	plug, err := adkplugin.NewTapePlugin(svc)
	if err != nil {
		t.Fatalf("NewTapePlugin: %v", err)
	}
	r, err := runner.New(runner.Config{
		AppName:           "t",
		Agent:             agt,
		SessionService:    session.InMemoryService(),
		AutoCreateSession: true,
		PluginConfig:      runner.PluginConfig{Plugins: []*plugin.Plugin{plug}},
	})
	if err != nil {
		t.Fatalf("runner.New: %v", err)
	}

	drain(t, r, "Charge alice $100")

	// The tool body ran exactly once.
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("tool body call count = %d, want 1", got)
	}

	// The effect ledger has exactly one CONFIRMED row for the tool.
	effs := listEffects(t, svc, "record_payment")
	if len(effs) != 1 {
		t.Fatalf("effect rows for record_payment = %d, want 1", len(effs))
	}
	eff := effs[0]
	if eff.Status != embedded.EffectStatusConfirmed {
		t.Errorf("effect status = %q, want %q", eff.Status, embedded.EffectStatusConfirmed)
	}
	if eff.Semantics != embedded.EffectSemanticsIdempotent {
		t.Errorf("effect semantics = %q, want idempotent", eff.Semantics)
	}
	if eff.DispatchMode != embedded.EffectDispatchInline {
		t.Errorf("effect dispatch_mode = %q, want inline", eff.DispatchMode)
	}
	resp, ok := eff.ResponseJSON.(map[string]any)
	if !ok {
		t.Fatalf("response_json is %T, want map", eff.ResponseJSON)
	}
	if resp["payment_id"] != "pmt-0001" {
		t.Errorf("recorded response payment_id = %v, want pmt-0001", resp["payment_id"])
	}

	// No effects left pending.
	pending, err := svc.ListPendingEffects(context.Background(),
		embedded.ListPendingEffectsOpts{IncludePending: true, IncludeUnknown: true})
	if err != nil {
		t.Fatalf("ListPendingEffects: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("pending effects = %d, want 0", len(pending))
	}
}

// ── e2e: replay short-circuits the tool body ──────────────────────────────

func TestE2E_ReplayShortCircuits(t *testing.T) {
	svc := newSvc(t)
	ctx := context.Background()

	// The invocation ID is generated by ADK — we can't know it in advance,
	// so this test seeds the journal via the SAME plugin path on a first
	// run, then proves a second run keyed identically does not re-call the
	// body. We force determinism by pinning a custom key off the args.
	var calls int64
	payTool, err := adkplugin.Tool(
		adkplugin.ToolConfig{Name: "charge", Description: "Charges a card."},
		embedded.Effect(
			func(args map[string]any) (any, error) {
				atomic.AddInt64(&calls, 1)
				return map[string]any{"receipt": "rcpt-1", "ok": true}, nil
			},
			embedded.EffectOpts{
				// Deterministic key: the plugin keys effects per
				// invocation, but a custom key makes the effect identical
				// across runs so we can prove the replay short-circuit.
				CustomKey: func(args map[string]any) string {
					return "charge:" + toStr(args["card"])
				},
			},
		),
	)
	if err != nil {
		t.Fatalf("adkplugin.Tool: %v", err)
	}

	// Pre-seed a CONFIRMED effect under the deterministic custom key, as if
	// a prior run had already completed it.
	if _, err := svc.BeginEffect(ctx, embedded.BeginEffectOpts{
		AppName: "t", UserID: "u", SessionID: "s-1",
		ToolName: "charge", CustomKey: "charge:card-9",
		Semantics: embedded.EffectSemanticsIdempotent,
	}); err != nil {
		t.Fatalf("seed BeginEffect: %v", err)
	}
	if _, err := svc.CompleteEffect(ctx, "t", "u", "s-1", "charge:card-9",
		embedded.EffectStatusConfirmed,
		map[string]any{"receipt": "rcpt-SEEDED", "ok": true}, nil); err != nil {
		t.Fatalf("seed CompleteEffect: %v", err)
	}

	llm := &scriptedLLM{responses: []*model.LLMResponse{
		callResponse("charge", map[string]any{"card": "card-9"}),
		textResponse("done"),
	}}
	agt, err := llmagent.New(llmagent.Config{
		Name: "card", Model: llm, Instruction: "Charge when asked.",
		Tools: []tool.Tool{payTool},
	})
	if err != nil {
		t.Fatalf("llmagent.New: %v", err)
	}
	plug, err := adkplugin.NewTapePlugin(svc)
	if err != nil {
		t.Fatalf("NewTapePlugin: %v", err)
	}
	r, err := runner.New(runner.Config{
		AppName: "t", Agent: agt, SessionService: session.InMemoryService(),
		AutoCreateSession: true,
		PluginConfig:      runner.PluginConfig{Plugins: []*plugin.Plugin{plug}},
	})
	if err != nil {
		t.Fatalf("runner.New: %v", err)
	}

	drain(t, r, "Charge card-9")

	// The tool body must NOT have run — the plugin returned the seeded
	// CONFIRMED response from beforeTool.
	if got := atomic.LoadInt64(&calls); got != 0 {
		t.Fatalf("tool body call count = %d, want 0 (replay must short-circuit)", got)
	}

	// The journal still holds the seeded response, not a fresh one.
	eff, err := svc.GetEffect(ctx, "t", "u", "s-1", "charge:card-9")
	if err != nil || eff == nil {
		t.Fatalf("GetEffect: %v eff=%v", err, eff)
	}
	resp := eff.ResponseJSON.(map[string]any)
	if resp["receipt"] != "rcpt-SEEDED" {
		t.Errorf("response receipt = %v, want rcpt-SEEDED", resp["receipt"])
	}
}

// ── e2e: outbox tool never runs inline ────────────────────────────────────

func TestE2E_OutboxToolJournaledPending(t *testing.T) {
	svc := newSvc(t)

	var landed int64 // bumped only if the body runs inline (it must not)
	wire := embedded.MustOutboxTool(
		func(args map[string]any) (any, error) {
			atomic.AddInt64(&landed, 1)
			return map[string]any{"wire_id": "wire-LIVE"}, nil
		},
		embedded.OutboxToolOpts{
			Connector:  "bank.wire",
			Compensate: "reverse_wire",
			BusinessKeyFn: func(args map[string]any) (string, error) {
				return toStr(args["account"]) + ":" + toStr(args["amount"]) + ":2026", nil
			},
		},
	)
	wireTool, err := adkplugin.Tool(
		adkplugin.ToolConfig{Name: "wire", Description: "Wires money.", IsLongRunning: true},
		wire,
	)
	if err != nil {
		t.Fatalf("adkplugin.Tool: %v", err)
	}

	llm := &scriptedLLM{responses: []*model.LLMResponse{
		callResponse("wire", map[string]any{
			"account": "acct-1", "amount": float64(2000000)}),
		textResponse("queued"),
	}}
	agt, err := llmagent.New(llmagent.Config{
		Name: "treasury", Model: llm, Instruction: "Use wire when asked.",
		Tools: []tool.Tool{wireTool},
	})
	if err != nil {
		t.Fatalf("llmagent.New: %v", err)
	}
	plug, err := adkplugin.NewTapePlugin(svc)
	if err != nil {
		t.Fatalf("NewTapePlugin: %v", err)
	}
	r, err := runner.New(runner.Config{
		AppName: "t", Agent: agt, SessionService: session.InMemoryService(),
		AutoCreateSession: true,
		PluginConfig:      runner.PluginConfig{Plugins: []*plugin.Plugin{plug}},
	})
	if err != nil {
		t.Fatalf("runner.New: %v", err)
	}

	drain(t, r, "Wire $2m to acct-1")

	// The body NEVER ran inline.
	if got := atomic.LoadInt64(&landed); got != 0 {
		t.Fatalf("outbox tool body ran inline %d times — outbox contract broken", got)
	}

	// The journal has one PENDING + OUTBOX + NON_IDEMPOTENT effect.
	toDispatch, err := svc.ListEffectsToDispatch(context.Background(),
		embedded.ListEffectsToDispatchOpts{})
	if err != nil {
		t.Fatalf("ListEffectsToDispatch: %v", err)
	}
	if len(toDispatch) != 1 {
		t.Fatalf("effects to dispatch = %d, want 1", len(toDispatch))
	}
	eff := toDispatch[0]
	if eff.Status != embedded.EffectStatusPending {
		t.Errorf("status = %q, want pending", eff.Status)
	}
	if eff.DispatchMode != embedded.EffectDispatchOutbox {
		t.Errorf("dispatch_mode = %q, want outbox", eff.DispatchMode)
	}
	if eff.Semantics != embedded.EffectSemanticsNonIdempotent {
		t.Errorf("semantics = %q, want non_idempotent", eff.Semantics)
	}
	if eff.BusinessKey != "acct-1:2000000:2026" {
		t.Errorf("business_key = %q, want acct-1:2000000:2026", eff.BusinessKey)
	}
	if eff.Connector != "bank.wire" {
		t.Errorf("connector = %q, want bank.wire", eff.Connector)
	}
}

// ── construction-time safety still fires ──────────────────────────────────

func TestOutboxToolConstructionRefusesMissingBusinessKey(t *testing.T) {
	if _, err := embedded.OutboxTool(
		func(map[string]any) (any, error) { return nil, nil },
		embedded.OutboxToolOpts{Connector: "bank.wire"}, // no business key
	); err == nil {
		t.Fatal("OutboxTool accepted a config with no business key")
	}
	if _, err := embedded.OutboxTool(
		func(map[string]any) (any, error) { return nil, nil },
		embedded.OutboxToolOpts{BusinessKeyStatic: "x:1:2026"}, // no connector
	); err == nil {
		t.Fatal("OutboxTool accepted a config with no connector")
	}
}

// ── helpers ────────────────────────────────────────────────────────────────

// effectKey — composite key of an effect ledger row.
type effectKey struct{ key, app, user, sess string }

// listEffects — pull every effect ledger row with the given tool name via
// a direct query against the service's DB handle.
//
// Note: the row cursor is fully drained and closed BEFORE any `GetEffect`
// call. The test runs against a shared in-memory SQLite DB pinned to a
// single connection (`SetMaxOpenConns(1)`); calling `GetEffect` while the
// cursor is still open would deadlock waiting for that one connection.
func listEffects(t *testing.T, svc *embedded.TapeSessionService, toolName string) []embedded.EffectRecord {
	t.Helper()
	rows, err := svc.DB().QueryContext(context.Background(),
		`SELECT idempotency_key, app_name, user_id, session_id FROM tape_effects WHERE tool_name = ?`,
		toolName)
	if err != nil {
		t.Fatalf("query effects: %v", err)
	}
	var keys []effectKey
	for rows.Next() {
		var k effectKey
		if err := rows.Scan(&k.key, &k.app, &k.user, &k.sess); err != nil {
			rows.Close()
			t.Fatalf("scan: %v", err)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatalf("rows.Err: %v", err)
	}
	rows.Close() // release the connection before the GetEffect calls

	var out []embedded.EffectRecord
	for _, k := range keys {
		eff, err := svc.GetEffect(context.Background(), k.app, k.user, k.sess, k.key)
		if err != nil || eff == nil {
			t.Fatalf("GetEffect(%q): %v", k.key, err)
		}
		out = append(out, *eff)
	}
	return out
}

func toStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'g', -1, 64)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	}
	return ""
}
