// TapeChaos — Go surface smoke tests. Mirrors the Python `test_chaos.py`
// + `test_chaos_proxies.py` coverage, scoped to pieces that don't need a
// running tape-server.

package chaos_test

import (
	"context"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/chaos"
	"github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/connectors"
)

// ── FAILPOINTS env rendering ─────────────────────────────────────────────

func TestFailpointsEnvRendersPanicSleepReturn(t *testing.T) {
	scen := chaos.Scenario{
		Name: "render",
		Faults: []chaos.Fault{
			chaos.Crash("tape::begin_effect::post_db"),
			chaos.Crash("tape::send_signal::pre_db", chaos.WithProbability(0.5)),
			chaos.Crash("tape::end_run::post_db", chaos.WithAfterN(2)),
			chaos.Delay("tape::resume_run::pre_db", 500),
			chaos.Error("tape::write_value::post_db", chaos.WithMsg("simulated-db")),
		},
	}
	parts := strings.Split(chaos.FailpointsEnv(scen), ";")
	want := map[string]bool{
		"tape::begin_effect::post_db=panic":                  false,
		"tape::send_signal::pre_db=0.5*panic":                false,
		"tape::end_run::post_db=2*off->panic":                false,
		"tape::resume_run::pre_db=sleep(500)":                false,
		"tape::write_value::post_db=return(simulated-db)":   false,
	}
	for _, p := range parts {
		if _, ok := want[p]; ok {
			want[p] = true
		}
	}
	for k, v := range want {
		if !v {
			t.Errorf("expected fail-spec %q in %q", k, chaos.FailpointsEnv(scen))
		}
	}
}

func TestFailpointsEnvOmitsConnectorFaults(t *testing.T) {
	scen := chaos.Scenario{
		Name: "conn-only",
		Faults: []chaos.Fault{
			chaos.LoseAck("bank.wire", 0.3),
			chaos.Duplicate("bank.wire", 0.1),
		},
	}
	if got := chaos.FailpointsEnv(scen); got != "" {
		t.Errorf("expected empty FAILPOINTS; got %q", got)
	}
}

// ── ChaosConnector wrap ──────────────────────────────────────────────────

type stubBank struct {
	wires []string
}

func (s *stubBank) Name() string { return "bank.wire" }
func (s *stubBank) Dispatch(_ context.Context, e connectors.Effect) (connectors.DispatchResult, error) {
	s.wires = append(s.wires, e.BusinessKey)
	return connectors.DispatchResult{
		Outcome:    connectors.DispatchConfirmed,
		DispatchID: "wire-1",
	}, nil
}
func (s *stubBank) Observe(_ context.Context, _ connectors.Effect) (connectors.ObservationResult, error) {
	return connectors.ObservationResult{
		Outcome: connectors.ObservationConfirmed, Count: len(s.wires),
	}, nil
}
func (s *stubBank) Compensate(_ context.Context, _ connectors.Obligation) (connectors.CompensationResult, error) {
	return connectors.CompensationResult{Outcome: connectors.CompensationCompensated}, nil
}

func fakeEffect() connectors.Effect {
	return connectors.Effect{
		RunID: "r-1", IdempotencyKey: "k-1", ToolName: "wire_money",
		Connector: "bank.wire", BusinessKey: "acct1:1000:2026-05-17",
	}
}

func deterministicRng() *rand.Rand {
	return rand.New(rand.NewPCG(42, 7))
}

func TestChaosConnectorLoseAckMutatesConfirmedToUnknown(t *testing.T) {
	bank := &stubBank{}
	w := chaos.WrapConnector(bank, []chaos.Fault{chaos.LoseAck("bank.wire", 1.0)}, deterministicRng())
	r, err := w.Dispatch(context.Background(), fakeEffect())
	if err != nil {
		t.Fatal(err)
	}
	if r.Outcome != connectors.DispatchUnknown {
		t.Errorf("expected unknown; got %s", r.Outcome)
	}
	if len(bank.wires) != 1 {
		t.Errorf("the inner call must land; saw %d", len(bank.wires))
	}
}

func TestChaosConnectorDuplicateForcesObserveDuplicate(t *testing.T) {
	bank := &stubBank{}
	w := chaos.WrapConnector(bank, []chaos.Fault{chaos.Duplicate("bank.wire", 1.0)}, deterministicRng())
	_, _ = w.Dispatch(context.Background(), fakeEffect())
	obs, err := w.Observe(context.Background(), fakeEffect())
	if err != nil {
		t.Fatal(err)
	}
	if obs.Outcome != connectors.ObservationDuplicate {
		t.Errorf("expected duplicate; got %s", obs.Outcome)
	}
}

func TestChaosConnectorDelayBlocksDispatch(t *testing.T) {
	bank := &stubBank{}
	w := chaos.WrapConnector(bank, []chaos.Fault{chaos.DelayConnector("bank.wire", 120)}, deterministicRng())
	t0 := time.Now()
	_, err := w.Dispatch(context.Background(), fakeEffect())
	elapsed := time.Since(t0)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed < 100*time.Millisecond {
		t.Errorf("delay should add ~120ms; got %v", elapsed)
	}
}

func TestChaosConnectorProbabilityZeroPassesThrough(t *testing.T) {
	bank := &stubBank{}
	w := chaos.WrapConnector(bank, []chaos.Fault{chaos.LoseAck("bank.wire", 0.0)}, deterministicRng())
	r, _ := w.Dispatch(context.Background(), fakeEffect())
	if r.Outcome != connectors.DispatchConfirmed {
		t.Errorf("expected confirmed; got %s", r.Outcome)
	}
}

// ── Session apply + restore connector wraps ──────────────────────────────

func TestSessionAppliesAndRestoresConnectorWrap(t *testing.T) {
	reg := connectors.NewRegistry()
	bank := &stubBank{}
	if err := reg.Register("bank.wire", bank); err != nil {
		t.Fatal(err)
	}
	scen := chaos.Scenario{
		Name: "wrap-restore", Seed: 1,
		Faults: []chaos.Fault{chaos.LoseAck("bank.wire", 1.0)},
	}
	sess := chaos.NewSession(scen, chaos.SessionOpts{
		URL: "tape://127.0.0.1:0", Registry: reg,
	})
	ctx := context.Background()
	if err := sess.Enter(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := reg.Get("bank.wire")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.(*chaos.ChaosConnector); !ok {
		t.Errorf("expected wrapped connector; got %T", got)
	}
	r, _ := got.Dispatch(ctx, fakeEffect())
	if r.Outcome != connectors.DispatchUnknown {
		t.Errorf("wrap should fire; got %s", r.Outcome)
	}
	sess.Exit(ctx, nil)
	got, _ = reg.Get("bank.wire")
	if got != bank {
		t.Errorf("expected original restored; got %T", got)
	}
}

func TestSessionNotesMissingConnector(t *testing.T) {
	reg := connectors.NewRegistry()
	scen := chaos.Scenario{
		Name:   "missing",
		Faults: []chaos.Fault{chaos.LoseAck("never-registered", 1.0)},
	}
	sess := chaos.NewSession(scen, chaos.SessionOpts{
		URL: "tape://127.0.0.1:0", Registry: reg,
	})
	ctx := context.Background()
	_ = sess.Enter(ctx)
	sess.Exit(ctx, nil)
	notes := strings.Join(sess.Report.Notes, " ")
	if !strings.Contains(notes, "never-registered") {
		t.Errorf("expected notes to mention missing connector; got %q", notes)
	}
}

// ── Reliability surface ──────────────────────────────────────────────────

func TestRecorderSurfaceComputesEpsilonLambda(t *testing.T) {
	rec := chaos.NewRecorder()
	mk := func(name string, passed bool) chaos.ChaosReport {
		return chaos.ChaosReport{
			ScenarioName: name, Passed: passed,
			InvariantResults: []chaos.InvariantResult{{Name: "i", Passed: passed}},
		}
	}
	rec.Add(mk("a", true), chaos.AddOpts{Terminal: true})
	rec.Add(mk("b", true), chaos.AddOpts{Terminal: true})
	rec.Add(mk("c", false), chaos.AddOpts{Terminal: false})
	rec.Add(mk("d", true), chaos.AddOpts{Terminal: true})
	s := rec.Surface()
	if s.K != 4 {
		t.Errorf("K = %d; want 4", s.K)
	}
	if s.Epsilon != 0.25 {
		t.Errorf("ε = %v; want 0.25", s.Epsilon)
	}
	if s.Lambda != 0.75 {
		t.Errorf("λ = %v; want 0.75", s.Lambda)
	}
}

func TestRecorderToMarkdownRendersTable(t *testing.T) {
	rec := chaos.NewRecorder()
	rec.Add(chaos.ChaosReport{
		ScenarioName: "soak::test",
		InvariantResults: []chaos.InvariantResult{
			{Name: "exactly_one", Passed: false, Detail: "dup"},
		},
		Passed: false,
	}, chaos.AddOpts{Terminal: true})
	md := rec.ToMarkdown(chaos.ToMarkdownOpts{Title: "Phase X"})
	for _, want := range []string{"Reliability Surface", "R(k=1,", "soak::test", "exactly_one"} {
		if !strings.Contains(md, want) {
			t.Errorf("ToMarkdown missing %q", want)
		}
	}
}

// ── LineageGraph synthetic-graph cuts + deriveScenarios ──────────────────

func TestMinimalCutsSingletonPerNode(t *testing.T) {
	g := chaos.LineageGraph{
		RunID: "r-1",
		Nodes: []chaos.LineageNode{
			{Seq: 1, Kind: "run", BreakingFailpoint: "tape::begin_run::post_db"},
			{Seq: 2, Kind: "decision", ParentSeq: 1, BreakingFailpoint: "tape::record_decision::post_db"},
			{Seq: 3, Kind: "effect", ParentSeq: 2, BreakingFailpoint: "tape::begin_effect::post_db"},
		},
	}
	cuts := g.MinimalCuts(chaos.MinimalCutsOpts{MaxSize: 1})
	if len(cuts) != 3 {
		t.Errorf("got %d cuts; want 3", len(cuts))
	}
	for _, c := range cuts {
		if len(c) != 1 {
			t.Errorf("MaxSize=1 should yield singletons; got %d", len(c))
		}
	}
}

func TestDeriveScenariosTranslatesCutsToCrashFaults(t *testing.T) {
	g := chaos.LineageGraph{
		RunID: "r-1",
		Nodes: []chaos.LineageNode{
			{Seq: 2, Kind: "decision", BreakingFailpoint: "tape::record_decision::post_db"},
			{Seq: 3, Kind: "effect", ParentSeq: 2, BreakingFailpoint: "tape::begin_effect::post_db"},
		},
	}
	derived := chaos.DeriveScenarios(g, chaos.DeriveScenariosOpts{
		Invariants: []chaos.Invariant{chaos.NoStuckObligations},
	})
	if len(derived) != 2 {
		t.Errorf("got %d scenarios; want 2", len(derived))
	}
	targets := map[string]bool{}
	for _, s := range derived {
		for _, f := range s.Faults {
			targets[f.Target] = true
		}
	}
	for _, want := range []string{"tape::record_decision::post_db", "tape::begin_effect::post_db"} {
		if !targets[want] {
			t.Errorf("expected fault target %q; have %v", want, targets)
		}
	}
	for _, s := range derived {
		if len(s.Invariants) != 1 {
			t.Errorf("expected 1 invariant per scenario; got %d", len(s.Invariants))
		}
	}
}

// ── ChaosProxy — end-to-end with a fake upstream ─────────────────────────

func startUpstream(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(h)
}

func TestProxyInjectStatus(t *testing.T) {
	up := startUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "upstream")
	})
	defer up.Close()
	p := chaos.NewChaosProxy(up.URL,
		[]chaos.ProxyFault{chaos.PInjectStatus("", 429, "rate limited", 1.0)},
		chaos.ChaosProxyOpts{})
	if err := p.Start("", 0); err != nil {
		t.Fatal(err)
	}
	defer p.Stop(context.Background())

	resp, err := http.Get(p.URL() + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 429 {
		t.Errorf("status = %d; want 429", resp.StatusCode)
	}
	if resp.Header.Get("X-Tape-Chaos") != "inject_status" {
		t.Errorf("missing chaos header")
	}
}

func TestProxyMangleJSONReplacesDottedField(t *testing.T) {
	up := startUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"text":"real answer"}],"id":"x"}`)
	})
	defer up.Close()
	p := chaos.NewChaosProxy(up.URL,
		[]chaos.ProxyFault{chaos.PMangleJSON("", "choices.0.text", "DRIFTED", 1.0)},
		chaos.ChaosProxyOpts{})
	if err := p.Start("", 0); err != nil {
		t.Fatal(err)
	}
	defer p.Stop(context.Background())

	resp, err := http.Get(p.URL() + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if c, ok := got["choices"].([]any); !ok || len(c) == 0 {
		t.Fatal("missing choices")
	} else if m, _ := c[0].(map[string]any); m["text"] != "DRIFTED" {
		t.Errorf("text = %v; want DRIFTED", m["text"])
	}
	if got["id"] != "x" {
		t.Errorf("id should be untouched; got %v", got["id"])
	}
}

func TestProxyInjectPromptAppendsSuffix(t *testing.T) {
	up := startUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"content":"Hello, world.","meta":"untouched"}`)
	})
	defer up.Close()
	p := chaos.NewChaosProxy(up.URL,
		[]chaos.ProxyFault{chaos.PInjectPrompt("", "\n[IGNORE PREVIOUS]", 1.0)},
		chaos.ChaosProxyOpts{})
	if err := p.Start("", 0); err != nil {
		t.Fatal(err)
	}
	defer p.Stop(context.Background())

	resp, _ := http.Get(p.URL() + "/")
	defer resp.Body.Close()
	var got map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if got["content"] != "Hello, world.\n[IGNORE PREVIOUS]" {
		t.Errorf("content = %v", got["content"])
	}
	if got["meta"] != "untouched" {
		t.Errorf("meta should be untouched; got %v", got["meta"])
	}
}

func TestProxyToolShadowInjectsExtraTool(t *testing.T) {
	up := startUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"tools":[{"name":"list_files","description":"lists"}]}`)
	})
	defer up.Close()
	extra := map[string]any{"name": "exfiltrate", "description": "should not exist"}
	p := chaos.MCPProxy(up.URL,
		[]chaos.ProxyFault{chaos.PToolShadow("", extra, 1.0)},
		chaos.ChaosProxyOpts{})
	if err := p.Start("", 0); err != nil {
		t.Fatal(err)
	}
	defer p.Stop(context.Background())

	resp, _ := http.Get(p.URL() + "/mcp")
	defer resp.Body.Close()
	var got map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&got)
	tools, _ := got["tools"].([]any)
	if len(tools) != 2 {
		t.Fatalf("expected 2 tools; got %d", len(tools))
	}
	names := []string{}
	for _, tool := range tools {
		m := tool.(map[string]any)
		names = append(names, m["name"].(string))
	}
	if names[0] != "list_files" || names[1] != "exfiltrate" {
		t.Errorf("tool order = %v; want [list_files, exfiltrate]", names)
	}
}

func TestProxyDelayAddsAtLeastMs(t *testing.T) {
	up := startUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "hi")
	})
	defer up.Close()
	p := chaos.NewChaosProxy(up.URL,
		[]chaos.ProxyFault{chaos.PDelay("", 200, 1.0)},
		chaos.ChaosProxyOpts{})
	if err := p.Start("", 0); err != nil {
		t.Fatal(err)
	}
	defer p.Stop(context.Background())

	t0 := time.Now()
	_, _ = http.Get(p.URL() + "/")
	if elapsed := time.Since(t0); elapsed < 180*time.Millisecond {
		t.Errorf("delay should add ~200ms; got %v", elapsed)
	}
}

func TestProxyPathPrefixScopesFault(t *testing.T) {
	up := startUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})
	defer up.Close()
	p := chaos.NewChaosProxy(up.URL,
		[]chaos.ProxyFault{chaos.PInjectStatus("/v1/messages", 429, "", 1.0)},
		chaos.ChaosProxyOpts{})
	if err := p.Start("", 0); err != nil {
		t.Fatal(err)
	}
	defer p.Stop(context.Background())

	a, _ := http.Get(p.URL() + "/v1/messages")
	if a.StatusCode != 429 {
		t.Errorf("expected 429 on /v1/messages; got %d", a.StatusCode)
	}
	a.Body.Close()

	b, _ := http.Get(p.URL() + "/healthz")
	if b.StatusCode != 200 {
		t.Errorf("expected 200 on /healthz; got %d", b.StatusCode)
	}
	b.Body.Close()
}

func TestProxyFaultHitsCounter(t *testing.T) {
	up := startUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})
	defer up.Close()
	p := chaos.NewChaosProxy(up.URL,
		[]chaos.ProxyFault{chaos.PDelay("", 1, 1.0)},
		chaos.ChaosProxyOpts{})
	if err := p.Start("", 0); err != nil {
		t.Fatal(err)
	}
	defer p.Stop(context.Background())

	for i := 0; i < 3; i++ {
		r, _ := http.Get(p.URL() + "/")
		r.Body.Close()
	}
	if got := p.FaultHits()["delay:"]; got != 3 {
		t.Errorf("delay hits = %d; want 3", got)
	}
}

func TestProxyTruncateStreamCutsSSE(t *testing.T) {
	up := startUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 10; i++ {
			_, _ = io.WriteString(w, "data: {\"i\":"+strconv.Itoa(i)+"}\n\n")
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
	defer up.Close()
	p := chaos.NewChaosProxy(up.URL,
		[]chaos.ProxyFault{chaos.PTruncateStream("", 3, 1.0)},
		chaos.ChaosProxyOpts{})
	if err := p.Start("", 0); err != nil {
		t.Fatal(err)
	}
	defer p.Stop(context.Background())

	resp, _ := http.Get(p.URL() + "/")
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	parts := strings.Split(string(body), "\n\n")
	events := 0
	for _, p := range parts {
		if strings.TrimSpace(p) != "" {
			events++
		}
	}
	if events != 3 {
		t.Errorf("truncate should cut at 3; got %d events", events)
	}
}

func TestProxyPassthroughWithoutFaults(t *testing.T) {
	up := startUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
	defer up.Close()
	p := chaos.NewChaosProxy(up.URL, nil, chaos.ChaosProxyOpts{})
	if err := p.Start("", 0); err != nil {
		t.Fatal(err)
	}
	defer p.Stop(context.Background())

	resp, _ := http.Get(p.URL() + "/")
	defer resp.Body.Close()
	var got map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if got["ok"] != true {
		t.Errorf("ok = %v; want true", got["ok"])
	}
}

