package tape

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return p
}

func waitFor(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("server never came up at %s", addr)
}

func startServer(t *testing.T) (string, func()) {
	t.Helper()
	bin, err := filepath.Abs("../../server/target/debug/tape-server")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("tape-server not built: %v (run `cargo build` in tape/server)", err)
	}
	port := freePort(t)
	addr := "127.0.0.1:" + itoa(port)
	cmd := exec.Command(bin, "--listen", addr, "--store", "memory")
	cmd.Stderr = nil
	cmd.Stdout = nil
	cmd.Env = append(os.Environ(), "RUST_LOG=tape_server=warn")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, addr, 15*time.Second)
	return "tape://" + addr, func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
}

func itoa(n int) string {
	const d = "0123456789"
	if n == 0 {
		return "0"
	}
	var s []byte
	for n > 0 {
		s = append([]byte{d[n%10]}, s...)
		n /= 10
	}
	return string(s)
}

func TestClientRoundTripsLifecycle(t *testing.T) {
	url, stop := startServer(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	r, err := c.BeginRun(ctx, BeginRunOpts{
		AppName: "a", UserID: "u", SessionID: "go-smoke",
		InvocationID: "inv-go", LeaseOwner: "test", LeaseTTLMs: 60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.Resumed {
		t.Fatalf("expected fresh run; got resumed=%v", r.Resumed)
	}

	if _, err := c.RecordDecision(ctx, r.RunId, 0, "m", "{}", `{"plan":1}`, "", "p1"); err != nil {
		t.Fatal(err)
	}
	if d, err := c.GetDecision(ctx, r.RunId, 0); err != nil || !d.Found {
		t.Fatalf("get_decision: %v found=%v", err, d.Found)
	}

	be, err := c.BeginEffect(ctx, BeginEffectOpts{
		RunID: r.RunId, DecisionIndex: 0, ToolName: "execute_sweep",
		CallIndex: 0, RequestJSON: "{}",
	})
	if err != nil {
		t.Fatal(err)
	}
	if int32(be.Status) != EffectStatusPending {
		t.Fatalf("status=%v want PENDING", be.Status)
	}
	if be.IdempotencyKey != r.RunId+"/decision-0/execute_sweep/0" {
		t.Fatalf("key=%q", be.IdempotencyKey)
	}

	// short-circuit on second begin
	be2, err := c.BeginEffect(ctx, BeginEffectOpts{RunID: r.RunId, DecisionIndex: 0, ToolName: "execute_sweep", CallIndex: 0})
	if err != nil {
		t.Fatal(err)
	}
	if be2.IdempotencyKey != be.IdempotencyKey {
		t.Fatalf("short-circuit key changed")
	}

	if _, err := c.CompleteEffect(ctx, r.RunId, be.IdempotencyKey, EffectStatusConfirmed, `{"wire_id":"w1"}`, ""); err != nil {
		t.Fatal(err)
	}
	ge, err := c.GetEffect(ctx, r.RunId, be.IdempotencyKey)
	if err != nil || !ge.Found {
		t.Fatalf("get_effect: %v found=%v", err, ge.Found)
	}
	if int32(ge.Effect.Status) != EffectStatusConfirmed {
		t.Fatalf("status after complete: %v", ge.Effect.Status)
	}

	if _, err := c.RegisterCompensation(ctx, r.RunId, be.IdempotencyKey, "reverse_wire", "{}"); err != nil {
		t.Fatal(err)
	}
	obs, err := c.ListObligations(ctx, r.RunId, true)
	if err != nil || len(obs.Obligations) != 1 {
		t.Fatalf("obligations: %v len=%d", err, len(obs.Obligations))
	}

	if _, err := c.SetBudget(ctx, r.RunId, 1.0, 0); err != nil {
		t.Fatal(err)
	}
	adm, _ := c.AdmitBudget(ctx, r.RunId, 0.5, 0)
	if !adm.Admitted {
		t.Fatalf("expected admitted")
	}
	if _, err := c.ChargeBudget(ctx, r.RunId, 0.9, 0); err != nil {
		t.Fatal(err)
	}
	adm, _ = c.AdmitBudget(ctx, r.RunId, 0.5, 0)
	if adm.Admitted {
		t.Fatalf("expected refused after charging")
	}

	// timer
	tr, err := c.SetTimer(ctx, SetTimerOpts{
		RunID: r.RunId, FireAtMs: time.Now().Add(-time.Second).UnixMilli(),
		Kind: "gate_timeout", PayloadJSON: `{"gate":"g1"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	due, _ := c.ListDueTimers(ctx, 0, 50, true)
	found := false
	for _, x := range due.Timers {
		if x.TimerId == tr.TimerId {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("timer not in due list")
	}

	if _, err := c.EndRun(ctx, r.RunId, RunStatusTerminal, ""); err != nil {
		t.Fatal(err)
	}
	fresh, _ := c.GetRun(ctx, r.RunId)
	if int32(fresh.Status) != RunStatusTerminal {
		t.Fatalf("status: %v", fresh.Status)
	}

	// re-begin -> resumed=TERMINAL
	again, _ := c.BeginRun(ctx, BeginRunOpts{
		AppName: "a", UserID: "u", SessionID: "go-smoke",
		InvocationID: "inv-go", LeaseOwner: "test", LeaseTTLMs: 60_000,
	})
	if !again.Resumed || again.RunId != r.RunId || int32(again.Status) != RunStatusTerminal {
		t.Fatalf("re-begin: resumed=%v rid=%q status=%v", again.Resumed, again.RunId, again.Status)
	}

	// session + event roundtrip
	if _, err := c.CreateSession(ctx, "a", "u", "go-sess", `{"k":1}`); err != nil {
		t.Fatal(err)
	}
	ae, err := c.AppendEvent(ctx, "a", "u", "go-sess",
		&pb.EventRecord{Id: "e1", InvocationId: "inv", Author: "user", ContentJson: "{}", ActionsJson: "{}"},
		`{"k":2}`)
	if err != nil || ae.Event == nil {
		t.Fatalf("append: %v ev=%v", err, ae.Event)
	}
	gs, err := c.GetSession(ctx, "a", "u", "go-sess", 0)
	if err != nil || !gs.Found {
		t.Fatalf("get_session: %v found=%v", err, gs.Found)
	}
	if len(gs.Session.Events) != 1 || gs.Session.StateJson == "" {
		t.Fatalf("session: %#v", gs.Session)
	}
}
