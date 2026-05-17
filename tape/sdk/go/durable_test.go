package tape

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	connectors "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/connectors"
)

// Outbox rules: non_idempotent without any safety net must be rejected at
// construction time.
func TestOutboxRejectsUnsafeNonIdempotent(t *testing.T) {
	_, err := NewOutboxTool(OutboxToolOpts{
		Name:      "wire",
		Connector: "bank.wire",
		Semantics: OutboxNonIdempotent,
	})
	if !IsOutboxConfigError(err) {
		t.Fatalf("expected OutboxConfigError, got %v", err)
	}
	if !strings.Contains(err.Error(), "non_idempotent") {
		t.Fatalf("unexpected message: %v", err)
	}
}

func TestOutboxAcceptsBusinessKey(t *testing.T) {
	tool, err := NewOutboxTool(OutboxToolOpts{
		Name:      "wire",
		Connector: "bank.wire",
		Semantics: OutboxNonIdempotent,
		BusinessKey: func(p map[string]any) (string, error) {
			return p["account"].(string) + ":" + jsonNum(p["amount"]), nil
		},
		WaitForResult: true,
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	env, err := tool.Envelope(map[string]any{
		"account": "ACME-1", "amount": 100, "beneficiary": "bob",
	})
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	if env["__outbox__"] != true {
		t.Fatalf("missing __outbox__")
	}
	if env["business_key"] != "ACME-1:100" {
		t.Fatalf("business_key wrong: %v", env["business_key"])
	}
	if !IsOutboxEnvelope(env) {
		t.Fatalf("IsOutboxEnvelope false on a real envelope")
	}
}

func jsonNum(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// Connector registry round-trips.
func TestConnectorRegistry(t *testing.T) {
	r := connectors.NewRegistry()
	c := connectors.NewLogConnector("/tmp/tape-go-test.jsonl")
	if err := r.Register("log", c); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := r.Register("log", c); !errors.Is(err, connectors.ErrAlreadyRegistered) {
		t.Fatalf("expected ErrAlreadyRegistered, got %v", err)
	}
	got, err := r.Get("log")
	if err != nil || got != c {
		t.Fatalf("get: %v / %v", err, got)
	}
	if _, err := r.Get("missing"); !errors.Is(err, connectors.ErrUnknownConnector) {
		t.Fatalf("expected ErrUnknownConnector, got %v", err)
	}
	res, err := got.Dispatch(context.Background(), connectors.Effect{
		RunID: "r1", IdempotencyKey: "k1", ToolName: "t", Connector: "log",
		Payload: map[string]any{"x": 1},
	})
	if err != nil || res.Outcome != connectors.DispatchConfirmed {
		t.Fatalf("dispatch: %v / %v", err, res.Outcome)
	}
}

// Tenancy warnings.
func TestTenancyWarnings(t *testing.T) {
	tc := TenancyConfig{Mode: TenancyHardMultiTenant, TenantID: "x"}
	ws := tc.WarnIfHardButUnenforced()
	if len(ws) == 0 {
		t.Fatalf("expected warning for hard_multi_tenant")
	}
	tc2 := TenancyConfig{Mode: TenancySingle}
	if w := tc2.WarnIfHardButUnenforced(); len(w) != 0 {
		t.Fatalf("expected no warnings for single, got %v", w)
	}
}

// Obs structured-log smoke test.
func TestObsLogJSON(t *testing.T) {
	// Just ensure it doesn't panic + produces valid JSON.
	old := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w
	LogJSON("test", map[string]any{
		"run_id": "r-1", "app_name": "treasury", "reactor": "recovery",
	})
	w.Close()
	os.Stderr = old
	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	if n == 0 {
		t.Fatalf("no stderr output")
	}
	var m map[string]any
	if err := json.Unmarshal(buf[:n], &m); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, buf[:n])
	}
	if m["run_id"] != "r-1" {
		t.Fatalf("missing run_id: %v", m)
	}
}

// DurableApp fails fast on missing Name.
func TestDurableAppRequiresName(t *testing.T) {
	_, err := NewDurableApp(context.Background(), DurableConfig{})
	if err == nil {
		t.Fatalf("expected error on missing Name")
	}
}
