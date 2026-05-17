package tape

// Tenancy — the DX-correct surface for *future* hard multi-tenancy.
// Mirrors `tape/sdk/python/tape/tenancy.py`.

import "os"

// TenancyMode — the supported deployment modes.
type TenancyMode string

const (
	TenancySingle           TenancyMode = "single"
	TenancyTrustedMultiApp  TenancyMode = "trusted_multi_app"
	TenancyHardMultiTenant  TenancyMode = "hard_multi_tenant"
)

// TenancyConfig — declared in tape.yaml; consumed by the SDK for log
// tagging and by `tape doctor` for the loud warning when `hard_multi_tenant`
// is requested but the runtime can't enforce it.
type TenancyConfig struct {
	Mode     TenancyMode
	TenantID string
}

// TenancyFromEnv — read from $TAPE_TENANCY / $TAPE_TENANT_ID with sane
// defaults.
func TenancyFromEnv() TenancyConfig {
	mode := TenancyMode(os.Getenv("TAPE_TENANCY"))
	if mode == "" {
		mode = TenancySingle
	}
	tid := os.Getenv("TAPE_TENANT_ID")
	if tid == "" {
		tid = "default"
	}
	return TenancyConfig{Mode: mode, TenantID: tid}
}

// IsHard — true iff the configured mode is hard_multi_tenant.
func (t TenancyConfig) IsHard() bool { return t.Mode == TenancyHardMultiTenant }

// WarnIfHardButUnenforced — return loud warnings when hard_multi_tenant
// is requested today (the proto and stores do not yet carry tenant_id).
func (t TenancyConfig) WarnIfHardButUnenforced() []string {
	if !t.IsHard() {
		return nil
	}
	return []string{
		"tenancy.mode=hard_multi_tenant requested but the Tape proto and stores " +
			"do not yet carry a first-class tenant_id. Cross-tenant data isolation " +
			"cannot be enforced at the runtime; this mode is DESIGN-ONLY today.",
	}
}
