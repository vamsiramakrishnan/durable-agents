package tape

// Durable — the wiring entrypoint for an ADK-style agent on Tape.
//
// The Python SDK ships `tape.adk.durable_app(...)` which returns an
// `(App, Runner)` pair wired to a TapeClient. The Go port of ADK isn't
// shipped yet (the protocol is the contract; the adapter is mechanical),
// so `NewDurableApp()` returns the wiring **values** an adapter needs:
//
//   * the resolved TAPE_URL (`tape://...` / `tapes://...`)
//   * the connected `*Client`
//   * the canonical lease-owner string
//   * the chosen `Budget`
//
// When a Go ADK port lands, its `Runner` constructor will accept a
// `*DurableApp` directly. Until then, the SDK uses these values for
// reactor processes, outbox-aware tools, and direct Client calls.
//
// Example:
//
//   d, err := tape.NewDurableApp(ctx, tape.DurableConfig{
//       Name:    "treasury",
//       Budget:  tape.Budget{USDCap: 50, TokenCap: 2_000_000},
//   })
//   if err != nil { return err }
//   defer d.Close()
//
//   // Later, in a tool body:
//   be, _ := d.Client.BeginEffect(ctx, tape.BeginEffectOpts{...})

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// DurableConfig — the user-facing settings. Sensible defaults everywhere.
type DurableConfig struct {
	// Name — the ADK App name. Required.
	Name string

	// TapeURL — defaults to $TAPE_URL, then "tape://localhost:7878".
	TapeURL string

	// Budget — admit/charge thresholds. Zero values mean "no cap".
	Budget Budget

	// Resumable — enable ADK ResumabilityConfig(is_resumable=True). The Go
	// ADK adapter will read this; today it is recorded for completeness.
	Resumable bool

	// CheckCancellation — let the plugin poll RunStatus on every model
	// boundary. The Go ADK adapter will read this.
	CheckCancellation bool

	// LeaseOwner — overrides the default "<hostname>:<pid>" identity.
	LeaseOwner string

	// LeaseTTLMs — overrides the default lease (120_000 ms when 0).
	LeaseTTLMs int64

	// ClientOptions — passed verbatim to `tape.Dial(...)`.
	ClientOptions *Options
}

// Budget — the per-run budget. USDCap/TokenCap of 0 means "no cap".
type Budget struct {
	USDCap   float64
	TokenCap int64
}

// DurableApp — the wired bundle returned by `NewDurableApp()`.
type DurableApp struct {
	Cfg        DurableConfig
	URL        string
	Client     *Client
	LeaseOwner string
	LeaseTTLMs int64
	close      func() error
}

// Close — close the underlying client.
func (d *DurableApp) Close() error {
	if d.close != nil {
		return d.close()
	}
	if d.Client != nil {
		return d.Client.Close()
	}
	return nil
}

// NewDurableApp — resolve config, dial the server, return the bundle.
// Honours `$TAPE_URL` for `TapeURL` and `$TAPE_LEASE_MS` for `LeaseTTLMs`.
//
// Mirrors `tape.adk.durable_app(...)` from the Python SDK. The Go ADK
// adapter, when it lands, will accept a `*DurableApp` directly.
func NewDurableApp(ctx context.Context, cfg DurableConfig) (*DurableApp, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("tape.NewDurableApp: Name is required")
	}
	url := cfg.TapeURL
	if url == "" {
		url = DefaultURL()
	}
	owner := cfg.LeaseOwner
	if owner == "" {
		owner = defaultLeaseOwner()
	}
	ttl := cfg.LeaseTTLMs
	if ttl == 0 {
		ttl = defaultLeaseTTLMs()
	}

	var (
		c   *Client
		err error
	)
	if cfg.ClientOptions != nil {
		c, err = Dial(url, *cfg.ClientOptions)
	} else {
		c, err = Dial(url)
	}
	if err != nil {
		return nil, fmt.Errorf("tape.NewDurableApp: dial %q: %w", url, err)
	}

	return &DurableApp{
		Cfg: cfg, URL: url, Client: c,
		LeaseOwner: owner, LeaseTTLMs: ttl,
		close: c.Close,
	}, nil
}

func defaultLeaseOwner() string {
	h, _ := os.Hostname()
	if h == "" {
		h = "host"
	}
	return fmt.Sprintf("%s:%d", h, os.Getpid())
}

func defaultLeaseTTLMs() int64 {
	if v := os.Getenv("TAPE_LEASE_MS"); v != "" {
		var n int64
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return 120_000
}

// ensureExec — checked-once warning helper (used by adapters that want to
// know whether a binary like `tape-server` is on PATH).
func ensureExec(name string) (string, bool) {
	p, err := exec.LookPath(name)
	if err != nil {
		return "", false
	}
	return p, true
}

// trimURL — strip the scheme from a Tape URL for display.
func trimURL(u string) string {
	for _, prefix := range []string{"tape://", "tapes://", "grpc://", "grpcs://"} {
		if strings.HasPrefix(u, prefix) {
			return strings.TrimPrefix(u, prefix)
		}
	}
	return u
}
