// Package tape — the Go client over the `tape.v1` gRPC service.
//
// URL schemes:
//
//	tape://host:port    plaintext gRPC (self-hosted, k8s, local)
//	tapes://host        TLS on :443 (Cloud Run / any HTTPS endpoint)
//
// On `tapes://`, if the endpoint is IAM-protected (e.g. an internal Cloud Run
// service), Tape attaches a Google OIDC ID token automatically via the
// google.golang.org/api/idtoken package — the caller's service account just
// needs roles/run.invoker. Set Auth=false / Audience="" to override.
package tape

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
)

// DefaultURL — honours $TAPE_URL.
func DefaultURL() string {
	if v := os.Getenv("TAPE_URL"); v != "" {
		return v
	}
	return "tape://localhost:7878"
}

// ──── status enums (re-exported for ergonomic use) ──────────────────────────
const (
	RunStatusRunnable     = int32(pb.RunStatus_RUN_STATUS_RUNNABLE)
	RunStatusRunning      = int32(pb.RunStatus_RUN_STATUS_RUNNING)
	RunStatusWaiting      = int32(pb.RunStatus_RUN_STATUS_WAITING)
	RunStatusTerminal     = int32(pb.RunStatus_RUN_STATUS_TERMINAL)
	RunStatusFailed       = int32(pb.RunStatus_RUN_STATUS_FAILED)
	RunStatusCompensating = int32(pb.RunStatus_RUN_STATUS_COMPENSATING)
	RunStatusStuck        = int32(pb.RunStatus_RUN_STATUS_STUCK)
	RunStatusCancelled    = int32(pb.RunStatus_RUN_STATUS_CANCELLED)

	EffectStatusPending   = int32(pb.EffectStatus_EFFECT_STATUS_PENDING)
	EffectStatusConfirmed = int32(pb.EffectStatus_EFFECT_STATUS_CONFIRMED)
	EffectStatusFailed    = int32(pb.EffectStatus_EFFECT_STATUS_FAILED)
	EffectStatusUnknown   = int32(pb.EffectStatus_EFFECT_STATUS_UNKNOWN)

	ObligationPending     = int32(pb.ObligationStatus_OBLIGATION_STATUS_PENDING)
	ObligationCommitted   = int32(pb.ObligationStatus_OBLIGATION_STATUS_COMMITTED)
	ObligationCompensated = int32(pb.ObligationStatus_OBLIGATION_STATUS_COMPENSATED)
	ObligationStuck       = int32(pb.ObligationStatus_OBLIGATION_STATUS_STUCK)
)

// Options for Dial.
type Options struct {
	Auth       bool   // default true on tapes://
	Audience   string // override the OIDC audience derived from the URL
	IDToken    string // static ID token (overrides Auth)
	DialOpts   []grpc.DialOption
}

func targetOf(url string) (target string, secure bool) {
	switch {
	case strings.HasPrefix(url, "tapes://"):
		h := strings.TrimPrefix(url, "tapes://")
		if !strings.Contains(h, ":") {
			h = h + ":443"
		}
		return h, true
	case strings.HasPrefix(url, "grpcs://"):
		return targetOf("tapes://" + strings.TrimPrefix(url, "grpcs://"))
	case strings.HasPrefix(url, "tape://"):
		return strings.TrimPrefix(url, "tape://"), false
	case strings.HasPrefix(url, "grpc://"):
		return strings.TrimPrefix(url, "grpc://"), false
	}
	return url, false
}

func audienceFor(url string) string {
	t, _ := targetOf(url)
	host := strings.SplitN(t, ":", 2)[0]
	return "https://" + host
}

// Client wraps a *grpc.ClientConn and the generated pb.TapeClient with friendlier
// helpers. Use Conn() / PB() if you want raw access.
type Client struct {
	url  string
	conn *grpc.ClientConn
	pb   pb.TapeClient
}

func Dial(url string, opts ...Options) (*Client, error) {
	var o Options
	o.Auth = true
	if len(opts) > 0 {
		o = opts[0]
	}
	target, secure := targetOf(url)
	dialOpts := append([]grpc.DialOption{}, o.DialOpts...)
	if secure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})))
		if o.IDToken != "" {
			dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(staticToken{o.IDToken}))
		} else if o.Auth {
			aud := o.Audience
			if aud == "" {
				if v := os.Getenv("TAPE_AUDIENCE"); v != "" {
					aud = v
				} else {
					aud = audienceFor(url)
				}
			}
			ts, err := newIDTokenSource(aud)
			if err == nil && ts != nil {
				dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(ts))
			}
			// if google-auth isn't wired up, the call goes through without auth —
			// fine if the endpoint isn't IAM-protected (TLS-without-IAM works).
		}
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	cc, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("tape dial %s: %w", url, err)
	}
	return &Client{url: url, conn: cc, pb: pb.NewTapeClient(cc)}, nil
}

func (c *Client) Close() error      { return c.conn.Close() }
func (c *Client) URL() string       { return c.url }
func (c *Client) Conn() *grpc.ClientConn { return c.conn }
func (c *Client) PB() pb.TapeClient { return c.pb }

// ──── run lifecycle ─────────────────────────────────────────────────────────

type BeginRunOpts struct {
	AppName, UserID, SessionID, InvocationID, LeaseOwner string
	LeaseTTLMs                                           int64
}

func (c *Client) BeginRun(ctx context.Context, o BeginRunOpts) (*pb.BeginRunResponse, error) {
	if o.LeaseTTLMs == 0 {
		o.LeaseTTLMs = 120_000
	}
	return c.pb.BeginRun(ctx, &pb.BeginRunRequest{
		AppName: o.AppName, UserId: o.UserID, SessionId: o.SessionID,
		InvocationId: o.InvocationID, LeaseOwner: o.LeaseOwner, LeaseTtlMs: o.LeaseTTLMs,
	})
}

func (c *Client) EndRun(ctx context.Context, runID string, status int32, detailJSON string) (*pb.EndRunResponse, error) {
	if status == 0 {
		status = RunStatusTerminal
	}
	return c.pb.EndRun(ctx, &pb.EndRunRequest{RunId: runID, Status: pb.RunStatus(status), DetailJson: detailJSON})
}

func (c *Client) GetRun(ctx context.Context, runID string) (*pb.RunState, error) {
	return c.pb.GetRun(ctx, &pb.GetRunRequest{RunId: runID})
}

func (c *Client) ListRunsToRecover(ctx context.Context, limit int64) (*pb.ListRunsToRecoverResponse, error) {
	if limit == 0 {
		limit = 100
	}
	return c.pb.ListRunsToRecover(ctx, &pb.ListRunsToRecoverRequest{Limit: limit})
}

// ──── decisions ─────────────────────────────────────────────────────────────

func (c *Client) RecordDecision(ctx context.Context, runID string, idx int64, model, requestJSON, responseJSON, rationale, policyVersion string) (*pb.DecisionRecord, error) {
	return c.pb.RecordDecision(ctx, &pb.RecordDecisionRequest{
		RunId: runID, DecisionIndex: idx, Model: model, RequestJson: requestJSON,
		ResponseJson: responseJSON, Rationale: rationale, PolicyVersion: policyVersion,
	})
}

func (c *Client) GetDecision(ctx context.Context, runID string, idx int64) (*pb.GetDecisionResponse, error) {
	return c.pb.GetDecision(ctx, &pb.GetDecisionRequest{RunId: runID, DecisionIndex: idx})
}

// ──── effects ───────────────────────────────────────────────────────────────

type BeginEffectOpts struct {
	RunID         string
	DecisionIndex int64
	ToolName      string
	CallIndex     int32
	RequestJSON   string
	CustomKey     string
}

func (c *Client) BeginEffect(ctx context.Context, o BeginEffectOpts) (*pb.BeginEffectResponse, error) {
	return c.pb.BeginEffect(ctx, &pb.BeginEffectRequest{
		RunId: o.RunID, DecisionIndex: o.DecisionIndex, ToolName: o.ToolName,
		CallIndex: o.CallIndex, RequestJson: o.RequestJSON, CustomKey: o.CustomKey,
	})
}

func (c *Client) CompleteEffect(ctx context.Context, runID, key string, status int32, responseJSON, errorJSON string) (*pb.EffectRecord, error) {
	return c.pb.CompleteEffect(ctx, &pb.CompleteEffectRequest{
		RunId: runID, IdempotencyKey: key, Status: pb.EffectStatus(status),
		ResponseJson: responseJSON, ErrorJson: errorJSON,
	})
}

func (c *Client) GetEffect(ctx context.Context, runID, key string) (*pb.GetEffectResponse, error) {
	return c.pb.GetEffect(ctx, &pb.GetEffectRequest{RunId: runID, IdempotencyKey: key})
}

func (c *Client) ReconcileEffect(ctx context.Context, runID, key string, resolved int32, responseJSON, errorJSON string) (*pb.EffectRecord, error) {
	return c.pb.ReconcileEffect(ctx, &pb.ReconcileEffectRequest{
		RunId: runID, IdempotencyKey: key, ResolvedStatus: pb.EffectStatus(resolved),
		ResponseJson: responseJSON, ErrorJson: errorJSON,
	})
}

// ──── obligations ───────────────────────────────────────────────────────────

// RegisterCompensationOpts: the extra options grew over time; use named fields
// rather than a five-arg positional call. CompensatorRef ("module:attr") lets a
// generic drainer resolve the inverse without importing your agent. MaxAttempts
// of 0 falls back to the server default (5).
type RegisterCompensationOpts struct {
	CompensatorRef string
	MaxAttempts    int32
}

func (c *Client) RegisterCompensation(ctx context.Context, runID, effectKey, kind, payloadJSON string, opts ...RegisterCompensationOpts) (*pb.ObligationRecord, error) {
	var o RegisterCompensationOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	return c.pb.RegisterCompensation(ctx, &pb.RegisterCompensationRequest{
		RunId: runID, EffectKey: effectKey, Kind: kind, PayloadJson: payloadJSON,
		CompensatorRef: o.CompensatorRef, MaxAttempts: o.MaxAttempts,
	})
}

// ListObligationsOpts: StatusFilter == 0 means "any status"; otherwise it's an
// exact ObligationStatus match. OnlyUnresolved is the shorthand "exclude
// terminal COMPENSATED/STUCK".
type ListObligationsOpts struct {
	StatusFilter int32
}

func (c *Client) ListObligations(ctx context.Context, runID string, onlyUnresolved bool, opts ...ListObligationsOpts) (*pb.ListObligationsResponse, error) {
	var o ListObligationsOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	return c.pb.ListObligations(ctx, &pb.ListObligationsRequest{
		RunId: runID, OnlyUnresolved: onlyUnresolved, StatusFilter: pb.ObligationStatus(o.StatusFilter),
	})
}

func (c *Client) ResolveObligation(ctx context.Context, runID string, obligationSeq int64, status int32, resultJSON string) (*pb.ObligationRecord, error) {
	return c.pb.ResolveObligation(ctx, &pb.ResolveObligationRequest{
		RunId: runID, ObligationSeq: obligationSeq,
		Status: pb.ObligationStatus(status), ResultJson: resultJSON,
	})
}

// ListUnresolvedObligationsOpts: the cross-run drainer feed. Defaults pick up
// ready-to-run PENDING plus COMMITTED rows whose lease has expired.
type ListUnresolvedObligationsOpts struct {
	Limit                   int32
	NowMs                   int64
	IncludePending          bool
	IncludeStuck            bool
	IncludeCommittedExpired bool
}

func (c *Client) ListUnresolvedObligations(ctx context.Context, opts ListUnresolvedObligationsOpts) (*pb.ListUnresolvedObligationsResponse, error) {
	return c.pb.ListUnresolvedObligations(ctx, &pb.ListUnresolvedObligationsRequest{
		Limit: opts.Limit, NowMs: opts.NowMs,
		IncludePending: opts.IncludePending, IncludeStuck: opts.IncludeStuck,
		IncludeCommittedExpired: opts.IncludeCommittedExpired,
	})
}

func (c *Client) ClaimObligation(ctx context.Context, runID string, obligationSeq int64, claimer string, leaseTtlMs int64) (*pb.ClaimObligationResponse, error) {
	return c.pb.ClaimObligation(ctx, &pb.ClaimObligationRequest{
		RunId: runID, ObligationSeq: obligationSeq, Claimer: claimer, LeaseTtlMs: leaseTtlMs,
	})
}

func (c *Client) RecordObligationAttempt(ctx context.Context, runID string, obligationSeq int64, errMsg string, nextAttemptAtMs int64) (*pb.ObligationRecord, error) {
	return c.pb.RecordObligationAttempt(ctx, &pb.RecordObligationAttemptRequest{
		RunId: runID, ObligationSeq: obligationSeq, Error: errMsg, NextAttemptAtMs: nextAttemptAtMs,
	})
}

// ──── budget ────────────────────────────────────────────────────────────────

func (c *Client) SetBudget(ctx context.Context, runID string, usdCap float64, tokenCap int64) (*pb.BudgetState, error) {
	return c.pb.SetBudget(ctx, &pb.SetBudgetRequest{RunId: runID, UsdCap: usdCap, TokenCap: tokenCap})
}

func (c *Client) AdmitBudget(ctx context.Context, runID string, usd float64, tokens int64) (*pb.AdmitBudgetResponse, error) {
	return c.pb.AdmitBudget(ctx, &pb.AdmitBudgetRequest{RunId: runID, UsdEstimate: usd, TokenEstimate: tokens})
}

func (c *Client) ChargeBudget(ctx context.Context, runID string, usd float64, tokens int64) (*pb.BudgetState, error) {
	return c.pb.ChargeBudget(ctx, &pb.ChargeBudgetRequest{RunId: runID, Usd: usd, Tokens: tokens})
}

// ──── gates / signals ───────────────────────────────────────────────────────

func (c *Client) AwaitSignal(ctx context.Context, runID, gate, payloadJSON string) (*pb.AwaitSignalResponse, error) {
	return c.pb.AwaitSignal(ctx, &pb.AwaitSignalRequest{RunId: runID, GateName: gate, PayloadJson: payloadJSON})
}

type SendSignalOpts struct {
	RunID, AppName, UserID, SessionID, GateName, ResolutionJSON string
}

func (c *Client) SendSignal(ctx context.Context, o SendSignalOpts) (*pb.SendSignalResponse, error) {
	return c.pb.SendSignal(ctx, &pb.SendSignalRequest{
		RunId: o.RunID, AppName: o.AppName, UserId: o.UserID, SessionId: o.SessionID,
		GateName: o.GateName, ResolutionJson: o.ResolutionJSON,
	})
}

// ──── reconciliation / timers / WAL tail ────────────────────────────────────

func (c *Client) ListPendingEffects(ctx context.Context, olderThanMs int64, includePending, includeUnknown bool, limit int64) (*pb.ListPendingEffectsResponse, error) {
	return c.pb.ListPendingEffects(ctx, &pb.ListPendingEffectsRequest{
		OlderThanMs: olderThanMs, IncludePending: includePending, IncludeUnknown: includeUnknown, Limit: limit,
	})
}

type SetTimerOpts struct {
	RunID, TimerID, Kind, PayloadJSON string
	FireAtMs                          int64
}

func (c *Client) SetTimer(ctx context.Context, o SetTimerOpts) (*pb.TimerRecord, error) {
	return c.pb.SetTimer(ctx, &pb.SetTimerRequest{
		RunId: o.RunID, TimerId: o.TimerID, FireAtMs: o.FireAtMs, Kind: o.Kind, PayloadJson: o.PayloadJSON,
	})
}

func (c *Client) CancelTimer(ctx context.Context, runID, timerID string) (*pb.CancelTimerResponse, error) {
	return c.pb.CancelTimer(ctx, &pb.CancelTimerRequest{RunId: runID, TimerId: timerID})
}

func (c *Client) ListDueTimers(ctx context.Context, nowMs, limit int64, claim bool) (*pb.ListDueTimersResponse, error) {
	return c.pb.ListDueTimers(ctx, &pb.ListDueTimersRequest{NowMs: nowMs, Limit: limit, Claim: claim})
}

// SubscribeEvents returns a streaming client; iterate with .Recv() until io.EOF.
func (c *Client) SubscribeEvents(ctx context.Context, fromTsMs int64, runID, kind string) (grpc.ServerStreamingClient[pb.EventEntry], error) {
	return c.pb.SubscribeEvents(ctx, &pb.SubscribeEventsRequest{FromTsMs: fromTsMs, RunId: runID, Kind: kind})
}

// ──── ADK SessionService shim ───────────────────────────────────────────────

func (c *Client) CreateSession(ctx context.Context, app, user, session, stateJSON string) (*pb.Session, error) {
	return c.pb.CreateSession(ctx, &pb.CreateSessionRequest{AppName: app, UserId: user, SessionId: session, StateJson: stateJSON})
}

func (c *Client) GetSession(ctx context.Context, app, user, session string, maxEvents int64) (*pb.GetSessionResponse, error) {
	return c.pb.GetSession(ctx, &pb.GetSessionRequest{AppName: app, UserId: user, SessionId: session, MaxEvents: maxEvents})
}

func (c *Client) AppendEvent(ctx context.Context, app, user, session string, ev *pb.EventRecord, stateDeltaJSON string) (*pb.AppendEventResponse, error) {
	return c.pb.AppendEvent(ctx, &pb.AppendEventRequest{AppName: app, UserId: user, SessionId: session, Event: ev, StateDeltaJson: stateDeltaJSON})
}
