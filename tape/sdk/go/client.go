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

	// Outbox / non-idempotent contract (see proto: EffectSemantics,
	// EffectDispatchMode, EffectResolution). The defaults preserve v1 behaviour
	// (idempotent + inline); opt into the outbox path by passing the
	// non-default values to BeginEffect.
	EffectSemanticsIdempotent    = int32(pb.EffectSemantics_EFFECT_SEMANTICS_IDEMPOTENT)
	EffectSemanticsNonIdempotent = int32(pb.EffectSemantics_EFFECT_SEMANTICS_NON_IDEMPOTENT)
	EffectSemanticsObserveOnly   = int32(pb.EffectSemantics_EFFECT_SEMANTICS_OBSERVE_ONLY)

	EffectDispatchInline = int32(pb.EffectDispatchMode_EFFECT_DISPATCH_MODE_INLINE)
	EffectDispatchOutbox = int32(pb.EffectDispatchMode_EFFECT_DISPATCH_MODE_OUTBOX)

	EffectResolutionConfirmed = int32(pb.EffectResolution_EFFECT_RESOLUTION_CONFIRMED)
	EffectResolutionFailed    = int32(pb.EffectResolution_EFFECT_RESOLUTION_FAILED)
	EffectResolutionAbsent    = int32(pb.EffectResolution_EFFECT_RESOLUTION_ABSENT)
	EffectResolutionDuplicate = int32(pb.EffectResolution_EFFECT_RESOLUTION_DUPLICATE)
	EffectResolutionStuck     = int32(pb.EffectResolution_EFFECT_RESOLUTION_STUCK)

	ObligationPending     = int32(pb.ObligationStatus_OBLIGATION_STATUS_PENDING)
	ObligationCommitted   = int32(pb.ObligationStatus_OBLIGATION_STATUS_COMMITTED)
	ObligationCompensated = int32(pb.ObligationStatus_OBLIGATION_STATUS_COMPENSATED)
	ObligationStuck       = int32(pb.ObligationStatus_OBLIGATION_STATUS_STUCK)
)

// HandlerKind and TaskStatus re-exports for ergonomic call-sites.
const (
	HandlerKindAgent   = pb.HandlerKind_HANDLER_KIND_AGENT
	HandlerKindTask    = pb.HandlerKind_HANDLER_KIND_TASK
	HandlerKindPublish = pb.HandlerKind_HANDLER_KIND_PUBLISH

	TaskStatusPending = pb.TaskStatus_TASK_STATUS_PENDING
	TaskStatusClaimed = pb.TaskStatus_TASK_STATUS_CLAIMED
	TaskStatusDone    = pb.TaskStatus_TASK_STATUS_DONE
	TaskStatusFailed  = pb.TaskStatus_TASK_STATUS_FAILED
	TaskStatusDLQ     = pb.TaskStatus_TASK_STATUS_DLQ
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
	// Identity & authorization context. AIPlex-managed deployments populate
	// these from the AIPLEX_* env vars; non-AIPlex callers may leave them
	// empty. See tape/proto/tape.proto §BeginRunRequest.
	TenantID, Actor, Subject, AgentID, AIPlexInstanceID, GatewayRoute string
	Scopes                                                            []string
	Labels                                                            map[string]string
}

func (c *Client) BeginRun(ctx context.Context, o BeginRunOpts) (*pb.BeginRunResponse, error) {
	if o.LeaseTTLMs == 0 {
		o.LeaseTTLMs = 120_000
	}
	return c.pb.BeginRun(ctx, &pb.BeginRunRequest{
		AppName: o.AppName, UserId: o.UserID, SessionId: o.SessionID,
		InvocationId: o.InvocationID, LeaseOwner: o.LeaseOwner, LeaseTtlMs: o.LeaseTTLMs,
		TenantId: o.TenantID, Actor: o.Actor, Subject: o.Subject,
		AgentId: o.AgentID, AiplexInstanceId: o.AIPlexInstanceID,
		GatewayRoute: o.GatewayRoute, Scopes: o.Scopes, Labels: o.Labels,
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
	// Outbox / non-idempotent contract. Zero values keep the v1 behaviour
	// (idempotent + inline); set Semantics + DispatchMode + BusinessKey +
	// Connector to opt into the outbox path. Server refuses
	// NON_IDEMPOTENT + INLINE — surface that error to the caller.
	Semantics    int32
	DispatchMode int32
	BusinessKey  string
	Connector    string
}

func (c *Client) BeginEffect(ctx context.Context, o BeginEffectOpts) (*pb.BeginEffectResponse, error) {
	return c.pb.BeginEffect(ctx, &pb.BeginEffectRequest{
		RunId: o.RunID, DecisionIndex: o.DecisionIndex, ToolName: o.ToolName,
		CallIndex: o.CallIndex, RequestJson: o.RequestJSON, CustomKey: o.CustomKey,
		Semantics:    pb.EffectSemantics(o.Semantics),
		DispatchMode: pb.EffectDispatchMode(o.DispatchMode),
		BusinessKey:  o.BusinessKey, Connector: o.Connector,
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

// ──── outbox dispatch (for non-idempotent upstreams) ────────────────────────

// ListEffectsToDispatch returns PENDING+OUTBOX effects whose
// next_dispatch_at_ms <= now and whose dispatch lease is empty or expired.
// `connector` scopes the result to one connector name; empty means any.
func (c *Client) ListEffectsToDispatch(ctx context.Context, connector string, limit int64) (*pb.ListEffectsToDispatchResponse, error) {
	return c.pb.ListEffectsToDispatch(ctx, &pb.ListEffectsToDispatchRequest{
		Connector: connector, Limit: limit,
	})
}

// ClaimEffectDispatch is the atomic CAS lease on the dispatch slot. Returns
// acquired=false (with the current row) when another dispatcher already
// holds the lease — the loser must not call the upstream.
func (c *Client) ClaimEffectDispatch(ctx context.Context, runID, key, claimer string, leaseTTLMs int64) (*pb.ClaimEffectDispatchResponse, error) {
	return c.pb.ClaimEffectDispatch(ctx, &pb.ClaimEffectDispatchRequest{
		RunId: runID, IdempotencyKey: key, Claimer: claimer, LeaseTtlMs: leaseTTLMs,
	})
}

// RecordDispatchAttempt reports a *failed* dispatch. `nextDispatchAtMs <= 0`
// asks the server to transition the effect to UNKNOWN (the safety exit for a
// lost ack — the reconciler resolves via observe(), no blind retry); a
// positive value schedules a retry.
func (c *Client) RecordDispatchAttempt(ctx context.Context, runID, key, errMsg string, nextDispatchAtMs int64) (*pb.EffectRecord, error) {
	return c.pb.RecordDispatchAttempt(ctx, &pb.RecordDispatchAttemptRequest{
		RunId: runID, IdempotencyKey: key, Error: errMsg, NextDispatchAtMs: nextDispatchAtMs,
	})
}

// RecordExternalObservationOpts records what the counterparty said about an
// effect — the reconciler's write path. `Resolution` is one of
// EffectResolution*. When DUPLICATE + `CompensateOnDuplicateKind` is set, the
// server registers a compensation obligation atomically with the observation.
type RecordExternalObservationOpts struct {
	RunID                     string
	Key                       string
	Resolution                int32
	ExternalRef               string
	ResponseJSON              string
	ErrorJSON                 string
	CompensateOnDuplicateKind string
}

func (c *Client) RecordExternalObservation(ctx context.Context, o RecordExternalObservationOpts) (*pb.EffectRecord, error) {
	return c.pb.RecordExternalObservation(ctx, &pb.RecordExternalObservationRequest{
		RunId: o.RunID, IdempotencyKey: o.Key, Resolution: pb.EffectResolution(o.Resolution),
		ExternalRef: o.ExternalRef, ResponseJson: o.ResponseJSON, ErrorJson: o.ErrorJSON,
		CompensateOnDuplicateKind: o.CompensateOnDuplicateKind,
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
//
// Legacy entry point. New code should use SubscribeEventsOpts (which supports
// `from_global_seq` and `subject_pattern`) or SubscribeBySubject.
func (c *Client) SubscribeEvents(ctx context.Context, fromTsMs int64, runID, kind string) (grpc.ServerStreamingClient[pb.EventEntry], error) {
	return c.pb.SubscribeEvents(ctx, &pb.SubscribeEventsRequest{FromTsMs: fromTsMs, RunId: runID, Kind: kind})
}

// SubscribeEventsOpts is the rich form for the WAL tail RPC: a single struct
// covering both the legacy filters (FromTsMs / RunID / Kind) and the new
// event-bus filters (FromGlobalSeq / SubjectPattern). Mix-and-match freely;
// `from_ts_ms` is honoured only when `from_global_seq` is zero.
type SubscribeEventsOpts struct {
	FromTsMs       int64
	RunID          string
	Kind           string
	FromGlobalSeq  int64
	SubjectPattern string
}

// SubscribeEventsWith is the option-struct form of SubscribeEvents. Prefer
// this for new code; it supports the event-bus rebuild fields.
func (c *Client) SubscribeEventsWith(ctx context.Context, o SubscribeEventsOpts) (grpc.ServerStreamingClient[pb.EventEntry], error) {
	return c.pb.SubscribeEvents(ctx, &pb.SubscribeEventsRequest{
		FromTsMs:       o.FromTsMs,
		RunId:          o.RunID,
		Kind:           o.Kind,
		FromGlobalSeq:  o.FromGlobalSeq,
		SubjectPattern: o.SubjectPattern,
	})
}

// ──── reactions & tasks (event-bus surface) ─────────────────────────────────

// RegisterReactionOpts mirrors the `Reaction` proto fields the client supplies
// at registration time. Leave a field zero/empty for the server default.
type RegisterReactionOpts struct {
	ReactionID        string
	Name              string
	SubjectPattern    string
	PredicateCEL      string
	HandlerKind       pb.HandlerKind
	AgentApp          string
	PublishTarget     string
	MaxConcurrency    int32
	RateLimitPerS     int32
	DebounceMs        int32
	RetryMax          int32
	RetryBackoffMs    int32
	DLQAfterN         int32
	NumShards         int32
	BootstrapFromHead bool
}

// RegisterReaction creates (or upserts on `reaction_id`) a server-side
// reaction. The returned Reaction echoes the persisted row with its
// server-assigned `reaction_id` filled in.
func (c *Client) RegisterReaction(ctx context.Context, o RegisterReactionOpts) (*pb.Reaction, error) {
	r := &pb.Reaction{
		ReactionId:        o.ReactionID,
		Name:              o.Name,
		SubjectPattern:    o.SubjectPattern,
		PredicateCel:      o.PredicateCEL,
		HandlerKind:       o.HandlerKind,
		AgentApp:          o.AgentApp,
		PublishTarget:     o.PublishTarget,
		MaxConcurrency:    o.MaxConcurrency,
		RateLimitPerS:     o.RateLimitPerS,
		DebounceMs:        o.DebounceMs,
		RetryMax:          o.RetryMax,
		RetryBackoffMs:    o.RetryBackoffMs,
		DlqAfterN:         o.DLQAfterN,
		NumShards:         o.NumShards,
		BootstrapFromHead: o.BootstrapFromHead,
	}
	return c.pb.RegisterReaction(ctx, r)
}

// DeregisterReaction marks the reaction `deleted=true`. Returns whether the
// server flipped the bit (false on unknown id).
func (c *Client) DeregisterReaction(ctx context.Context, reactionID string) (bool, error) {
	resp, err := c.pb.DeregisterReaction(ctx, &pb.DeregisterReactionRequest{ReactionId: reactionID})
	if err != nil {
		return false, err
	}
	return resp.GetDeregistered(), nil
}

// ListReactions returns every active reaction. Pass an empty pattern to list
// all reactions; otherwise an exact-match on `subject_pattern` is applied.
func (c *Client) ListReactions(ctx context.Context, subjectPattern string) ([]*pb.Reaction, error) {
	resp, err := c.pb.ListReactions(ctx, &pb.ListReactionsRequest{SubjectPattern: subjectPattern})
	if err != nil {
		return nil, err
	}
	return resp.GetReactions(), nil
}

// ClaimTasksOpts is the option-struct form of the ClaimTasks RPC.
//
// Defaults applied client-side: LeaseMs <=0 → 60_000, Max <=0 → 16. Shard <0
// asks the server for tasks from any shard.
type ClaimTasksOpts struct {
	ReactionID string
	Shard      int32 // <0 = any
	Owner      string
	LeaseMs    int64 // <=0 = 60_000
	Max        int32 // <=0 = 16
	NowMs      int64 // 0 = server time
}

// ClaimTasks atomically leases up to `Max` pending tasks for the dispatcher.
func (c *Client) ClaimTasks(ctx context.Context, o ClaimTasksOpts) ([]*pb.Task, error) {
	if o.LeaseMs <= 0 {
		o.LeaseMs = 60_000
	}
	if o.Max <= 0 {
		o.Max = 16
	}
	resp, err := c.pb.ClaimTasks(ctx, &pb.ClaimTasksRequest{
		ReactionId: o.ReactionID,
		Shard:      o.Shard,
		Owner:      o.Owner,
		LeaseMs:    o.LeaseMs,
		Max:        o.Max,
		NowMs:      o.NowMs,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetTasks(), nil
}

// CompleteTask marks the task as DONE (success ack from the handler).
func (c *Client) CompleteTask(ctx context.Context, taskID, owner string) (*pb.Task, error) {
	resp, err := c.pb.CompleteTask(ctx, &pb.CompleteTaskRequest{TaskId: taskID, Owner: owner})
	if err != nil {
		return nil, err
	}
	return resp.GetTask(), nil
}

// NackTask reports a failed attempt. Set `permanent=true` to push it straight
// to DLQ; otherwise the server will re-lease until `dlq_after_n` is reached.
func (c *Client) NackTask(ctx context.Context, taskID, owner, errMsg string, permanent bool) (*pb.Task, error) {
	resp, err := c.pb.NackTask(ctx, &pb.NackTaskRequest{
		TaskId: taskID, Owner: owner, Error: errMsg, Permanent: permanent,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetTask(), nil
}

// ListTasks returns tasks filtered by reaction and status (useful for
// observability and DLQ inspection).
func (c *Client) ListTasks(ctx context.Context, reactionID string, status pb.TaskStatus, limit int32) ([]*pb.Task, error) {
	resp, err := c.pb.ListTasks(ctx, &pb.ListTasksRequest{
		ReactionId: reactionID, Status: status, Limit: limit,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetTasks(), nil
}

// SubscribeBySubject opens a streaming WAL tail filtered by subject pattern
// and an optional CEL predicate, cursored on `global_seq`. Iterate with
// .Recv() until io.EOF.
func (c *Client) SubscribeBySubject(ctx context.Context, subjectPattern, predicateCEL string, fromGlobalSeq int64) (grpc.ServerStreamingClient[pb.EventEntry], error) {
	return c.pb.SubscribeBySubject(ctx, &pb.SubscribeBySubjectRequest{
		SubjectPattern: subjectPattern,
		PredicateCel:   predicateCEL,
		FromGlobalSeq:  fromGlobalSeq,
	})
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
