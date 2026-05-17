//go:build pubsub

package connectors

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"cloud.google.com/go/pubsub"
)

// PubSubOpts — configuration with the pubsub build tag.
type PubSubOpts struct {
	Name            string
	Project         string
	Topic           string
	CompensateTopic string
	TapeURL         string
}

// PubSubConnector — Pub/Sub-backed outbox connector (real impl).
type PubSubConnector struct {
	opts   PubSubOpts
	client *pubsub.Client
}

// NewPubSubConnector — construct; the client is lazily opened on first use.
func NewPubSubConnector(opts PubSubOpts) *PubSubConnector {
	if opts.Name == "" {
		opts.Name = "pubsub:" + opts.Topic
	}
	return &PubSubConnector{opts: opts}
}

func (c *PubSubConnector) Name() string { return c.opts.Name }

func (c *PubSubConnector) ensure(ctx context.Context) error {
	if c.client != nil {
		return nil
	}
	cli, err := pubsub.NewClient(ctx, c.opts.Project)
	if err != nil {
		return err
	}
	c.client = cli
	return nil
}

func (c *PubSubConnector) Dispatch(ctx context.Context, e Effect) (DispatchResult, error) {
	if err := c.ensure(ctx); err != nil {
		return DispatchResult{Outcome: DispatchUnknown, Error: err.Error()}, nil
	}
	body, err := json.Marshal(e.Payload)
	if err != nil {
		return DispatchResult{Outcome: DispatchFailed, Error: err.Error()}, nil
	}
	t := c.client.Topic(c.opts.Topic)
	t.EnableMessageOrdering = true
	res := t.Publish(ctx, &pubsub.Message{
		Data:        body,
		OrderingKey: e.RunID,
		Attributes: map[string]string{
			"tape_idempotency_key": e.IdempotencyKey,
			"tape_run_id":          e.RunID,
			"tape_business_key":    e.BusinessKey,
			"tape_tool":            e.ToolName,
			"tape_attempt":         strconv.Itoa(e.Attempt),
		},
	})
	id, err := res.Get(ctx)
	if err != nil {
		return DispatchResult{Outcome: DispatchUnknown, Error: err.Error()}, nil
	}
	return DispatchResult{Outcome: DispatchConfirmed,
		Response: map[string]any{"message_id": id}, DispatchID: id}, nil
}

// Observe — read a result that a subscriber wrote to a Tape value via
// `tape.SetValue(...)`. The contract: the subscriber writes
// `("outbox/<connector>", idempotency_key) -> {"count": N, ...}`.
// To keep this file dep-light, we rely on the user supplying a
// `TapeURL`; we use the value namespace convention.
func (c *PubSubConnector) Observe(ctx context.Context, e Effect) (ObservationResult, error) {
	// We can't depend on the parent `tape` package here (import cycle), so
	// we leave Observe() as a documented hook: callers should override or
	// chain to an HttpConnector for the status path.
	return ObservationResult{Outcome: ObservationUnknown,
		Error: "PubSubConnector.Observe: configure a status sidecar via HttpConnector.ObserveURL"}, nil
}

func (c *PubSubConnector) Compensate(ctx context.Context, o Obligation) (CompensationResult, error) {
	if c.opts.CompensateTopic == "" {
		return CompensationResult{Outcome: CompensationStuck,
			Error: "no CompensateTopic configured"}, nil
	}
	if err := c.ensure(ctx); err != nil {
		return CompensationResult{Outcome: CompensationPending, Error: err.Error()}, nil
	}
	body, err := json.Marshal(o.Payload)
	if err != nil {
		return CompensationResult{Outcome: CompensationFailed, Error: err.Error()}, nil
	}
	t := c.client.Topic(c.opts.CompensateTopic)
	t.EnableMessageOrdering = true
	res := t.Publish(ctx, &pubsub.Message{
		Data:        body,
		OrderingKey: o.RunID,
		Attributes: map[string]string{
			"tape_obligation_kind": o.Kind,
			"tape_effect_key":      o.EffectKey,
			"tape_run_id":          o.RunID,
		},
	})
	id, err := res.Get(ctx)
	if err != nil {
		return CompensationResult{Outcome: CompensationPending, Error: err.Error()}, nil
	}
	return CompensationResult{Outcome: CompensationCompensated,
		Response: map[string]any{"message_id": id}}, nil
}

// Ensure the connector is referenced even when unused in some build paths.
var _ = fmt.Sprintf
