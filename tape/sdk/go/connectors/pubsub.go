// Pub/Sub connector — publish the intent as a Pub/Sub message.
//
// The real google-cloud-pubsub client (`cloud.google.com/go/pubsub`) is an
// optional dependency. To keep the SDK build light, this file ships in two
// halves: the always-compiled stub (this file) which reports "not built"
// errors, and a `pubsub_real.go` that's compiled with `-tags pubsub`. This
// mirrors the Pub/Sub bridge in `reactions_pubsub.go`.

//go:build !pubsub

package connectors

import (
	"context"
	"errors"
)

// PubSubOpts — configuration. With the default build, these values are
// preserved on the connector but `Dispatch` returns ErrPubSubNotBuilt.
type PubSubOpts struct {
	Name            string
	Project         string
	Topic           string
	CompensateTopic string
	TapeURL         string // for the Observe() path via tape.GetValue
}

// ErrPubSubNotBuilt — returned by the default build of the Pub/Sub
// connector. Build with `-tags pubsub` to enable, and `go get
// cloud.google.com/go/pubsub`.
var ErrPubSubNotBuilt = errors.New("connectors/pubsub: build with -tags pubsub")

// PubSubConnector — Pub/Sub-backed outbox connector.
type PubSubConnector struct {
	opts PubSubOpts
}

// NewPubSubConnector — construct (Dispatch returns ErrPubSubNotBuilt in
// the default build).
func NewPubSubConnector(opts PubSubOpts) *PubSubConnector {
	if opts.Name == "" {
		opts.Name = "pubsub:" + opts.Topic
	}
	return &PubSubConnector{opts: opts}
}

func (c *PubSubConnector) Name() string { return c.opts.Name }

func (c *PubSubConnector) Dispatch(ctx context.Context, e Effect) (DispatchResult, error) {
	return DispatchResult{Outcome: DispatchUnknown, Error: ErrPubSubNotBuilt.Error()}, ErrPubSubNotBuilt
}

func (c *PubSubConnector) Observe(ctx context.Context, e Effect) (ObservationResult, error) {
	return ObservationResult{Outcome: ObservationUnknown, Error: ErrPubSubNotBuilt.Error()}, ErrPubSubNotBuilt
}

func (c *PubSubConnector) Compensate(ctx context.Context, o Obligation) (CompensationResult, error) {
	return CompensationResult{Outcome: CompensationStuck, Error: ErrPubSubNotBuilt.Error()}, ErrPubSubNotBuilt
}
