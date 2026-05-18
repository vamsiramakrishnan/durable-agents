//go:build !pubsub

package sinks

import (
	"context"
	"errors"

	pb "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
)

// PubSubSinkOpts — configuration for the Pub/Sub sink.
type PubSubSinkOpts struct {
	Project string
	Topic   string
}

// ErrPubSubSinkNotBuilt — the default build ships a stub. Build with
// `-tags pubsub` (and `go get cloud.google.com/go/pubsub`) to enable.
var ErrPubSubSinkNotBuilt = errors.New("sinks/pubsub: build with -tags pubsub")

// PubSubSink — stub. Use `-tags pubsub` for the real one.
type PubSubSink struct{ opts PubSubSinkOpts }

// NewPubSubSink — returns a stub that errors on Publish.
func NewPubSubSink(opts PubSubSinkOpts) (*PubSubSink, error) {
	return nil, ErrPubSubSinkNotBuilt
}

// Publish — always errors in the stub build.
func (s *PubSubSink) Publish(ctx context.Context, _ *pb.EventEntry) error {
	return ErrPubSubSinkNotBuilt
}

// Close — no-op in the stub build.
func (s *PubSubSink) Close() error { return nil }
