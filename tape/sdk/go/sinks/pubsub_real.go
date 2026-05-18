//go:build pubsub

package sinks

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
	pb "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
)

// PubSubSinkOpts — configuration for the Pub/Sub sink (real build).
type PubSubSinkOpts struct {
	Project string
	Topic   string
}

// PubSubSink — publishes each entry to Cloud Pub/Sub. `OrderingKey = run_id`
// so subscribers preserve per-run order when they enable ordered delivery.
// The `tape-event-id` attribute (= `run_id/seq`) is what consumers dedup on
// (Pub/Sub assigns its own message_id).
type PubSubSink struct {
	mu     sync.Mutex
	client *pubsub.Client
	topic  *pubsub.Topic
	opts   PubSubSinkOpts
}

// NewPubSubSink — open a client + topic handle. The publisher uses message
// ordering; the Cloud Pub/Sub topic must have ordering enabled.
func NewPubSubSink(opts PubSubSinkOpts) (*PubSubSink, error) {
	if opts.Project == "" || opts.Topic == "" {
		return nil, errors.New("PubSubSink: Project and Topic are required")
	}
	ctx := context.Background()
	cli, err := pubsub.NewClient(ctx, opts.Project)
	if err != nil { return nil, fmt.Errorf("PubSubSink: %w", err) }
	t := cli.Topic(opts.Topic)
	t.EnableMessageOrdering = true
	return &PubSubSink{client: cli, topic: t, opts: opts}, nil
}

// Publish — fire one journal entry into Pub/Sub.
func (s *PubSubSink) Publish(ctx context.Context, e *pb.EventEntry) error {
	body, err := marshalEntry(e); if err != nil { return err }
	res := s.topic.Publish(ctx, &pubsub.Message{
		Data:        body,
		OrderingKey: e.GetRunId(),
		Attributes: map[string]string{
			"tape-event-id": fmt.Sprintf("%s/%d", e.GetRunId(), e.GetSeq()),
			"kind":          e.GetKind(),
		},
	})
	_, err = res.Get(ctx)
	return err
}

// Close — flush pending publishes and stop the topic.
func (s *PubSubSink) Close() error {
	s.mu.Lock(); defer s.mu.Unlock()
	if s.topic != nil { s.topic.Stop() }
	if s.client != nil { return s.client.Close() }
	return nil
}
