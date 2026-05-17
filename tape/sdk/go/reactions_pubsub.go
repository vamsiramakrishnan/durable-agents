//go:build pubsub

package tape

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
)

// RunPubSubBridge pulls PUBLISH-kind tasks and publishes them to a Pub/Sub
// topic. The message body is the task's `payload_json` (UTF-8 bytes);
// attributes carry `tape-task-id`, `tape-reaction-id`, `tape-subject`,
// `tape-global-seq`, `tape-trace-id`. The Pub/Sub `OrderingKey` is the
// source `run_id` so per-run order is preserved at the subscriber (if it
// enabled ordered delivery).
//
// If `opts.ReactionID` is empty, every registered PUBLISH reaction is pulled
// in turn (registering them first if they haven't been). `opts.Once=true`
// returns after a single pass over every reaction (handy for tests).
func RunPubSubBridge(ctx context.Context, c *Client, opts RunPubSubBridgeOpts) error {
	if opts.Owner == "" {
		opts.Owner = defaultOwner()
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 500 * time.Millisecond
	}
	if opts.ClaimMax <= 0 {
		opts.ClaimMax = 32
	}
	if opts.LeaseMs <= 0 {
		opts.LeaseMs = 60_000
	}

	rids, err := pubsubReactionIDs(ctx, c, opts.ReactionID)
	if err != nil {
		return fmt.Errorf("resolve PUBLISH reactions: %w", err)
	}

	cli, err := pubsub.NewClient(ctx, opts.Project)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient(%s): %w", opts.Project, err)
	}
	defer cli.Close()

	topic := cli.Topic(opts.Topic)
	topic.EnableMessageOrdering = true
	defer topic.Stop()

	for {
		didAny := false
		for _, rid := range rids {
			tasks, err := c.ClaimTasks(ctx, ClaimTasksOpts{
				ReactionID: rid, Shard: -1, Owner: opts.Owner,
				LeaseMs: opts.LeaseMs, Max: opts.ClaimMax,
			})
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				continue
			}
			for _, t := range tasks {
				didAny = true
				msg := &pubsub.Message{
					Data:        []byte(t.GetPayloadJson()),
					OrderingKey: t.GetSourceRunId(),
					Attributes: map[string]string{
						"tape-task-id":     t.GetTaskId(),
						"tape-reaction-id": t.GetReactionId(),
						"tape-subject":     t.GetSubject(),
						"tape-global-seq":  strconv.FormatInt(t.GetSourceGlobalSeq(), 10),
						"tape-trace-id":    t.GetTraceId(),
					},
				}
				res := topic.Publish(ctx, msg)
				if _, perr := res.Get(ctx); perr != nil {
					// Defer the DLQ decision to the server: it knows the
					// reaction's `dlq_after_n` and will promote once
					// attempts exceed it.
					_, _ = c.NackTask(ctx, t.GetTaskId(), opts.Owner,
						fmt.Sprintf("pubsub-publish: %v", perr), false)
					continue
				}
				_, _ = c.CompleteTask(ctx, t.GetTaskId(), opts.Owner)
			}
		}
		if opts.Once {
			return nil
		}
		if !didAny {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(opts.PollInterval):
			}
		}
	}
}
