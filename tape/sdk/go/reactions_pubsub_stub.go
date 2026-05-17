//go:build !pubsub

package tape

import "context"

// RunPubSubBridge — stub when not built with `-tags pubsub`. Returns
// ErrPubSubNotBuilt so callers get a clear message instead of a silent no-op.
//
// To enable: add `cloud.google.com/go/pubsub` to your module and build with
// `-tags pubsub`. See `reactions_pubsub.go` for the active implementation.
func RunPubSubBridge(ctx context.Context, c *Client, opts RunPubSubBridgeOpts) error {
	_ = ctx
	_ = c
	_ = opts
	return ErrPubSubNotBuilt
}
