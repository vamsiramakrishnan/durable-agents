// Cloud Tasks connector — enqueue an HTTP target. Cloud Tasks owns
// retries, backoff, and scheduling; the connector just creates the task.
//
// Like the Pub/Sub connector, the real `cloud.google.com/go/cloudtasks`
// client is an optional dependency. The default build ships a stub
// returning ErrCloudTasksNotBuilt; build with `-tags cloudtasks` (and
// `go get cloud.google.com/go/cloudtasks`) to enable.

//go:build !cloudtasks

package connectors

import (
	"context"
	"errors"
)

// CloudTasksOpts — configuration.
type CloudTasksOpts struct {
	Name              string
	Project           string
	Location          string
	Queue             string
	TargetURL         string
	ServiceAccount    string
	ObserveURL        string
	CompensateURL     string
}

// ErrCloudTasksNotBuilt — returned by the default build.
var ErrCloudTasksNotBuilt = errors.New("connectors/cloud_tasks: build with -tags cloudtasks")

// CloudTasksConnector — Cloud Tasks-backed outbox connector.
type CloudTasksConnector struct {
	opts CloudTasksOpts
}

// NewCloudTasksConnector — construct (stub in the default build).
func NewCloudTasksConnector(opts CloudTasksOpts) *CloudTasksConnector {
	if opts.Name == "" {
		opts.Name = "tasks:" + opts.Queue
	}
	return &CloudTasksConnector{opts: opts}
}

func (c *CloudTasksConnector) Name() string { return c.opts.Name }

func (c *CloudTasksConnector) Dispatch(ctx context.Context, e Effect) (DispatchResult, error) {
	return DispatchResult{Outcome: DispatchUnknown, Error: ErrCloudTasksNotBuilt.Error()}, ErrCloudTasksNotBuilt
}

func (c *CloudTasksConnector) Observe(ctx context.Context, e Effect) (ObservationResult, error) {
	return ObservationResult{Outcome: ObservationUnknown, Error: ErrCloudTasksNotBuilt.Error()}, ErrCloudTasksNotBuilt
}

func (c *CloudTasksConnector) Compensate(ctx context.Context, o Obligation) (CompensationResult, error) {
	return CompensationResult{Outcome: CompensationStuck, Error: ErrCloudTasksNotBuilt.Error()}, ErrCloudTasksNotBuilt
}
