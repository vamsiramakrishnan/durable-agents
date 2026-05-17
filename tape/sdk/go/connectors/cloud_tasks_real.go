//go:build cloudtasks

package connectors

import (
	"context"
	"encoding/json"
	"regexp"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
)

// CloudTasksOpts — configuration with the cloudtasks build tag.
type CloudTasksOpts struct {
	Name           string
	Project        string
	Location       string
	Queue          string
	TargetURL      string
	ServiceAccount string
	ObserveURL     string
	CompensateURL  string
}

// CloudTasksConnector — real Cloud Tasks-backed outbox connector.
type CloudTasksConnector struct {
	opts   CloudTasksOpts
	client *cloudtasks.Client
}

// NewCloudTasksConnector — construct; the client is lazily opened.
func NewCloudTasksConnector(opts CloudTasksOpts) *CloudTasksConnector {
	if opts.Name == "" {
		opts.Name = "tasks:" + opts.Queue
	}
	return &CloudTasksConnector{opts: opts}
}

func (c *CloudTasksConnector) Name() string { return c.opts.Name }

var safeTaskID = regexp.MustCompile(`[^a-zA-Z0-9\-_]`)

func (c *CloudTasksConnector) ensure(ctx context.Context) error {
	if c.client != nil {
		return nil
	}
	cli, err := cloudtasks.NewClient(ctx)
	if err != nil {
		return err
	}
	c.client = cli
	return nil
}

func (c *CloudTasksConnector) queuePath() string {
	return "projects/" + c.opts.Project + "/locations/" + c.opts.Location + "/queues/" + c.opts.Queue
}

func (c *CloudTasksConnector) Dispatch(ctx context.Context, e Effect) (DispatchResult, error) {
	if err := c.ensure(ctx); err != nil {
		return DispatchResult{Outcome: DispatchUnknown, Error: err.Error()}, nil
	}
	body, err := json.Marshal(e.Payload)
	if err != nil {
		return DispatchResult{Outcome: DispatchFailed, Error: err.Error()}, nil
	}
	taskID := safeTaskID.ReplaceAllString(e.IdempotencyKey, "-")
	if len(taskID) > 500 {
		taskID = taskID[:500]
	}
	httpReq := &taskspb.HttpRequest{
		HttpMethod: taskspb.HttpMethod_POST,
		Url:        c.opts.TargetURL,
		Headers: map[string]string{
			"Content-Type":            "application/json",
			"X-Tape-Idempotency-Key":  e.IdempotencyKey,
			"X-Tape-Run-Id":           e.RunID,
			"X-Tape-Business-Key":     e.BusinessKey,
		},
		Body: body,
	}
	if c.opts.ServiceAccount != "" {
		httpReq.AuthorizationHeader = &taskspb.HttpRequest_OidcToken{
			OidcToken: &taskspb.OidcToken{
				ServiceAccountEmail: c.opts.ServiceAccount,
				Audience:            c.opts.TargetURL,
			},
		}
	}
	task := &taskspb.Task{
		Name:        c.queuePath() + "/tasks/" + taskID,
		MessageType: &taskspb.Task_HttpRequest{HttpRequest: httpReq},
	}
	out, err := c.client.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: c.queuePath(),
		Task:   task,
	})
	if err != nil {
		// ALREADY_EXISTS means the dedup worked. That's CONFIRMED.
		if isAlreadyExists(err) {
			return DispatchResult{Outcome: DispatchConfirmed,
				Response: map[string]any{"deduped": true}}, nil
		}
		return DispatchResult{Outcome: DispatchUnknown, Error: err.Error()}, nil
	}
	return DispatchResult{Outcome: DispatchPending,
		Response: map[string]any{"name": out.Name}, DispatchID: out.Name}, nil
}

func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() != "" && (containsAny(err.Error(), "AlreadyExists", "already exists", "ALREADY_EXISTS"))
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}

func (c *CloudTasksConnector) Observe(ctx context.Context, e Effect) (ObservationResult, error) {
	if c.opts.ObserveURL != "" {
		// Chain to a small HttpConnector for the status path.
		h := NewHttpConnector(HttpOpts{URL: c.opts.ObserveURL, ObserveURL: c.opts.ObserveURL})
		return h.Observe(ctx, e)
	}
	return ObservationResult{Outcome: ObservationUnknown,
		Error: "no ObserveURL configured"}, nil
}

func (c *CloudTasksConnector) Compensate(ctx context.Context, o Obligation) (CompensationResult, error) {
	if c.opts.CompensateURL != "" {
		h := NewHttpConnector(HttpOpts{CompensateURL: c.opts.CompensateURL})
		return h.Compensate(ctx, o)
	}
	return CompensationResult{Outcome: CompensationStuck,
		Error: "no CompensateURL configured"}, nil
}
