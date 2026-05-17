package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// HttpOpts — configuration for `NewHttpConnector`.
type HttpOpts struct {
	Name           string         // defaults to "http"
	URL            string         // POST target for Dispatch
	ObserveURL     string         // POST target for Observe (status lookup)
	CompensateURL  string         // POST target for Compensate
	Timeout        time.Duration  // default 30s
	Headers        map[string]string
	HttpClient     *http.Client   // optional override (e.g. mTLS)
}

// HttpConnector — POST the intent payload to an HTTPS endpoint. Headers:
//   X-Tape-Idempotency-Key  the runner-derived dedup key
//   X-Tape-Business-Key     when supplied by the outbox tool
//   X-Tape-Run-Id           for traceability
//   X-Tape-Attempt          dispatch attempt #
//
// Outcome mapping: 2xx => CONFIRMED, 4xx => FAILED, 5xx / network =>
// UNKNOWN (the reactor will Observe()).
type HttpConnector struct {
	opts HttpOpts
	cli  *http.Client
}

// NewHttpConnector — construct with sane defaults.
func NewHttpConnector(opts HttpOpts) *HttpConnector {
	if opts.Name == "" {
		opts.Name = "http"
	}
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	cli := opts.HttpClient
	if cli == nil {
		cli = &http.Client{Timeout: opts.Timeout}
	}
	return &HttpConnector{opts: opts, cli: cli}
}

func (c *HttpConnector) Name() string { return c.opts.Name }

func (c *HttpConnector) post(ctx context.Context, url string, body any,
	idempKey, runID, bizKey string, attempt int) (int, []byte, error) {
	if url == "" {
		return 0, nil, fmt.Errorf("http connector %q: url unset", c.opts.Name)
	}
	b, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tape-Idempotency-Key", idempKey)
	if runID != "" {
		req.Header.Set("X-Tape-Run-Id", runID)
	}
	if bizKey != "" {
		req.Header.Set("X-Tape-Business-Key", bizKey)
	}
	if attempt > 0 {
		req.Header.Set("X-Tape-Attempt", strconv.Itoa(attempt))
	}
	for k, v := range c.opts.Headers {
		req.Header.Set(k, v)
	}
	resp, err := c.cli.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	buf, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, buf, nil
}

func (c *HttpConnector) Dispatch(ctx context.Context, e Effect) (DispatchResult, error) {
	status, body, err := c.post(ctx, c.opts.URL, e.Payload,
		e.IdempotencyKey, e.RunID, e.BusinessKey, e.Attempt)
	if err != nil {
		return DispatchResult{Outcome: DispatchUnknown, Error: err.Error()}, nil
	}
	if status >= 200 && status < 300 {
		var parsed any
		_ = json.Unmarshal(body, &parsed)
		return DispatchResult{Outcome: DispatchConfirmed, Response: parsed}, nil
	}
	if status >= 400 && status < 500 {
		return DispatchResult{Outcome: DispatchFailed, Error: fmt.Sprintf("http %d", status),
			Response: string(body)}, nil
	}
	return DispatchResult{Outcome: DispatchUnknown, Error: fmt.Sprintf("http %d", status),
		Response: string(body)}, nil
}

func (c *HttpConnector) Observe(ctx context.Context, e Effect) (ObservationResult, error) {
	if c.opts.ObserveURL == "" {
		return ObservationResult{Outcome: ObservationUnknown, Error: "no ObserveURL configured"}, nil
	}
	probe := map[string]any{
		"idempotency_key": e.IdempotencyKey,
		"business_key":    e.BusinessKey,
		"payload":         e.Payload,
	}
	status, body, err := c.post(ctx, c.opts.ObserveURL, probe,
		e.IdempotencyKey, e.RunID, e.BusinessKey, e.Attempt)
	if err != nil {
		return ObservationResult{Outcome: ObservationUnknown, Error: err.Error()}, nil
	}
	if status != 200 {
		return ObservationResult{Outcome: ObservationUnknown,
			Error: fmt.Sprintf("http %d", status), Response: string(body)}, nil
	}
	var parsed map[string]any
	_ = json.Unmarshal(body, &parsed)
	count, _ := parsed["count"].(float64)
	switch int(count) {
	case 0:
		return ObservationResult{Outcome: ObservationAbsent, Response: parsed, Count: 0}, nil
	case 1:
		return ObservationResult{Outcome: ObservationConfirmed, Response: parsed, Count: 1}, nil
	default:
		return ObservationResult{Outcome: ObservationDuplicate, Response: parsed, Count: int(count)}, nil
	}
}

func (c *HttpConnector) Compensate(ctx context.Context, o Obligation) (CompensationResult, error) {
	if c.opts.CompensateURL == "" {
		return CompensationResult{Outcome: CompensationStuck, Error: "no CompensateURL configured"}, nil
	}
	status, body, err := c.post(ctx, c.opts.CompensateURL, o.Payload,
		o.EffectKey, o.RunID, "", o.Attempt)
	if err != nil {
		return CompensationResult{Outcome: CompensationPending, Error: err.Error()}, nil
	}
	if status >= 200 && status < 300 {
		var parsed any
		_ = json.Unmarshal(body, &parsed)
		return CompensationResult{Outcome: CompensationCompensated, Response: parsed}, nil
	}
	if status >= 400 && status < 500 {
		return CompensationResult{Outcome: CompensationFailed, Error: fmt.Sprintf("http %d", status),
			Response: string(body)}, nil
	}
	return CompensationResult{Outcome: CompensationPending, Error: fmt.Sprintf("http %d", status),
		Response: string(body)}, nil
}
