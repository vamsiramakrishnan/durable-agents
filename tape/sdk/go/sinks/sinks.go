// Package sinks — WAL fan-out destinations.
//
// A Sink is `Publish(ctx, entry) error` (+ optional `Close()`). Combined with
// RunEventFanout it gives an at-least-once relay; pair with consumer-side
// dedup on (run_id, seq) for exactly-once-effective delivery.
//
// Built-in sinks:
//
//   * LogSink     — writes one JSON line per entry; useful as a tap.
//   * WebhookSink — POSTs each entry with X-Tape-Event-Id; retries with backoff.
//   * PubSubSink  — publishes to Cloud Pub/Sub with ordering_key=run_id.
//                   Lazy: builds without the pubsub Go client unless
//                   `-tags pubsub` is used (same pattern as connectors/pubsub).
package sinks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
)

// Sink — what RunEventFanout adapters call for every journal entry.
type Sink interface {
	Publish(ctx context.Context, entry *pb.EventEntry) error
	Close() error
}

// Entry — the on-wire JSON representation of one journal entry. Stable;
// receivers dedupe on (run_id, seq).
type Entry struct {
	RunID       string `json:"run_id"`
	Seq         int64  `json:"seq"`
	Kind        string `json:"kind"`
	PayloadJSON string `json:"payload_json"`
	TsMs        int64  `json:"ts_ms"`
}

func marshalEntry(e *pb.EventEntry) ([]byte, error) {
	return json.Marshal(Entry{
		RunID:       e.GetRunId(),
		Seq:         int64(e.GetSeq()),
		Kind:        e.GetKind(),
		PayloadJSON: e.GetPayloadJson(),
		TsMs:        e.GetTsMs(),
	})
}

// ── LogSink ─────────────────────────────────────────────────────────────────

// LogSink appends one JSON line per entry. `path=""` or `":stderr"` writes to
// stderr; `":stdout"` to stdout.
type LogSink struct {
	mu sync.Mutex
	w  io.Writer
	f  *os.File
}

func NewLogSink(path string) (*LogSink, error) {
	if path == "" || path == ":stderr" {
		return &LogSink{w: os.Stderr}, nil
	}
	if path == ":stdout" {
		return &LogSink{w: os.Stdout}, nil
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil { return nil, err }
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil { return nil, err }
	return &LogSink{w: f, f: f}, nil
}

func (s *LogSink) Publish(_ context.Context, e *pb.EventEntry) error {
	b, err := marshalEntry(e); if err != nil { return err }
	s.mu.Lock(); defer s.mu.Unlock()
	if _, err := s.w.Write(append(b, '\n')); err != nil { return err }
	return nil
}

func (s *LogSink) Close() error {
	s.mu.Lock(); defer s.mu.Unlock()
	if s.f != nil { return s.f.Close() }
	return nil
}

// ── WebhookSink ─────────────────────────────────────────────────────────────

// WebhookSinkOpts — POST each entry as JSON.
type WebhookSinkOpts struct {
	URL              string
	Headers          map[string]string
	MaxRetries       int
	InitialBackoff   time.Duration
	Timeout          time.Duration
	HTTPClient       *http.Client
}

// WebhookSink — POST each entry as JSON. Sets `X-Tape-Event-Id: run_id/seq`.
type WebhookSink struct {
	opts WebhookSinkOpts
	cli  *http.Client
}

func NewWebhookSink(opts WebhookSinkOpts) (*WebhookSink, error) {
	if opts.URL == "" { return nil, errors.New("WebhookSink: opts.URL required") }
	if opts.MaxRetries <= 0 { opts.MaxRetries = 3 }
	if opts.InitialBackoff <= 0 { opts.InitialBackoff = 500 * time.Millisecond }
	if opts.Timeout <= 0 { opts.Timeout = 10 * time.Second }
	cli := opts.HTTPClient
	if cli == nil { cli = &http.Client{Timeout: opts.Timeout} }
	return &WebhookSink{opts: opts, cli: cli}, nil
}

func (s *WebhookSink) Publish(ctx context.Context, e *pb.EventEntry) error {
	body, err := marshalEntry(e); if err != nil { return err }
	delay := s.opts.InitialBackoff
	var lastErr error
	for i := 0; i < s.opts.MaxRetries; i++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.opts.URL, bytes.NewReader(body))
		if err != nil { return err }
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Tape-Event-Id", fmt.Sprintf("%s/%d", e.GetRunId(), e.GetSeq()))
		for k, v := range s.opts.Headers { req.Header.Set(k, v) }
		resp, err := s.cli.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body); resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 { return nil }
			lastErr = fmt.Errorf("webhook %s returned HTTP %d", s.opts.URL, resp.StatusCode)
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done(): return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
	if lastErr == nil { lastErr = fmt.Errorf("webhook %s exhausted retries", s.opts.URL) }
	return lastErr
}

func (s *WebhookSink) Close() error { return nil }

// ── FnSink ──────────────────────────────────────────────────────────────────

// FnSink — wraps a function as a Sink.
type FnSink func(ctx context.Context, entry *pb.EventEntry) error

func (f FnSink) Publish(ctx context.Context, e *pb.EventEntry) error { return f(ctx, e) }
func (FnSink) Close() error { return nil }
