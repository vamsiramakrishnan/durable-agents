package connectors

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LogConnector — append each dispatch/observe/compensate as a JSON line.
// Useful for tests, demos, and the non-idempotent-bank example.
type LogConnector struct {
	name string
	path string
	mu   sync.Mutex
}

// NewLogConnector — open / create the JSON-lines file at `path`.
func NewLogConnector(path string) *LogConnector {
	if path == "" {
		path = "/tmp/tape-outbox.jsonl"
	}
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	return &LogConnector{name: "log", path: path}
}

func (c *LogConnector) Name() string { return c.name }

func (c *LogConnector) append(kind string, body any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	f, err := os.OpenFile(c.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	rec := map[string]any{"kind": kind, "ts_ms": time.Now().UnixMilli(), "body": body}
	enc, _ := json.Marshal(rec)
	if _, err := fmt.Fprintln(f, string(enc)); err != nil {
		return err
	}
	return nil
}

func (c *LogConnector) Dispatch(ctx context.Context, e Effect) (DispatchResult, error) {
	_ = c.append("dispatch", e)
	return DispatchResult{Outcome: DispatchConfirmed, Response: map[string]any{"logged": true}}, nil
}

func (c *LogConnector) Observe(ctx context.Context, e Effect) (ObservationResult, error) {
	_ = c.append("observe", e)
	return ObservationResult{Outcome: ObservationConfirmed, Count: 1}, nil
}

func (c *LogConnector) Compensate(ctx context.Context, o Obligation) (CompensationResult, error) {
	_ = c.append("compensate", o)
	return CompensationResult{Outcome: CompensationCompensated}, nil
}
