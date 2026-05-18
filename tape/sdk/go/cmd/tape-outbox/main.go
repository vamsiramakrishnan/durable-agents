// tape-outbox — Go counterpart of `python -m tape.reactors.outbox`.
//
// Usage:
//
//	tape-outbox --url tape://localhost:7878 \
//	            --connector bank.wire \
//	            [--once] [--interval 1s] [--max-attempts 5]
//
// Connectors must be registered before the loop starts. Because Go has no
// runtime "import this Python module" hook, registration is done by linking
// against the host application — usually a tiny init() that calls
// `connectors.Default.Register(...)`. For ad-hoc operational use, the
// `LogConnector` is always available.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	tape "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go"
	"github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/connectors"
)

func main() {
	url := flag.String("url", "tape://localhost:7878", "Tape server URL")
	conn := flag.String("connector", "", "restrict to one connector name")
	interval := flag.Duration("interval", time.Second, "poll interval")
	maxAttempts := flag.Int("max-attempts", 5, "give up after N attempts (then mark FAILED)")
	claimer := flag.String("claimer", "", "identity recorded as dispatch_claimed_by")
	once := flag.Bool("once", false, "run one pass and exit")
	registerLog := flag.Bool("register-log-connector", false,
		"register the built-in LogConnector under its name (handy for tests/demos)")
	logPath := flag.String("log-connector-path", "",
		"path the LogConnector writes to (default: /tmp/tape-outbox.jsonl)")
	flag.Parse()

	if *registerLog {
		_ = connectors.Default.Register("log", connectors.NewLogConnector(*logPath))
	}

	c, err := tape.Dial(*url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tape-outbox: dial %s: %v\n", *url, err)
		os.Exit(1)
	}
	defer c.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	err = tape.RunOutboxDispatcher(ctx, c, tape.RunOutboxOptions{
		OutboxOptions: tape.OutboxOptions{
			Connector:           *conn,
			Claimer:             *claimer,
			DispatchMaxAttempts: *maxAttempts,
		},
		Interval: *interval,
		Once:     *once,
		OnTick: func(outs []tape.OutboxOutcome) {
			if len(outs) == 0 { return }
			payload, _ := json.Marshal(map[string]any{"outbox": outs})
			fmt.Println(string(payload))
		},
	})
	if err != nil && err != context.Canceled {
		fmt.Fprintf(os.Stderr, "tape-outbox: %v\n", err)
		os.Exit(1)
	}
}
