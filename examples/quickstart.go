// Tape quickstart — Go. The same scenario as the Python / TS / Java siblings.
//
//   go run examples/quickstart.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	tape "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go"
)

const lang = "go"

func main() {
	url := os.Getenv("TAPE_URL")
	if url == "" {
		url = "tape://127.0.0.1:7878"
	}
	fmt.Printf("[quickstart/%s] connecting to %s\n", lang, url)

	c, err := tape.Dial(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", url, err)
		os.Exit(1)
	}
	defer c.Close()

	ctx := context.Background()
	invocation := fmt.Sprintf("qs-%s-%d", lang, time.Now().Unix())

	run, err := c.BeginRun(ctx, tape.BeginRunOpts{
		AppName: "quickstart", UserID: "quickstart-user",
		SessionID: invocation, InvocationID: invocation,
		LeaseOwner: "qs-" + lang, LeaseTTLMs: 60_000,
	})
	check(err, "begin_run")
	fmt.Printf("[quickstart/%s] begin_run    → run-id=%s\n", lang, run.RunId)

	_, err = c.RecordDecision(ctx, run.RunId, 0, "quickstart", "{}", "{}", "", "")
	check(err, "record_decision")
	fmt.Printf("[quickstart/%s] record_decision  decision_index=0\n", lang)

	reqJSON, _ := json.Marshal(map[string]string{"who": lang})
	be, err := c.BeginEffect(ctx, tape.BeginEffectOpts{
		RunID: run.RunId, DecisionIndex: 0,
		ToolName: "hello", CallIndex: 0,
		RequestJSON: string(reqJSON),
	})
	check(err, "begin_effect")
	fmt.Printf("[quickstart/%s] begin_effect   → key=%s  status=%v\n", lang, be.IdempotencyKey, be.Status)

	respJSON, _ := json.Marshal(map[string]any{"ok": true, "who": lang})
	_, err = c.CompleteEffect(ctx, run.RunId, be.IdempotencyKey,
		tape.EffectStatusConfirmed, string(respJSON), "")
	check(err, "complete_effect")
	fmt.Printf("[quickstart/%s] complete_effect → status=CONFIRMED\n", lang)

	got, err := c.GetEffect(ctx, run.RunId, be.IdempotencyKey)
	check(err, "get_effect")
	fmt.Printf("[quickstart/%s] get_effect     status=%v  response=%s\n",
		lang, got.Effect.Status, got.Effect.ResponseJson)
}

func check(err error, where string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", where, err)
		os.Exit(1)
	}
}
