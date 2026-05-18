# Fan the journal out — sinks in every language

A **sink** is the consumer side of the WAL fan-out: each journal entry the
relay sees gets handed to `sink.publish(entry)`. Combined with a relay that
advances a durable cursor only after `publish` succeeds, and a consumer that
dedupes on `(run_id, seq)`, the whole thing gives **exactly-once-effective**
delivery (at-least-once relay + idempotent receipt).

Python has shipped `LogSink`, `WebhookSink`, and `PubSubSink` from day one.
TypeScript, Go, and Java now ship the same three.

| Sink            | Use it for                                    | Receiver dedupes on                                                       |
|-----------------|-----------------------------------------------|---------------------------------------------------------------------------|
| `LogSink`       | tests, demos, an audit tap                    | n/a                                                                       |
| `WebhookSink`   | a partner HTTP endpoint, internal services    | `X-Tape-Event-Id: <run_id>/<seq>` header                                  |
| `PubSubSink`    | broadcast / fan-out, multiple consumers       | `tape-event-id = <run_id>/<seq>` message attribute, `orderingKey = run_id`|

All three preserve per-run order on the wire — Pub/Sub via `orderingKey`,
Webhook via the relay's sequential `publish` calls.

## Usage

=== ":material-language-python: Python"

    ```python
    from tape.sinks import LogSink, WebhookSink, PubSubSink
    import tape.reactors

    tape.reactors.run_outbox_relay(
        "tape://localhost:7878",
        sink=WebhookSink(
            url="https://partner.example.com/tape/events",
            headers={"Authorization": "Bearer …"},
            max_retries=3,
        ),
        cursor_path="/var/lib/tape/cursor.json",     # durable — restart resumes here
    )
    ```

    `PubSubSink(project="my-proj", topic="tape-events")` is identical to
    swap in — it lazy-imports `google-cloud-pubsub`, so the dep is
    optional unless you use it.

=== ":material-language-typescript: TypeScript"

    ```ts
    import { LogSink, WebhookSink, PubSubSink, runEventFanout } from 'tape-ts';

    const sink = new WebhookSink({
      url: 'https://partner.example.com/tape/events',
      headers: { Authorization: 'Bearer …' },
      maxRetries: 3,
    });

    await runEventFanout({
      url: 'tape://localhost:7878',
      sink: (entry) => sink.publish(entry),
      subjectPattern: '/tape/**',
    });
    ```

    `PubSubSink({ project, topic })` lazy-imports `@google-cloud/pubsub`
    (declared as an `optionalDependencies` in `package.json`). If you
    don't use it, you don't have to install it.

=== ":simple-go: Go"

    ```go
    import (
        "context"
        tape "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go"
        "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/sinks"
        pb "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
    )

    sink, _ := sinks.NewWebhookSink(sinks.WebhookSinkOpts{
        URL: "https://partner.example.com/tape/events",
        Headers: map[string]string{"Authorization": "Bearer …"},
        MaxRetries: 3,
    })

    tape.RunEventFanoutBySubject(ctx, client, "/tape/**", "", 0,
        func(e *pb.EventEntry) error { return sink.Publish(ctx, e) })
    ```

    `PubSubSink` lives behind the `pubsub` build tag — `go build -tags
    pubsub ./...` enables the real `cloud.google.com/go/pubsub` client.
    The default build ships a stub that returns `ErrPubSubSinkNotBuilt`.

=== ":fontawesome-brands-java: Java"

    ```java
    import dev.tape.sinks.*;

    Sink sink = new WebhookSink(new WebhookSink.Opts()
        .url("https://partner.example.com/tape/events")
        .header("Authorization", "Bearer …")
        .maxRetries(3));

    // Wire to your event-fanout adapter:
    for (EventEntry entry : client.subscribeBySubject("/tape/**", "", 0L)) {
        sink.publish(entry);
    }
    ```

    `PubSubSink` loads `com.google.cloud:google-cloud-pubsub` reflectively
    — the jar stays a runtime-optional dependency. Add it to your agent's
    pom if you use it.

## Webhook contract

A webhook receiver should look for `X-Tape-Event-Id` and dedupe:

```http
POST /tape/events HTTP/1.1
Content-Type: application/json
X-Tape-Event-Id: r-1234/42

{"run_id":"r-1234","seq":42,"kind":"effect_confirmed",
 "payload_json":"{…}","ts_ms":1714900800123}
```

Return `2xx` to acknowledge. `4xx` is a permanent failure and won't be
retried by the sink (the relay will surface it on its next tick); `5xx` and
network errors are retried with exponential backoff (cap at
`maxRetries`).

## Pub/Sub contract

| Attribute          | Value                              |
|--------------------|------------------------------------|
| `ordering_key`     | `<run_id>`                         |
| Attribute `tape-event-id` | `<run_id>/<seq>`            |
| Attribute `kind`   | The journal kind (`effect_confirmed`, `decision`, …) |
| Body               | `{"run_id", "seq", "kind", "payload_json", "ts_ms"}` JSON |

The topic must have **message ordering enabled** for `orderingKey` to be
honoured. The subscriber dedupes on `tape-event-id`.

## At-least-once + dedup = exactly-once-effective

The relay advances its durable cursor only after `publish` returns. If the
relay crashes between `publish` succeeding and the cursor advancing, the
next run re-publishes the same entry — the receiver MUST dedupe on
`(run_id, seq)` (or `tape-event-id`). That's the model the WAL fan-out
gives you, and it's the same one the Tape server uses internally for
effect-effect dedup.

## See also

- [Reactors](../concepts/reactors.md) — the WAL fan-out belongs to the
  reactor family.
- [Outbox dispatcher](outbox-daemon.md) — the other half of the journal
  loop, now in every language.
- Python's `tape.reactors.run_outbox_relay` is the long-form relay; TS /
  Go / Java pass the sink into `runEventFanout` / `RunEventFanoutBySubject`
  / a hand-rolled loop over `subscribeBySubject`.
