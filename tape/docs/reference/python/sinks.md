# Sinks

A *sink* is the consumer side of the WAL fan-out. The relay reads journal
entries via `SubscribeEvents` (or `SubscribeBySubject`) and hands each one to
`sink.publish(entry)`. Combined with a durable cursor and a consumer-side
dedup on `(run_id, seq)`, that gives **exactly-once-effective** delivery.

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
    cursor_path="/var/lib/tape/cursor.json",
)
```

See [**how-to: sinks**](../../how-to/sinks.md) for the cross-language story
and the Webhook / Pub/Sub wire contracts.

::: tape.sinks.Sink

::: tape.sinks.LogSink

::: tape.sinks.WebhookSink

::: tape.sinks.PubSubSink

::: tape.sinks.FnSink
