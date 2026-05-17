# reactive-kv-coordination

Two agents coordinate through the Tape KV (`set_value` / `watch_value`).
One agent updates an FX rate; another agent watches the key and re-prices on
each transition.

```python
import tape
tape.set_value("fx", "EURUSD", 1.0850, writer="rates-feed")

for evt in tape.watch_value("fx", "EURUSD", from_version=0):
    print(evt.prev_value_json, "→", evt.value.value_json)
```

The KV is journaled, versioned, watchable, and survives crashes. Both
agents see the *transition* — not just the latest value.
