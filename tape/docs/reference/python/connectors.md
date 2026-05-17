# Connectors

A *capability connector* knows how to talk to one upstream. Three operations cover the three
places Tape touches the outside world: `dispatch`, `observe`, `compensate`.

```python
from tape import connectors
from tape.connectors.http import HTTPConnector

connectors.register(HTTPConnector(
    name="bank.wire",
    endpoint="https://bank.example/wires",
    observe_endpoint="https://bank.example/wires/lookup",
    compensate_endpoint="https://bank.example/wires/reverse",
))
```

The outbox reactor matches `@tape.effect(connector=...)` to a registered connector by name and
invokes it under a CAS lease. See
[**Non-idempotent upstreams**](../../non-idempotent-upstreams.md) for the wider contract.

## Protocol

::: tape.connectors.base.EffectConnector
    options:
      heading_level: 3
::: tape.connectors.base.DispatchResult
    options:
      heading_level: 3
::: tape.connectors.base.ObservationResult
    options:
      heading_level: 3
::: tape.connectors.base.CompensationResult
    options:
      heading_level: 3

## Registry

::: tape.connectors.base.register
    options:
      heading_level: 3
::: tape.connectors.base.get
    options:
      heading_level: 3
::: tape.connectors.base.all_registered
    options:
      heading_level: 3
::: tape.connectors.base.clear
    options:
      heading_level: 3

## Built-in connectors

::: tape.connectors.http.HTTPConnector
    options:
      heading_level: 3
      members_order: source

::: tape.connectors.pubsub.PubSubConnector
    options:
      heading_level: 3
      members_order: source
