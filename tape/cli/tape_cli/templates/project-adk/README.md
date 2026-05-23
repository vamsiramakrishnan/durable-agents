# {{ name }}

A durable ADK agent on **Tape** — the embedded tier. No separate server:
`tape-adk` extends ADK's own `DatabaseSessionService` with an effect
ledger, an obligation ledger, and the reactors that make non-idempotent
tool calls exactly-once-effective.

## Run it

```bash
pip install -e .
tape dev          # runs the reactor loop + a live journal view
```

`tape dev` starts the outbox / reconciler / compensation / timer reactors
against your store and opens a live view of the journal. In another
terminal, drive your agent however you like — `adk run app`, `adk web`,
or your own script that calls `app.agent.build_runner()`. Effects appear
in the `tape dev` view as they're journaled.

## Inspect

```bash
tape inspect-adk <session-id> --app {{ name }} --user <user>   # snapshot
tape doctor --live --db-url "$TAPE_ADK_DB_URL"                 # triage
```

## Layout

```
{{ name }}/
  tape.yaml              project config (tier: adk)
  pyproject.toml         depends on tape-adk
  app/
    agent.py             the ADK agent + TapeSessionService + the plugin
    tools.py             @effect + @outbox_tool tools
    connectors.py        the connectors the outbox reactor dispatches through
```

## The contract

* `@effect` tools — idempotent. Journaled, replayed on recovery.
* `@outbox_tool` tools — non-idempotent. Never run inline; the outbox
  reactor dispatches them exactly once through a connector, even across
  crashes. A lost ack becomes `UNKNOWN`, which the reconciler resolves
  by asking the upstream — never a blind retry.

## Production

Swap the SQLite `db_url` in `tape.yaml` for `postgresql+asyncpg://…`.
Nothing else changes. Run the reactor loop as its own process /
container: `python -m tape_adk --db-url "$TAPE_ADK_DB_URL"
--connectors app.connectors:CONNECTORS`.
