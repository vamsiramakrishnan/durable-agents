# AIPlex ↔ Tape Integration — Phase 0 Survey (Tape side)

Status: survey only. No behavioural changes are made by this document.
Companion survey: `aiplex/docs/integration/aiplex-tape-survey.md`.

This note inventories the Tape surfaces that an upcoming integration with
[AIPlex](https://github.com/vamsiramakrishnan/aiplex) will touch. AIPlex
intends to use Tape as a managed durable-runtime backend: AIPlex governs
identity, scopes, consent, routing, catalog, deployment and policy; Tape
governs run journals, model decisions, effects, replay, leases, gates,
timers, budgets, reconciliation and compensation.

The guiding invariant:

> AIPlex decides whether an agent is allowed to act.
> Tape proves what happened when it acted.

Tape stays independently usable. AIPlex-specific context flows in as
generic run metadata + labels — Tape does not take a build-time
dependency on AIPlex.

---

## 1. Proto / IDL

- File: `tape/proto/tape.proto`
- Run creation: `BeginRunRequest` / `BeginRunResponse` (around lines 120–133).
  Fields today: `app_name`, `user_id`, `session_id`, `invocation_id`,
  `lease_owner`, `lease_ttl_ms`.
- Run state: `RunState` (~lines 178–191). Fixed columns, no general
  metadata map.
- Journal: `JournalEntry` (~lines 193–205). Has `subject`,
  `schema_version`, `trace_id`, `span_id`, `parent_span_id`.
- Effects: `EffectRecord` (~lines 332–357) with `semantics`,
  `dispatch_mode`, `business_key`, `connector`, `external_ref`,
  `dispatch_claimed_by`.

Extension shape (PR 1): add an optional `RunMetadata` message
(`tenant_id`, `actor`, `subject`, `gateway_route`, `aiplex_instance_id`,
`scopes`, `labels`) and reference it from `BeginRunRequest` and
`RunState`. New field numbers must sit above the current max. The name
stays generic — AIPlex-specific values live in `labels` (e.g.
`aiplex.agent_id`, `aiplex.plane`, `aiplex.route`).

## 2. Server run creation path

- gRPC handler: `tape/server/src/service.rs::begin_run` (~lines 34–43).
- Store trait: `tape/server/src/store/mod.rs::RunStore` with
  implementations in `sql.rs`, `bigtable.rs`, `mem.rs`.
- SQLite migration: `tape/server/migrations/0001_init.sqlite.sql`
  (~lines 9–26). Schema:

  ```sql
  CREATE TABLE tape_runs (
    run_id TEXT PRIMARY KEY,
    app_name TEXT, user_id TEXT, session_id TEXT, invocation_id TEXT,
    status INTEGER, seq_cursor INTEGER, lease_owner TEXT,
    lease_expires_at_ms INTEGER, waiting_on_gate TEXT,
    detail_json TEXT, started_at_ms INTEGER, ended_at_ms INTEGER
  );
  ```

Persistence choice for PR 1: add a single `metadata_json` (JSONB-shaped)
column rather than expanding the column list. Mechanical migration per
backend, no schema explosion when AIPlex adds labels later.

## 3. Python SDK run creation entry point

- `tape/sdk/python/tape/adk/durable.py::durable_app`
  (lines 43–86) returns `(App, Runner)`.
- Alternative entry points:
  - `tape/sdk/python/tape/adk/plugin.py::TapePlugin`
  - `tape/sdk/python/tape/adk/session.py::TapeSessionService`
- BeginRun is fired in `TapePlugin.before_run_callback`
  (`plugin.py` lines 92–107).

PR 1 surface: thread an optional `metadata: RunMetadata` kwarg through
`durable_app`, `TapePlugin.__init__`, `before_run_callback`, and the
`BeginRunRequest` builder. Also add an env-derived helper
(`tape.adk.metadata.from_env()`) that reads `AIPLEX_TENANT_ID`,
`AIPLEX_AGENT_ID`, `AIPLEX_ACTOR`, `AIPLEX_SUBJECT`, `AIPLEX_ROUTE`,
`AIPLEX_SCOPES`. The helper lives in the SDK but is generic — env names
are AIPlex-prefixed by convention, not by code dependency.

## 4. ADK adapter

Same files as §3. The hook surface is:

- `before_run_callback` — natural place to attach `RunMetadata` to
  `BeginRunRequest`.
- `after_model_callback` / `before_tool_callback` / `after_tool_callback` —
  where effect scope checks (PR 2) plug in.
- `after_run_callback` — closes the run.

`TAPE_LEASE_MS` already establishes the env-var pattern (`plugin.py`
~line 37). The AIPlex env vars follow the same model.

## 5. Outbox relay and event emission

- Effect emission: `TapePlugin.before_tool_callback`
  (`plugin.py` lines 193–226) → `begin_effect` RPC →
  `TapeService.begin_effect` (`service.rs` lines 114–130) → write
  `EffectRecord` + journal entry.
- Event envelope is the `JournalEntry` proto (§1).
- Sinks: `tape/sdk/python/tape/sinks.py` exposes `Sink`, `LogSink`,
  `WebhookSink`, `PubSubSink`. Wired via
  `tape.reactors.run_event_fanout(url, sink)`.

PR 3 surface: ensure `RunMetadata` is included in the payload emitted to
sinks (most reactors today only carry the journal payload; AIPlex needs
tenant/agent/actor on every event for routing and authz on the ingest
side). The simplest path is to enrich the outbox event in the relay
loop with the run's metadata, not on every BeginEffect record.

## 6. Effect decorator

- `tape/sdk/python/tape/effect.py::effect` (lines 126–239).
- Metadata stored on the wrapped function under `_tape_effect`
  (lines 173–189); read via `effect_meta_of(fn)`.
- Safety validation: `_validate_semantics()` (lines 96–123) already
  enforces non-idempotent → status_check | compensate | business_key.
  Server re-validates at `BeginEffectRequest`.

PR 2 surface: add an optional `scope: str` parameter. Persist on
`_tape_effect`. In `TapePlugin.before_tool_callback`, look up the
effect's scope and verify membership in `run.metadata.scopes` before
calling `begin_effect`. On failure, append a `policy.violation` journal
entry and raise a typed SDK error — do not invoke the tool body. Server
should re-check at BeginEffect for defence-in-depth.

## 7. Reactor lifecycle

- Entry module: `tape/sdk/python/tape/reactors/__init__.py`.
- Coroutines: `recover_once`, `reconcile_once` (lines 100–195),
  `compensate_once`, `fire_due_timers_once`.
- Aggregator: `run_reactors(runner, url, ...)`; launched via
  `python -m tape.reactors --runner-from my_app:build_runner
  --url tape://...`.
- Env: `TAPE_URL`, `TAPE_REACTOR`, `TAPE_TENANCY`, `TAPE_TENANT_ID`.

No PR 1–3 changes required here, but the outbox sink chosen by AIPlex
(webhook or Pub/Sub) is driven by reactor configuration.

## 8. Deployment

- Terraform: `tape/deploy/gcp/terraform/modules/postgres-cloudsql/`.
- Helm: `tape/deploy/gcp/k8s/chart/tape/` with
  `templates/server.yaml` (port 7878, `TAPE_STORE` env) and
  `templates/reactors.yaml` (per-reactor Deployment loop with
  `python -m tape.reactors --only <name>`).
- Cloud Run: `tape/deploy/gcp/server.service.yaml`,
  `outbox.service.yaml`.
- Image: `tape/server/Dockerfile`.

AIPlex will generate its own manifests pointing at the existing Helm
chart values. No new Tape deployment artefacts needed for PR 1–3.

## 9. Docs structure

- Root: `tape/docs/` with `start/`, `concepts/`, `how-to/`, `deploy/`,
  `reference/`, `design/`, `help/`.
- No `integrations/` subdir today. This file creates it.

## 10. Examples directory

- Root: `tape/examples/`.
- Existing: `treasury/`, `non_idempotent_bank/`, plus
  `standalone/{hello-durable-adk, human-approval-gate,
  reactive-kv-coordination, cloud-run-alloydb, gke-bigtable}`.
- Layout per example:

  ```
  example-name/
  ├── README.md
  ├── app/__init__.py
  ├── app/agent.py
  ├── requirements.txt
  └── run.py
  ```

PR 3 will add `tape/examples/standalone/aiplex-integration/` following
this layout: reads AIPlex env vars, threads `RunMetadata` into
`durable_app`, demonstrates a scoped non-idempotent effect.

## 11. Tests

- Python SDK: `tape/tests/test_*.py` (pytest). Notable files:
  `test_resume.py`, `test_features.py`, `test_non_idempotent.py`,
  `test_obligations.py`, `test_event_bus.py`, `test_chaos_*.py`,
  `test_values.py`, `test_reactors.py`, `test_bigtable.py`.
  Fixtures in `tape/tests/conftest.py`.
- Server (Rust): inline in `tape/server/src/service.rs` from ~line 545
  using `#[test]` / `tokio::test`.
- Cross-SDK parity harness: `tape/tests/parity/`.

PR 1 needs tests for empty vs populated metadata round-trip, plus a
parity update. PR 2 needs allowed / missing-scope / legacy-effect
tests.

## 12. Backward-compatibility signals

Repo-level docs that gate the integration:

- `CHANGELOG.md` — "Unreleased" section is the destination for new
  notes.
- `SDK_PARITY.md` (root) — feature matrix across Python, Go, Java, .NET
  SDKs. Adding `RunMetadata` and effect `scope` requires new rows.
- `CLAUDE.md` — "What *not* to do" section.

Caution by surface:

| Surface           | Risk        | Notes                                       |
| ----------------- | ----------- | ------------------------------------------- |
| Proto fields      | Mechanical  | Use field numbers above current max.        |
| `RunState` fields | Moderate    | Touches all four SDKs + parity matrix.      |
| Storage schema    | Invasive    | Migrations for SQLite/Postgres/Bigtable.    |
| Plugin signature  | Moderate    | New params must be optional and defaulted.  |

## PR breakdown (Tape side)

The integration plan splits into three Tape PRs, each landing on its
own branch off `claude/aiplex-tape-integration-odwFR`:

1. **Generic run metadata** — proto, server, store column, Python SDK,
   ADK adapter, outbox enrichment, tests, `CHANGELOG.md`,
   `SDK_PARITY.md`.
2. **Effect scope enforcement** — optional `scope` on `@tape.effect`,
   pre-tool authorization check, `policy.violation` journal entry,
   typed SDK error, tests.
3. **AIPlex integration example + docs** — runnable example under
   `tape/examples/standalone/aiplex-integration/` and a how-to in
   this directory.

None of these PRs introduces an AIPlex import. AIPlex-specific
identifiers stay in env-var conventions, label keys, and example code.
