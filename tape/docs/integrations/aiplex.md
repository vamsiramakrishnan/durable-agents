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

Tape stays independently usable but is **not** treating any current
caller as a fixed constraint. Both projects are pre-users, so this
integration redesigns the shapes that need redesigning — protos, store
schema, SDK signatures — without migration shims or optional fallbacks.
AIPlex-specific values still live in label keys (`aiplex.agent_id`,
`aiplex.plane`, etc.) so Tape itself takes no build-time dependency on
AIPlex.

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

Reshape for PR 1 (greenfield rework): promote identity, authorization
and routing context to first-class fields on `BeginRunRequest` and
`RunState` directly — no sub-message, no optional metadata bag. Target
shape:

```proto
message BeginRunRequest {
  // identity / addressing
  string tenant_id = 1;
  string actor = 2;               // SPIFFE-style workload identity
  string subject = 3;             // human principal (email, sub, ...)
  string aiplex_instance_id = 4;
  string gateway_route = 5;

  // agent + run identity (renamed: user_id → subject above)
  string app_name = 10;
  string agent_id = 11;
  string session_id = 12;
  string invocation_id = 13;

  // authorization context
  repeated string scopes = 20;
  map<string, string> labels = 21;

  // lease
  string lease_owner = 30;
  uint64 lease_ttl_ms = 31;
}
```

`RunState` mirrors these as columns. Field numbers are reassigned
freely — no requirement to sit above the current max. The ADK adapter
maps ADK's `user_id` callback into `subject`. `actor` is distinct from
`lease_owner`: `actor` is the principal who initiated the run, while
`lease_owner` is the worker process that currently holds at-most-one
execution rights.

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

Persistence choice for PR 1: rewrite the initial migration in place
(no chained `0002_*.sql`). The new `tape_runs` schema gains explicit
columns for `tenant_id`, `actor`, `subject`, `aiplex_instance_id`,
`gateway_route`, `agent_id`, `scopes` (text array or join table per
backend), and a `labels_json` blob for the open-ended `map<string,
string>`. Indexes on `(tenant_id, agent_id)` and `(actor, started_at_ms)`
so the AIPlex run timeline queries are cheap. Mirror in `sql.rs`,
`bigtable.rs`, `mem.rs`.

## 3. Python SDK run creation entry point

- `tape/sdk/python/tape/adk/durable.py::durable_app`
  (lines 43–86) returns `(App, Runner)`.
- Alternative entry points:
  - `tape/sdk/python/tape/adk/plugin.py::TapePlugin`
  - `tape/sdk/python/tape/adk/session.py::TapeSessionService`
- BeginRun is fired in `TapePlugin.before_run_callback`
  (`plugin.py` lines 92–107).

PR 1 surface: extend `durable_app`, `TapePlugin.__init__` and the
`BeginRunRequest` builder with the new first-class arguments
(`tenant_id`, `actor`, `subject`, `agent_id`, `scopes`, `labels`,
`aiplex_instance_id`, `gateway_route`). `tenant_id`, `actor` and
`agent_id` are **required**; the SDK raises at startup if they are
missing rather than letting a malformed run reach the server.

Add `tape.adk.identity.from_env()` returning a typed
`RunIdentity` dataclass populated from `AIPLEX_TENANT_ID`,
`AIPLEX_AGENT_ID`, `AIPLEX_ACTOR`, `AIPLEX_SUBJECT`, `AIPLEX_ROUTE`,
`AIPLEX_INSTANCE_ID`, `AIPLEX_SCOPES`. The helper lives in the SDK but
is generic — env-var prefixing is convention only, not a code
dependency on AIPlex.

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

PR 3 surface: enrich the outbox event envelope so every sink sees
`tenant_id`, `actor`, `subject`, `agent_id` and `aiplex_instance_id`
without having to round-trip back to `RunState`. The relay loop joins
the run's identity columns into each event before fan-out — keeps the
journal entry itself lean while the egress envelope is self-contained.

## 6. Effect decorator

- `tape/sdk/python/tape/effect.py::effect` (lines 126–239).
- Metadata stored on the wrapped function under `_tape_effect`
  (lines 173–189); read via `effect_meta_of(fn)`.
- Safety validation: `_validate_semantics()` (lines 96–123) already
  enforces non-idempotent → status_check | compensate | business_key.
  Server re-validates at `BeginEffectRequest`.

PR 2 surface: add a `scope: str` parameter that is **required for
`semantics="non_idempotent"`**. `_validate_semantics()` grows a fourth
clause: non-idempotent without scope raises at decoration time, not at
call time. Persist on `_tape_effect`. In
`TapePlugin.before_tool_callback`, verify membership in `run.scopes`
(now a top-level run field per §1) before calling `begin_effect`. On
failure, append a `policy.violation` journal entry and raise a typed
SDK error — the tool body never executes. Server re-checks at
`BeginEffectRequest` for defence-in-depth; defence-in-depth is not
optional here since AIPlex's gateway authz and Tape's effect authz are
expected to drift unless one re-validates the other.

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

PR 1 needs tests asserting the new required fields reject empty input
at SDK construction and at the server, plus a parity update. PR 2 needs
allowed-scope, missing-scope, and "non-idempotent without scope fails
at decoration" tests.

## 12. Cross-cutting docs to update

No backward-compatibility constraints — Tape and AIPlex have no users
yet, so this integration takes the right shape directly rather than
chaining migrations on top of a placeholder design. Repo-level docs
that still need touching:

- `CHANGELOG.md` — "Unreleased" entry per PR. No "BREAKING" warnings
  needed; everything pre-1.0 is implicitly under the same banner.
- `SDK_PARITY.md` — Python is the integration target; Go / Java / .NET
  SDKs gain new rows tracking what's still to port, but are not
  blockers for AIPlex onboarding.
- `CLAUDE.md` — refresh the "what *not* to do" section if PR 2's
  scoped-effects model affects existing guidance.

Reshape per surface:

| Surface           | PR 1 action                                                            |
| ----------------- | ---------------------------------------------------------------------- |
| Proto fields      | Reassign field numbers freely; inline identity onto `BeginRunRequest`. |
| `RunState` fields | Promote identity to first-class columns; rewrite the type.             |
| Storage schema    | Rewrite `0001_init.*.sql` in place; no chained migrations.             |
| Plugin signature  | New required params; SDK raises if caller omits them.                  |

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
