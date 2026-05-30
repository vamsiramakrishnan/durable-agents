# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — AIPlex integration (PR 3: docs + example)
- **`tape/examples/standalone/aiplex-integration/`** — runnable worked
  example with a treasury agent that has two scoped effects
  (`read_balance` and the non-idempotent `bank_wire`). `run.py` drives
  the gRPC primitives directly so the admit/deny paths are visible step
  by step (no LLM API key needed). The default config grants only the
  read scope so the demo lands on the **failure path** — the
  `policy.violation` journal entry that AIPlex audit ingestion will
  surface in the run timeline.
- **`tape/docs/integrations/aiplex.md`** rewritten from Phase 0 survey
  into a how-to guide reflecting what's shipped in PR 1 + PR 2. Linked
  from `mkdocs.yml` and `how-to/index.md`.

### Added — AIPlex integration (PR 2: effect scope enforcement)
- **`@tape.effect(scope=...)`** declares the authorization scope an effect
  requires (e.g. `"mcp:tools:bank_wire"`). The Python SDK refuses
  `semantics="non_idempotent"` without a `scope` at **decoration time**;
  `allow_unsafe=True` is the documented override.
- **Runtime pre-check in `TapePlugin.before_tool_callback`**. The plugin
  caches the run's `scopes` grant set at `before_run_callback` time
  (fetched from `RunState` on re-drive so a fresh process picks up the
  same grants). If the effect's declared scope isn't in the run's grants,
  the plugin returns `{error, scope_denied: true}` and the tool body
  never runs. A typed `tape.effect.ScopeDenied` exception is exposed for
  callers that want to handle it explicitly.
- **Server-side defence-in-depth.** `BeginEffectRequest` carries a `scope`
  string; the Rust server verifies scope membership in the run's
  `scopes_json` before any effect row is written, returns
  `tonic::Status::permission_denied` on mismatch, and writes a
  `kind="policy"` journal entry naming the violation (required_scope,
  granted_scopes, tool) so AIPlex audit ingestion sees what was
  attempted. An outdated SDK that doesn't pre-check still can't bypass.
- **`tape_effects.scope`** column on SQLite + Postgres + Bigtable; rewritten
  in place in `0001_init.*.sql` (no chained migration — pre-users).
- **All four SDKs carry the wire field.** Python: typed kwarg + decoration
  enforcement. Java: extended `beginEffect(...)` overload + legacy
  no-scope overload preserved. Go: `BeginEffectOpts.Scope`. TypeScript:
  optional `scope` on the `beginEffect({...})` argument shape.
- **9 new Python tests** in `tape/tests/test_effect_scope.py` covering
  decoration-time refusal, decoration-time scope passthrough, the
  `allow_unsafe` bypass, server-side admit/deny/skip, and the
  policy-violation journal entry shape.

### Added — AIPlex integration (PR 1: run identity)
- **`BeginRunRequest` and `RunState` carry identity & authorization context.**
  Seven new fields: `tenant_id`, `actor` (SPIFFE workload identity), `subject`
  (human principal, distinct from ADK's `user_id`), `agent_id` (stable AIPlex
  catalog id), `aiplex_instance_id`, `gateway_route`, `scopes`, and `labels`.
  Stored as first-class columns on `tape_runs` (scopes/labels JSON-encoded);
  indexed on `(tenant_id, agent_id, started_at_ms)`, `(actor, started_at_ms)`
  and `(subject, started_at_ms)` for the AIPlex run timeline queries.
- **`tape.adk.identity.RunIdentity`** — typed dataclass with
  `RunIdentity.from_env()` reading the conventional `AIPLEX_*` env vars
  (`AIPLEX_TENANT_ID`, `AIPLEX_ACTOR`, `AIPLEX_SUBJECT`, `AIPLEX_AGENT_ID`,
  `AIPLEX_INSTANCE_ID`, `AIPLEX_ROUTE`, `AIPLEX_SCOPES`, `AIPLEX_LABELS`).
  `durable_app` defaults `identity=RunIdentity.from_env()` so AIPlex-deployed
  agents get identity threaded for free; non-AIPlex callers may pass
  `RunIdentity()` explicitly to opt out.
- **Java SDK** ships `dev.tape.RunIdentity` with the same `fromEnv` parser and
  `TapeClient.beginRun(..., RunIdentity)` overload.
- **Go SDK** extends `BeginRunOpts` with the identity fields; tapepb
  regenerated.
- **TypeScript SDK** extends the `beginRun({...})` argument shape with the
  same fields (dynamic call surface — no codegen needed).
- **Schema rewrite, not migration.** `0001_init.{sqlite,postgres}.sql` rewritten
  in place to include the new columns; no chained migration. Tape and AIPlex
  are pre-users, so the schema takes its final shape directly.
- **No server-side required-field validation in PR 1.** AIPlex-deployed
  servers will enforce non-empty `tenant_id` / `actor` / `agent_id` via a
  `TAPE_REQUIRE_IDENTITY` config in PR 5; local dev and existing examples
  keep working with empty identity.

### Added — DevEx
- **One-command bootstrap.** `./setup.sh` installs mise + every toolchain (Rust, Python, Node, Go, Java, just), builds the Rust server, and editable-installs the Python SDK + CLI. `--minimal` and `--skip-build` flags.
- **Curl-pipe installer.** `install.sh` fetches the prebuilt `tape-server` binary from GitHub Releases and pip-installs the CLI. Falls back to a clear "run `./setup.sh` from a clone" message if no release exists yet.
- **Root `Makefile`.** `make help` lists every common task. Targets: `setup`, `build`, `test`, `sdk-test-{python,ts,go,java,all}`, `sdk-parity`, `dev`, `demo`, `demo-resume`, `docker-up`/`down`/`clean`, `docs-serve`, `release-dry`, `doctor`, `status`, `logs`, `clean`.
- **`.mise.toml`** pinning rust 1.83, python 3.12, node 22, go 1.25, java 21, just 1.36.
- **`.env.example`** documenting every `TAPE_*` env var with the store-URL backend matrix.
- **Root `docker-compose.yml`** for a Postgres-backed local stack.
- **`CLAUDE.md`** — onboarding doc for future Claude sessions and new maintainers.
- **`.devcontainer/`** — Codespaces-ready dev environment.
- **Root-level `examples/`** — `quickstart.py`, `quickstart.ts`, `quickstart.go`, `QuickstartJava.java` — the same minimal scenario in every SDK.
- **`make doctor`** — tick/cross diagnostic that reports toolchain, server binary, server reachability, and per-SDK round-trip status.
- **`.editorconfig`** and **`.pre-commit-config.yaml`** — shared formatting + lint baseline.
- **`CONTRIBUTING.md`, `SECURITY.md`, `RELEASING.md`** — standard OSS hygiene.

### Added — SDK parity (G1–G5)
- **Outbox-reactor daemons** in TypeScript (`bin/tape-outbox-ts.ts`), Go (`cmd/tape-outbox`), Java (`dev.tape.cli.TapeOutbox`). Same dispatch loop, same `non_idempotent` safety contract.
- **`WebhookSink` + `PubSubSink`** in TS, Go, and Java (Python already shipped them). Webhook sets `X-Tape-Event-Id`, Pub/Sub sets `orderingKey = run_id`.
- **Cross-SDK parity harness** at `tape/tests/parity/`. One scenario driven through all four SDKs; identical journal projection asserted on every PR. `make sdk-parity`.
- **Java ADK adapter.** `dev.tape.adk.TapePlugin`, `dev.tape.adk.TapeSessionService`, `dev.tape.adk.TapeAdkApp` over `com.google.adk:google-adk:1.2.0` (provided scope).
- **Uniform SDK README reference table** — same 6-row navigation table at the top of `tape-py`, `tape-ts`, `tape-go`, `tape-java`.

### Added — Release / CI
- **`.github/workflows/release.yml`** — cross-builds `tape-server` for `{linux, darwin, windows} × {x86_64, aarch64}` on `v*.*.*` tag. Optional `python` / `npm` / `maven` jobs, each gated on secret existence so they activate the moment credentials are added.
- **`.github/workflows/sdk-tests.yml`** — per-SDK CI matrix + cross-SDK parity job. One cached Rust build feeds all four language jobs.
- **`.github/dependabot.yml`** — keeps every ecosystem's deps fresh.

### Added — Documentation
- **`SDK_PARITY.md`** — live four-language scorecard with G1–G7 roadmap and status.
- **`tape/docs/start/install.md`** — rewritten around the fast-path (`./setup.sh`), the zero-setup path (Codespaces / devcontainer), and the granular per-SDK path. Documents the difference between `make doctor` (repo-scoped) and `tape doctor` (project-scoped).
- **`tape/docs/how-to/outbox-daemon.md`** — the new TS / Go / Java dispatcher CLIs, with a flag-by-flag table.
- **`tape/docs/how-to/sinks.md`** — `LogSink` / `WebhookSink` / `PubSubSink` in every SDK, with the Webhook + Pub/Sub contracts.
- **`tape/docs/how-to/cross-sdk-parity.md`** — what the harness asserts, how to run it locally, how to add a new scenario.
- **`tape/docs/adk.md`** — added a `## Java` section covering `TapePlugin`, `TapeSessionService`, `TapeAdkApp.wire(...)`, the callback → RPC mapping, and the G4 status caveat (model replay + budget are follow-ups).
- **`mkdocs.yml`** and **`tape/docs/how-to/index.md`** — wired the three new how-to pages into the nav.
- **`CLAUDE.md`** — refreshed the "Common tasks" lookup table to cover `make doctor`, `make quickstart-all`, `make status`, `make logs`, and the Codespaces entrypoint.

[Unreleased]: https://github.com/vamsiramakrishnan/durable-agents/compare/main...HEAD
