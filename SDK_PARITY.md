# SDK Parity — Python · TypeScript · Go · Java

Tape ships four SDKs against one wire protocol (`tape/proto/tape.proto`). The
**protocol is the contract**: any feature reachable by RPC is reachable from
every language. This document is the live parity scorecard and the roadmap to
closing the remaining gaps.

> If you're looking at the per-language wiring table, the canonical one lives in
> [`tape/README.md`](tape/README.md#same-dx-in-go-typescript-and-java). This
> file extends it with **status, gaps, owners, and next steps**.

---

## TL;DR

| | Python | TypeScript | Go | Java |
|---|:--:|:--:|:--:|:--:|
| gRPC client (every RPC) | ✅ | ✅ | ✅ | ✅ |
| `BeginEffect` / `CompleteEffect` short-circuit on confirmed | ✅ | ✅ | ✅ | ✅ |
| `@effect` / retry policy sugar | ✅ | ✅ | ✅ | ✅ |
| `durableApp(...)` one-call wiring | ✅ | ✅ | ✅ | ✅ |
| `outboxTool(...)` (non-idempotent dispatch) | ✅ | ✅ | ✅ | ✅ |
| `non_idempotent` safety enforcement at construction | ✅ | ✅ | ✅ | ✅ |
| Connectors: `log`, `http` | ✅ | ✅ | ✅ | ✅ |
| Connectors: `pubsub`, `cloudtasks` | ✅ | ✅ (lazy) | ✅ (build tags) | ✅ (reflective) |
| Reactions / event-bus subscribe | ✅ | ✅ | ✅ | ✅ |
| Tenancy config + DESIGN-ONLY warnings | ✅ | ✅ | ✅ | ✅ |
| Observability: structured logs + OTel span hook | ✅ | ✅ | ✅ | ✅ |
| ADK adapter (`TapePlugin`, `TapeSessionService`) | ✅ | — *(no ADK)* | — *(no ADK)* | ⚠️ planned, see roadmap |
| Built-in outbox-reactor runner | ✅ | ⚠️ partial | ⚠️ partial | ⚠️ partial |
| Built-in sinks (`Log`, `Webhook`, `PubSub`) | ✅ | ⚠️ Log only | ⚠️ Log only | ⚠️ Log only |
| CLI (`tape init/dev/doctor/provision/deploy`) | ✅ | — *(call from Python CLI)* | — | — |
| Cross-SDK parity test harness | ⚠️ Python-only | ⚠️ | ⚠️ | ⚠️ |
| End-user docs reference (auto-gen) | ✅ mkdocstrings | ✅ typedoc | ✅ gomarkdoc | ✅ javadoc |

✅ shipping · ⚠️ partial · — n/a by design

---

## How to think about parity

The Python SDK is the **reference**. It pioneers the surface area, then the
other three follow. Parity does **not** mean every Python module exists in
TS/Go/Java verbatim — some surfaces (the ADK plugin, the Typer CLI, mkdocstrings
tooling) are framework-bound to Python. Parity means:

1. **Protocol parity:** every gRPC RPC is reachable. (✅ done.)
2. **Idiom parity:** the same primitive uses the language's natural shape
   — Python decorators, Go option structs, TS option objects, Java builders.
3. **Safety parity:** every safety invariant the Python SDK enforces locally
   (e.g. `non_idempotent` without a `business_key` ⇒ refuse to build the tool)
   is enforced in every other SDK *at construction time*, not just on the
   server.
4. **Operational parity:** the reactor runners, sinks, and connectors needed
   to actually *operate* a durable agent ship out of the box.

The wire protocol enforces the safety contract at `BeginEffect` time so even
an old SDK can't slip through — but a polite SDK refuses *before* the round
trip.

---

## Open gaps (the roadmap)

### G1. Outbox-reactor runners in TS / Go / Java
**Status:** Python ships `tape.reactors.outbox._main` (the
`tape-outbox` console-script) — a long-running daemon that subscribes to
`PENDING|OUTBOX` effects and dispatches via registered connectors.
TS/Go/Java each have **all the pieces** (connector registries, the
`SubscribeEvents` stream, `CompleteEffect`) but no packaged daemon entrypoint.

**Deliverable:**
- `tape/sdk/typescript/bin/tape-outbox-ts` (Node CLI; `npm run outbox`)
- `tape/sdk/go/cmd/tape-outbox/main.go` (`go run` or compiled binary)
- `tape/sdk/java/src/main/java/dev/tape/cli/TapeOutbox.java` (Maven exec / fat-jar)

Each takes `--url`, `--load <module>` (or `--connector <class>`), and runs the
same dispatch loop with the same `non_idempotent` safety check.

**Acceptance:** the kill-and-resume integration test passes when the Python
outbox is replaced by each language's daemon.

---

### G2. Sink parity (`Webhook`, `PubSub`) in TS / Go / Java
**Status:** Python ships `LogSink`, `WebhookSink`, `PubSubSink`. TS/Go/Java
have `LogSink` only.

**Deliverable:** `WebhookSink` (POST + retries + `X-Tape-Event-Id`) and
`PubSubSink` (ordering key = `run_id`) in each of the three.

**Acceptance:** the outbox-relay smoke test from
`tape/tests/test_features.py` runs against each SDK with `--sink webhook` and
`--sink pubsub`.

---

### G3. Cross-SDK parity test harness
**Status:** kill-and-resume integration tests live in `tape/tests/` and target
Python. TS/Go/Java each have their own smoke tests but they don't share a
common scenario harness.

**Deliverable:** a `tape/tests/parity/` suite that drives **the same scenario**
against each SDK via the language's `outbox` CLI from G1. One scenario file
(YAML) per behaviour; the harness loops over `{python, typescript, go, java}`
and asserts identical journal projections.

**Acceptance:** GitHub Actions `sdk-parity` job is green on every PR.

---

### G4. Java ADK adapter
**Status:** Google has published a Java ADK
([google.github.io/adk-docs/java](https://google.github.io/adk-docs/)) since
the Python adapter shipped. Tape's TS/Go ADK adapters are out of scope (no
non-Python ADK at the time of writing for those languages), but **Java is
now reachable**.

**Deliverable:** `dev.tape.adk.TapePlugin` + `dev.tape.adk.TapeSessionService`
mirroring the Python surface; `DurableApp.wire(...)` already exists as the
non-ADK wiring entrypoint.

**Acceptance:** the treasury example runs end-to-end with a Java root agent
against the same Tape server.

---

### G5. SDK README structural consistency
**Status:** the four SDK READMEs have drifted in shape. Python has 51 lines,
Go has 237, TS has 181, Java has 176.

**Deliverable:** a shared README skeleton with the same six sections in the
same order:

1. **Install**
2. **30-second example**
3. **Wire it into an agent**
4. **Reference** (link to the auto-generated language docs)
5. **What's wired** (the per-feature checklist for that SDK)
6. **Contribute** (link to this file)

**Acceptance:** all four READMEs lint-pass a markdown structure check that
ships with `make docs`.

---

### G6. One-command per-SDK test target
**Status:** to run all SDK tests you have to know per-language invocations.

**Deliverable:** root `Makefile` targets:

```
make sdk-test-python   # pytest against the Rust server (in-memory)
make sdk-test-ts       # node --test
make sdk-test-go       # go test
make sdk-test-java     # mvn test
make sdk-test-all      # all four, parallel where independent
```

**Acceptance:** CI runs `make sdk-test-all` and the dev loop on a fresh
clone is `./setup.sh && make sdk-test-all`.

✅ **Shipped in this PR.** See `Makefile`.

---

### G7. CLI parity (deferred — see decision log)
The Typer CLI (`tape init/dev/doctor/provision/deploy`) stays Python-only by
design: it provisions cloud infrastructure and scaffolds projects, both
language-agnostic artifacts. A Go/TS/Java reimplementation would be the wrong
investment. The *agent process* can be in any of the four languages; the
*infra scaffold* is one tool, like `terraform` or `helm`.

---

## Done in this PR (the devex layer)

| Surface | Was | Now |
|---|---|---|
| One-command setup | `pip install -e tape/sdk/python -e tape/cli` (after manual Rust + just install) | `./setup.sh` |
| Binary installer | none | `curl -fsSL .../install.sh \| sh` |
| Root Makefile | none | `make help` lists every target |
| `.mise.toml` | none | rust + python + node + go + java + just pinned |
| `.env.example` | none | every `TAPE_*` env var documented |
| Root `docker-compose.yml` | only under `tape/` | root → top-level `docker compose up` works |
| `CLAUDE.md` | none | onboarding doc for future Claude sessions |
| SDK parity matrix | scattered | this file |
| Per-SDK test target | per-language invocation | `make sdk-test-{python,ts,go,java,all}` |
| Release workflow | docs only | `.github/workflows/release.yml` for cross-platform `tape-server` binaries |

---

## How to contribute

Pick a gap (G1–G6). Each gap has a deliverable, an acceptance criterion, and
a target SDK. Open a PR with the language's normal layout under
`tape/sdk/<lang>/`. The parity matrix above is the source of truth — if you
add a primitive, update both the matrix and `tape/README.md`'s wiring table.
