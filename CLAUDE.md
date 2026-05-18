# CLAUDE.md — durable-agents (Tape)

This file orients a future Claude session in this repository. Read it once and
you should know the shape of the system, what's where, what conventions to
follow, and what *not* to do.

---

## What this is

**Tape** is a *durable-execution substrate* for agents that act. It journals
every model decision and every external effect so a crashed agent **resumes
from the journal** instead of re-acting. The runtime is a separate system — a
Rust server with a language-agnostic gRPC contract — and SDKs in four
languages plug into [Google ADK](https://google.github.io/adk-docs/) **with no
changes to ADK**.

The mental model — say it out loud, this is the entire system:

> One append-only execution journal, with semantic projections (decisions,
> effects, obligations, timers, gates, budgets, reactive KV). The WAL tells
> you what *happened*; the projections tell you what *is true now*.

If a sentence in this repo doesn't reconcile to that model, it's wrong.

The repo has **two halves**:

| Half | What | Where |
|---|---|---|
| **The argument** | The treatise on why an agent runtime must look like a runtime — `agents-that-act-treatise.md` + the `tape.md` design spec + 37 SVG figures | `design-principles/` |
| **The substrate** | Rust server + four SDKs + Python CLI + examples + deploy assets | `tape/` |

---

## Repository layout

```
durable-agents/
├── README.md                    Repo intro. Pointer to design-principles/ and tape/.
├── CLAUDE.md                    You are here.
├── SDK_PARITY.md                The parity matrix + roadmap for Python/TS/Go/Java.
├── Makefile                     `make help` is the entry point for every common task.
├── setup.sh                     One-command bootstrap (mise + tools + build + SDKs + CLI).
├── install.sh                   Curl-pipe binary installer (post-release).
├── docker-compose.yml           Postgres + tape-server (local stack).
├── .mise.toml                   Pinned tool versions (rust, python, node, go, java, just).
├── .env.example                 Every TAPE_* env var documented.
│
├── design-principles/           THE ARGUMENT
│   ├── agents-that-act-treatise.md      The foundational essay (172KB).
│   ├── agents-that-act-rhythmic.md      A shorter rhythmic version.
│   ├── tape.md                           The design spec.
│   ├── tape-event-bus.md                 The reaction-system spec.
│   ├── parity.md                         Tape vs Temporal/LangGraph/DBOS.
│   └── 01_*.svg .. 37_*.svg              Visual insertions for the treatise.
│
├── docs/                        MkDocs build deps only (requirements.txt).
│                                The actual docs source lives at tape/docs/.
│
├── scripts/docs/                Per-language reference generators (run by docs CI).
│
└── tape/                        THE SUBSTRATE
    ├── README.md                Quickstart, architecture, the wiring table.
    ├── proto/tape.proto         THE CONTRACT — the wire protocol everything shares.
    ├── server/                  The Rust gRPC server (Tokio, Tonic).
    │   ├── Cargo.toml
    │   ├── Dockerfile
    │   ├── migrations/          SQL migrations per backend.
    │   └── src/store/           RunStore trait + SQLite/Postgres/Bigtable impls.
    ├── sdk/
    │   ├── python/              tape-py — the reference SDK + ADK adapter (TapePlugin,
    │   │                        TapeSessionService, durable_app, reactors, sinks).
    │   ├── typescript/          tape-ts — wired client (TapeClient + effect/durableApp/outboxTool).
    │   ├── go/                  Go SDK — wired client (TapeClient + DurableApp + OutboxTool).
    │   └── java/                Java SDK — wired client (TapeClient + DurableApp + OutboxTool).
    ├── cli/                     tape-cli — Typer CLI (init / dev / doctor / provision / deploy).
    │                            Python-only by design — see SDK_PARITY.md G7.
    ├── examples/
    │   ├── treasury/            The treatise's treasury agent. The canonical demo.
    │   ├── non_idempotent_bank/ Outbox + reconciliation against a flaky upstream.
    │   └── standalone/          Self-contained ADK scaffolds (hello-durable-adk, ...).
    ├── tests/                   Integration tests (kill-and-resume, features, reactors, Bigtable).
    ├── docs/                    MkDocs source (start/, concepts/, how-to/, deploy/, reference/).
    ├── deploy/                  Terraform (GCP) + Helm chart (GKE Autopilot).
    ├── docker-compose.yml       (Inner version — `make docker-up` uses the root one.)
    └── justfile                 Inner-loop recipes (build, test, demo, demo-resume, serve).
```

---

## Conventions

### The protocol is the contract.

Every SDK speaks `tape/proto/tape.proto`. If you add a primitive, you add it
to the proto first, then the server, then the four SDKs. Never add a feature
to one SDK that requires a server change without updating the proto.

### Python is the reference SDK.

When a primitive lands, Python ships first (the reference shape, the
docstrings the docs site renders, the integration tests). TS/Go/Java follow
in idiom-appropriate form. The parity matrix in `SDK_PARITY.md` tracks
what's caught up.

### Safety invariants are enforced at construction *and* on the wire.

The `non_idempotent` safety rule (no `business_key`, no `status_check`, no
`compensate` ⇒ refuse to build the tool) is enforced by every SDK at
decoration / construction time **and** by the server at `BeginEffect` time.
If you weaken one without the other, you've introduced a footgun.

### `make help` is the entry point.

Every common task has a Makefile target. The Rust inner loop and the
"insider" recipes live in `tape/justfile`; `make demo` / `make demo-resume`
shell out to it so the root Makefile stays the single front door.

### Docs are journey-shaped, not API-shaped.

The auto-generated reference pages (mkdocstrings / typedoc / gomarkdoc /
javadoc) hang off `tape/docs/reference/`. The hand-written pages are
journey-shaped: *quickstart* → *concepts* → *how-to* → *deploy*. When you
add a feature, add a how-to page that shows it in context — not just a
reference entry.

### Examples are first-class tests.

`tape/examples/treasury/` and `tape/examples/non_idempotent_bank/` are
referenced by `tape/tests/test_resume.py` and `test_features.py` — they
double as the kill-and-resume smoke test. If you change an example, run
`make test`.

### Branches and commits.

Develop on the branch the harness specifies (`claude/<slug>`). One logical
change per commit. Commit messages describe the *why*; the diff describes
the *what*.

---

## Common tasks (the lookup table)

| You want to... | Run | Or read |
|---|---|---|
| First-time setup on a clean clone | `./setup.sh` | This file |
| Run the treasury demo | `make demo` | `tape/justfile` |
| Prove kill-and-resume works | `make demo-resume` | `tape/examples/treasury/` |
| Run every SDK's smoke test | `make sdk-test-all` | `SDK_PARITY.md` |
| Run the Rust + Python integration tests | `make test` | `tape/tests/` |
| Stand up a Postgres-backed Tape server | `make docker-up` | `docker-compose.yml` |
| Build the docs site locally | `make docs-serve` | `tape/docs/` |
| Regenerate Python gRPC stubs | `cd tape && just codegen-py` | `tape/sdk/python/regen_protos.sh` |
| Add a new RPC | edit `tape/proto/tape.proto` → server → all 4 SDKs | `tape/server/src/grpc.rs` |
| Add a new connector | follow `tape/sdk/python/tape/connectors/http.py` | `tape/docs/how-to/non-idempotent-upstreams.md` |
| Add a new sink | follow `tape/sdk/python/tape/sinks.py` | `SDK_PARITY.md` G2 |
| Add a new docs page | drop it under `tape/docs/`, link from `mkdocs.yml` | `mkdocs.yml` |

---

## Versioning & releases

Releases are tagged `vX.Y.Z` and built by `.github/workflows/release.yml`:

- **`tape-server`** — cross-built for `{linux, darwin, windows} × {x86_64, aarch64}` via `cargo` + `cross` and uploaded to GitHub Releases as `tape-server_<version>_<target>.tar.gz`. `install.sh` fetches these.
- **`tape-py` + `tape-cli`** — published to PyPI from `tape/sdk/python/pyproject.toml` and `tape/cli/pyproject.toml`.
- **`tape-ts`** — published to npm from `tape/sdk/typescript/package.json`.
- **`tape-go`** — the Go module is consumed by its module path; tag is what callers pin.
- **`tape-java`** — published to Maven Central from `tape/sdk/java/pom.xml`.

The version in each manifest is the truth; CI checks they all match the tag.

---

## What *not* to do

1. **Don't add ceremony "on top of" ADK.** Tape rides ADK extension points
   (plugins, `SessionService`, `LongRunningFunctionTool`, `invocation_id`).
   If a feature requires patching ADK, find another door.

2. **Don't store mutable state in the server process.** The server is
   *stateless between requests*. State lives in the journal (the WAL +
   projections in the store). This is what lets `docker compose up --scale
   tape-server=3` work.

3. **Don't break the safety contract.** The `non_idempotent` rule is
   load-bearing. An UNKNOWN dispatch that gets blindly retried is the bug
   the whole project is designed to prevent.

4. **Don't ship a feature to one SDK without filing a parity issue.** Drop
   a row into `SDK_PARITY.md`'s "Open gaps" table the same PR.

5. **Don't reach for Rust as the user-facing language.** Rust runs the
   server; users write agents in Python, TypeScript, Go, or Java. Adding a
   Rust-facing primitive that users have to learn is a smell.

6. **Don't commit `tape/docs/design/`, `tape/docs/reference/typescript/{classes,...}`, or `tape/docs/reference/java/javadoc/`.** They're generator output (see `.gitignore`).

7. **Don't run destructive git ops.** No `--force` pushes to `main`, no
   `reset --hard` without asking. See the repo-wide policy in the harness.

---

## When you're stuck

The treatise (`design-principles/agents-that-act-treatise.md`) is long but
authoritative. Section IX (the "primitives" section) explains every concept
in the system in the order they appear in the journal. If something in the
codebase doesn't reconcile to a primitive in §IX, it's drift.

For a faster orientation: read `tape/README.md` end-to-end (it's ~350
lines), then skim `design-principles/tape.md` §12 (state and concurrency)
and `design-principles/parity.md` (what's different from Temporal /
LangGraph / DBOS).
