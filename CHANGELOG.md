# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/vamsiramakrishnan/durable-agents/compare/main...HEAD
