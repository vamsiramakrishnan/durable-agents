# Install

Tape has a fast path and a granular path. Use the fast path unless you only
want one specific SDK.

## Fast path — clone + one command

```bash
git clone https://github.com/vamsiramakrishnan/durable-agents && cd durable-agents
./setup.sh                # mise + rust/python/node/go/java/just + build server + install SDKs
make doctor               # tick/cross diagnostic — verifies everything's healthy
make quickstart-all       # the same 20-line scenario in all four SDKs
```

`./setup.sh` installs [`mise`](https://mise.jdx.dev) (a tool-version manager)
and then everything pinned in
[`.mise.toml`](https://github.com/vamsiramakrishnan/durable-agents/blob/main/.mise.toml).
After it finishes, `make doctor` runs a tick/cross check across the toolchain,
the server binary, the SDK install, and a live `BeginRun/EndRun` round-trip
against the server.

Pass `--minimal` for the "just the core" path (Rust + Python + just):

```bash
./setup.sh --minimal
```

## Zero-setup path — Codespaces / devcontainer

[**Open in GitHub Codespaces**](https://codespaces.new/vamsiramakrishnan/durable-agents) — the `.devcontainer/devcontainer.json` in
the repo provisions a ready-to-go environment with every toolchain
pre-installed. `make doctor` runs on first attach. Then `make
quickstart-all` or `make demo` works immediately.

The same config works locally in VS Code's *Dev Containers* extension or
any Codespaces-compatible IDE.

## Binary install — `curl … | sh`

Once a release is tagged, the prebuilt `tape-server` binary plus the Python
CLI install via:

```bash
curl -fsSL https://raw.githubusercontent.com/vamsiramakrishnan/durable-agents/main/install.sh | sh
```

This drops `tape-server` into `~/.local/bin/` and pip-installs `tape-py` +
`tape-cli`. Pin a version with `sh -s -- --version v0.1.0`; skip the server
with `--no-server`; skip the CLI with `--no-cli`.

Until the first release is cut, the script prints a clear "build from
source" message and exits gracefully — it doesn't fail silently.

## Granular path — one SDK at a time

If you already have a Tape server running somewhere and only want to wire
one language's agent against it:

=== ":material-language-python: Python"

    ```bash
    pip install tape-py             # the SDK (or from a clone: pip install -e tape/sdk/python)
    pip install tape-cli            # the Typer CLI (or: pip install -e tape/cli)
    ```

    The SDK is `import tape`. The CLI is `tape <command>`. Verify with
    `tape --version` and `python -c "import tape; print(tape.__version__)"`.

    !!! tip "Why `-e` editable installs?"
        From a clone, editable installs let you `git pull` upgrades without
        re-running `pip install`. PyPI installs lock to a published version.

=== ":material-language-typescript: TypeScript"

    ```bash
    npm install tape-ts
    ```

    The SDK ships ESM with TypeScript type definitions (`src/index.ts`).
    Includes the [`tape-outbox-ts`](../how-to/outbox-daemon.md) binary
    for the outbox dispatcher.

=== ":simple-go: Go"

    ```bash
    go get github.com/vamsiramakrishnan/durable-agents/tape/sdk/go
    ```

    Pub/Sub and Cloud Tasks are gated behind build tags (`pubsub` and
    `cloudtasks`) so the default build pulls no GCP deps. The
    [`cmd/tape-outbox`](../how-to/outbox-daemon.md) package is the daemon
    CLI.

=== ":fontawesome-brands-java: Java"

    ```xml
    <dependency>
      <groupId>dev.tape</groupId>
      <artifactId>tape</artifactId>
      <version>0.1.0</version>
    </dependency>
    ```

    `dev.tape.adk.TapePlugin` + `dev.tape.adk.TapeSessionService` wire Tape
    into a [Google ADK (Java)](https://google.github.io/adk-docs/)
    `Runner` — see [ADK on Tape](../adk.md#java). The ADK dependency is
    `provided`-scope so non-ADK callers aren't forced to pull it in.

## The server

Local development uses one of three modes:

- **`make serve`** — runs the locally-built `tape-server` binary in the
  foreground. Default: SQLite file at `./tape.db`.
- **`make docker-up`** — `docker compose up` with Postgres + the
  `tape-server` image. Production-shaped local stack.
- **`tape dev`** — the Python CLI runs `tape-server` for you (native if
  `cargo` or a `tape-server` binary is on `PATH`; otherwise Docker).

For tests and demos, the SDKs spawn `tape-server --store memory` themselves.

## Verify

Two distinct `doctor` commands:

- **`make doctor`** — repo-scoped. Toolchain + server binary + server
  reachability + Python SDK round-trip. Run after `./setup.sh`.
- **`tape doctor`** — project-scoped (after `tape init <project>`).
  Checks the project's local environment plus optional GCP access
  (gcloud, ADC, Cloud Run / Pub/Sub bindings).

If `make doctor` is green, you're ready for the
[**quickstart**](../quickstart.md).

## Updating

```bash
git pull
./setup.sh --skip-build           # picks up any new tool versions
make build                        # rebuild the server + every SDK
```

Or, after `pip install tape-py` / `npm install tape-ts` / etc., upgrade the
language package the usual way (`pip install -U tape-py`, `npm update
tape-ts`, `go get -u`, bump the Maven version).

When new schema fields land in `tape.proto`, the server migrates on startup
— but the SDK and the server should be on matching minor versions. `tape
doctor` warns when they drift.

## Where things live

```
durable-agents/
├─ tape/sdk/python/      the Python SDK (tape-py)
├─ tape/sdk/{ts,go,java} the other-language SDKs
├─ tape/cli/             the Typer CLI (tape)
├─ tape/server/          the Rust server (tape-server)
├─ tape/proto/           the wire protocol (tape.v1.*)
├─ tape/deploy/          Terraform + Helm + manifests
├─ tape/examples/        end-to-end examples
└─ examples/             20-line quickstarts (one per language)
```
