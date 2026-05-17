# Install

Tape has three installable pieces. You probably want all of them.

## Python SDK + CLI

```bash
pip install -e tape/sdk/python    # tape-py — SDK + ADK adapter
pip install -e tape/cli           # tape   — the standalone CLI
```

The SDK is `import tape`. The CLI is `tape <command>`. Verify:

```bash
tape --version
python -c "import tape; print(tape.__version__)"
```

!!! tip "Why `-e` editable installs?"
    The Python SDK lives in this repo. Editable installs let you `git pull`
    upgrades without re-running `pip install`. When the package is published
    to PyPI we'll publish `pip install tape-py`.

## Other-language SDKs

=== ":simple-go: Go"

    ```bash
    go get github.com/vamsiramakrishnan/durable-agents/tape/sdk/go
    ```

    See [Go SDK reference](../reference/go/index.md) for build tags
    (Pub/Sub and Cloud Tasks are gated behind `pubsub` and `cloudtasks`).

=== ":material-language-typescript: TypeScript"

    ```bash
    npm install tape-ts
    ```

    See [TypeScript SDK reference](../reference/typescript/index.md).

=== ":fontawesome-brands-java: Java"

    ```xml
    <dependency>
      <groupId>dev.tape</groupId>
      <artifactId>tape</artifactId>
      <version>0.1.0</version>
    </dependency>
    ```

    See [Java SDK reference](../reference/java/index.md).

## The server

For local development, `tape dev` runs `tape-server` for you. Two modes:

- **Native** — if `cargo` is installed (or a `tape-server` binary is on
  `PATH`). Faster startup.
- **Docker** — fallback. Pulls the bundled image and starts it via Docker
  Compose.

You don't need to install anything Rust-related yourself unless you're
hacking on the server.

## Verify

```bash
tape doctor               # tick/cross diagnostic of your local setup
```

If everything's green, you're ready for the [**quickstart**](../quickstart.md).

## Updating

```bash
git pull
pip install -e tape/sdk/python -e tape/cli   # picks up any new deps
```

When new schema fields land in `tape.proto`, the server migrates on startup
— but the SDK and the server should be on matching minor versions. `tape
doctor` warns when they drift.

## Where things live

```
durable-agents/
├─ tape/sdk/python/      # the Python SDK (tape-py)
├─ tape/sdk/{go,ts,java} # the other-language SDKs
├─ tape/cli/             # the Typer CLI (tape)
├─ tape/server/          # the Rust server (tape-server)
├─ tape/proto/           # the wire protocol (tape.v1.*)
├─ tape/deploy/          # Terraform + Helm + manifests
└─ tape/examples/        # end-to-end examples
```
