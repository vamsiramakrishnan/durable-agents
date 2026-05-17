# Start here

You've landed on Tape. Pick the path that matches what you want to do today.

<div class="tape-hub" markdown>

- [**Install**](install.md)
  Pick up the SDK and CLI. Five minutes. No GCP needed.

- [**Quickstart (10 min)**](../quickstart.md)
  Zero to a recovering, observable agent on your laptop, then on Cloud Run.

- [**ADK on Tape**](../adk.md)
  The `durable_app(...)` recipe — 15 lines that make any ADK agent durable.

- [**Local development**](../local-dev.md)
  `tape dev` against SQLite, the Pub/Sub emulator, the Bigtable emulator, and
  the kill-resume demo.

- [**First crash-survival**](first-crash-survival.md)
  Kill the agent mid-tool-call. Watch the reactor resume it. Verify exactly-once.

</div>

## Prerequisites

You'll need:

- Python 3.10+ (the reference SDK + the CLI)
- A POSIX shell (`bash` or `zsh`)
- (optional) Docker, if you don't have `cargo` and prefer the bundled
  `tape-server` image
- (optional) Google Cloud SDK + a project, if you're going to deploy

Tape is one Rust server + per-language SDKs. The reference SDK and the CLI are
Python. Your **agent process** can be Python, Go, TypeScript, or Java — see
[SDK reference](../reference/index.md).

## What's next

- Don't yet know what "outbox" or "reactor" means? Start with the
  [**Concepts**](../concepts/index.md) section — it's the mental model.
- Want a single page that shows every API in one shot?
  [**Cheat sheet**](../reference/cheatsheet.md).
- Stuck on something? [**FAQ**](../help/faq.md) ·
  [**Troubleshooting**](../help/troubleshooting.md).
