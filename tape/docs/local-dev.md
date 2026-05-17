# Local dev

`tape dev` is the one command. Its job: bring up tape-server + reactors + your
agent against a local store.

## SQLite (the default)

```bash
tape dev                     # native mode if cargo's available, else docker
tape dev --store sqlite
```

Stores everything under `./.tape/dev.db`. Restart the agent and the run picks
up where it left off — that's the recovery reactor doing its job.

## Postgres / AlloyDB

```bash
tape dev --store postgres    # uses docker compose with the bundled postgres
```

The project scaffold ships a `docker-compose.yaml` that brings up postgres,
tape-server, and the reactor sidecar. Edit it to taste.

## Pub/Sub emulator

```bash
tape dev --events pubsub-emulator
```

Brings up Google Cloud SDK's `gcloud beta emulators pubsub` on `:8085`. The
`PubSubConnector` honours `PUBSUB_EMULATOR_HOST=localhost:8085` automatically.

## Bigtable emulator

```bash
tape dev --store bigtable-emulator
```

Brings up `cbtemulator` and pre-creates the `tape` table + `m` family + GC
policy (the same shape `tape doctor` verifies on real Bigtable). No more
hand-rolling `cbt createtable`.

## Kill-resume demo

```bash
tape dev --kill-resume-demo
```

Crashes the agent mid-effect, lets the reactor recover it, and asserts that
the upstream saw exactly one wire. Use this every time you change the journal
shape — it's the canary.

## Native mode (no Docker)

If `cargo` is installed (or a `tape-server` binary is on PATH), `tape dev`
runs the server natively against SQLite. Faster startup; same contract.
