# CLI Reference

!!! info "Generated"
    Generated from the live Typer app by `scripts/docs/gen_cli.py`. To change the content,
    edit the Typer commands themselves and re-run the script.

`tape` is the standalone CLI. It composes the substrate (Tape) and the cloud (GCP) without
making developers learn every seam first.

```bash
pip install -e tape/cli
tape --help
```

## `tape`

Tape — a durable-execution substrate for ADK agents on GCP.

```
Usage: tape [OPTIONS] COMMAND [ARGS]...
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--version` | `False` | Show the version and exit. |

### `tape chaos`

Drive chaos scenarios + replay + LDFI.

```
Usage: tape chaos COMMAND [ARGS]...
```

#### `tape chaos derive`

LDFI: derive chaos scenarios from one successful run's lineage.

```
Usage: tape chaos derive [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--run`, `-r` | — | The baseline run_id. |
| `--url`, `-u` | — | — |
| `--max-cut` | `1` | Maximum cut size (1 = singletons; >=2 multiplies). |

#### `tape chaos doctor`

Verify the local chaos surface is wired correctly.

```
Usage: tape chaos doctor [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--url`, `-u` | — | — |

#### `tape chaos lineage`

Walk one run's lineage DAG and print each node + its breaking failpoint.

```
Usage: tape chaos lineage [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--run`, `-r` | — | The run_id to walk. |
| `--url`, `-u` | — | — |

#### `tape chaos replay`

Replay a scenario twice with the same seed and check determinism.

```
Usage: tape chaos replay [OPTIONS] SCENARIO
```

**Arguments**

| Name | Help |
|---|---|
| `SCENARIO` | — |

**Options**

| Flag | Default | Help |
|---|---|---|
| `--seed`, `-s` | — | Override the scenario's seed. |
| `--url`, `-u` | — | — |

#### `tape chaos run`

Run a scenario once and print the report.

```
Usage: tape chaos run [OPTIONS] SCENARIO
```

**Arguments**

| Name | Help |
|---|---|
| `SCENARIO` | Path to a scenario .py file. |

**Options**

| Flag | Default | Help |
|---|---|---|
| `--url`, `-u` | — | Tape server URL (default $TAPE_URL). |

### `tape demo`

Theatrical, self-contained demos (the 'show me durability' command).

```
Usage: tape demo COMMAND [ARGS]...
```

#### `tape demo crash-resume`

Crash an agent mid-effect, recover, prove exactly-one wire.

```
Usage: tape demo crash-resume [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--pause`, `-p` | `0.6` | Pause between phases (seconds). Lower = faster demo. |
| `--keep` | `False` | Don't tear down the server / ledger when the demo finishes (so you can `tape inspect`). |
| `--server-binary` | — | Path to a built `tape-server` (default: auto-locate in the repo). |

#### `tape demo unknown-reconcile`

Non-idempotent + OUTBOX + UNKNOWN + reconciler — the full ambiguity loop.

```
Usage: tape demo unknown-reconcile [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--pause`, `-p` | `0.7` | Pause between phases (seconds). UNKNOWN is the loudest signal in the runtime — slowing this down a notch is fine. |
| `--keep` | `False` | Don't tear down the server / ledger when the demo finishes. |
| `--server-binary` | — | Path to a built `tape-server` (default: auto-locate). |

### `tape deploy`

Build & deploy services.

```
Usage: tape deploy COMMAND [ARGS]...
```

#### `tape deploy gcp`

Deploy to GCP (cloud-run / gke / agent-runtime-adapter).

```
Usage: tape deploy gcp [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--target` | `'cloud-run'` | cloud-run \| gke \| agent-runtime-adapter |
| `--store` | — | — |
| `--reactors` | — | Comma-separated subset: recovery,reconciler,outbox,timers,compensation |
| `--image-tag` | `'0.1'` | — |
| `--skip-build` | `False` | — |
| `--out` | `'deploy/gcp/release'` | Where to write the rendered service spec(s). |

### `tape destroy`

Tear down provisioned infra.

```
Usage: tape destroy COMMAND [ARGS]...
```

#### `tape destroy gcp`

Run `tofu destroy` against the generated Terraform.

```
Usage: tape destroy gcp [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--dir` | `'deploy/gcp/terraform'` | Path to the generated Terraform directory. |
| `--yes`, `-y` | `False` | Skip confirmation. |

### `tape dev`

Run server + reactors + agent locally.

```
Usage: tape dev [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--store` | — | Override store: sqlite \| bigtable-emulator \| postgres-emulator. |
| `--events` | — | Override events: none \| pubsub-emulator. |
| `--docker`, `--no-docker` | — | Run via Docker Compose (default: yes if available and store != sqlite). |
| `--server-binary` | — | Path to a built `tape-server` binary (native mode). |
| `--kill-resume-demo` | `False` | Crash mid-run; the recovery reactor resumes; verifies one effect lands. |
| `--port` | `7878` | — |

### `tape doctor`

Diagnose local and GCP setup.

```
Usage: tape doctor [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--local`, `--no-local` | `True` | — |
| `--gcp`, `--no-gcp` | `False` | — |
| `--agents-cli-aware` | `False` | Also run agents-cli scaffold compatibility checks. |
| `--live` | `False` | Query a running tape-server and report operational health (runs needing recovery, UNKNOWN effects, stuck obligations, outbox + timer lag, reactor DLQ). Skips the env checks. |
| `--watch`, `-w` | `False` | With --live: refresh the report in place every --interval seconds (Ctrl-C to stop). Without --live: noop. |
| `--interval` | `2.0` | Refresh interval in seconds for --watch. |
| `--pending-threshold-ms` | `60000` | Effects PENDING longer than this are flagged. |
| `--url` | — | Tape server URL (default: $TAPE_URL or tape.yaml). |

### `tape enhance`

Add Tape to an existing ADK project.

```
Usage: tape enhance [OPTIONS] [PATH]
```

**Arguments**

| Name | Help |
|---|---|
| `PATH` | Path to the project root. |

**Options**

| Flag | Default | Help |
|---|---|---|
| `--name` | — | Project name (defaults to directory name). |
| `--region` | `'us-central1'` | — |
| `--store` | `'sqlite'` | — |
| `--events` | `'none'` | — |
| `--yes`, `-y` | `False` | Accept all prompts. |

### `tape init`

Scaffold a new Tape project.

```
Usage: tape init [OPTIONS] NAME
```

**Arguments**

| Name | Help |
|---|---|
| `NAME` | Project name (lowercase, snake/kebab). |

**Options**

| Flag | Default | Help |
|---|---|---|
| `--here` | `False` | Scaffold into the current directory instead of `./<name>`. |
| `--region` | `'us-central1'` | Default GCP region. |
| `--store` | `'sqlite'` | Default store: sqlite \| postgres \| alloydb \| spanner \| bigtable. |
| `--events` | `'none'` | Default events: none \| pubsub. |
| `--force` | `False` | Overwrite existing files. |

### `tape inspect`

Inspect a run's journal — Textual TUI (default) or rich snapshot / JSONL.

```
Usage: tape inspect [OPTIONS] [RUN_ID]
```

**Arguments**

| Name | Help |
|---|---|
| `RUN_ID` | Run id to inspect. Omit to list recoverable runs. |

**Options**

| Flag | Default | Help |
|---|---|---|
| `--print`, `-P` | `False` | Print a rich snapshot and exit (no Textual app). |
| `--raw` | `False` | JSONL — one JournalEntry per line. Implies streaming. |
| `--summary` | `False` | Stats only — counts by status, duration. Exits 1 on UNKNOWN. |
| `--follow`, `-f`, `--no-follow`, `-F` | `True` | With --raw: keep streaming after drain. Default: yes. |
| `--from-seq`, `-s` | `0` | Start streaming from this seq (0 => from the beginning). |
| `--limit`, `-n` | — | Stop after this many entries (used with --raw / --print). |
| `--list`, `-l` | `False` | List recoverable runs (same as `tape inspect` with no args). |
| `--replay` | `False` | Launch directly into the side-by-side replay-diff screen (FIRST RUN vs REPLAY for every journal entry). |
| `--url` | — | Override the tape server URL. Default: $TAPE_URL or tape.yaml. |

### `tape logs`

Tail Cloud Logging for the deployed services.

```
Usage: tape logs [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--follow`, `-f`, `--no-follow` | `True` | — |
| `--service` | — | Limit to a specific service: tape-server \| tape-reactor-recovery \| ... |
| `--limit`, `-n` | `50` | — |

### `tape migrate`

Run schema migrations for the configured store.

```
Usage: tape migrate [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--store` | — | Override TAPE_STORE for this invocation. |
| `--dry-run` | `False` | — |
| `--server-binary` | — | — |

### `tape provision`

Render & apply infrastructure.

```
Usage: tape provision COMMAND [ARGS]...
```

#### `tape provision gcp`

Render Terraform for GCP — optionally apply.

```
Usage: tape provision gcp [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--store` | — | Override store: alloydb \| postgres \| spanner \| bigtable. |
| `--events` | — | Override events: pubsub \| none. |
| `--target` | — | Override target: cloud-run \| gke. |
| `--region` | — | — |
| `--dry-run`, `--no-dry-run` | `True` | Render only (default). |
| `--apply` | `False` | Render and apply via `tofu apply`. |
| `--out` | `'deploy/gcp/terraform'` | Output directory for Terraform. |

### `tape status`

Show runs / effects / obligations / reactor lag.

```
Usage: tape status [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--limit`, `-n` | `20` | — |

### `tape tail`

Tail the journal across all runs (cross-run live stream).

```
Usage: tape tail [OPTIONS]
```

**Options**

| Flag | Default | Help |
|---|---|---|
| `--subject`, `-s` | `''` | Subject pattern, e.g. '/tape/effect/**'. Supports * (one segment) and ** (rest). |
| `--kind`, `-k` | `''` | Legacy filter (decision\|effect\|obligation\|gate\|value\|run\|event). |
| `--run`, `-r` | `''` | Restrict to one run id. |
| `--predicate`, `-p` | `''` | Server-side CEL predicate on the event (see tape-event-bus.md). |
| `--from-global-seq`, `-g` | `0` | Resume from this global_seq (0 => from earliest). |
| `--from-ts-ms` | `0` | Legacy SubscribeEvents-style ts-cursor (only used when no subject). |
| `--limit`, `-n` | — | Stop after this many entries. |
| `--raw` | `False` | Emit JSONL — one EventEntry per line, no formatting. |
| `--url` | — | Override the tape server URL. Default: $TAPE_URL or tape.yaml. |

