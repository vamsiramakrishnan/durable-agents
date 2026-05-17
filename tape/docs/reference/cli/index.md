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

