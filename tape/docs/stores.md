# Stores

Tape's backend is a URL. Switch by changing one env var (`TAPE_STORE`) — the
RPC contract doesn't move.

| `TAPE_STORE` | When to use |
|---|---|
| `sqlite:./tape.db` | local dev, single-node, tests |
| `sqlite::memory:` | unit tests |
| `postgres://...`   | self-managed PostgreSQL |
| `alloydb://...`    | recommended GCP production — PostgreSQL-wire-compatible, autoscaling, regional |
| `bigtable://project/instance/table` | very-high-scale; row-oriented; single-row atomic mutations |
| `spanner://project/instance/database` | globally-consistent; **experimental** until the backend graduates |

## AlloyDB

The Tape server speaks plain PostgreSQL to AlloyDB. Use the AlloyDB Auth Proxy
sidecar (the `tape-cloud-run` Terraform module wires it for you) so the server
sees `postgres://tape:...@127.0.0.1:5432/tape` regardless of the underlying
private IP / IAM situation.

Schema is migrated by the server at startup.

## Bigtable

Tape stores its journal in a single Bigtable table with one column family
`m` (`maxversions=1`). The `bigtable` Terraform module creates this for you
— `tape doctor --gcp` verifies it. No more `cbt createtable` choreography.

Operations on a single row are atomic; *cross-row* operations are not. The
journal design respects that: per-run mutations land on one row.

## Spanner — EXPERIMENTAL

The Spanner backend exists in the Rust server but is feature-flagged. The
runtime refuses to boot a Spanner store unless `TAPE_SPANNER_EXPERIMENTAL=1`.
The Terraform module provisions a correct, empty database; the rest is
roadmap.

`tape doctor --gcp` will warn on this.

## Postgres (Cloud SQL)

Same wire protocol as AlloyDB, smaller blast radius. Use it for staging,
qualification, or when AlloyDB is overkill.

## Picking one

  * **Single tenant, modest scale, GCP**: AlloyDB.
  * **Multi-region or extreme scale**: Bigtable.
  * **Eventually**: Spanner once the backend lands.
  * **Local + tests**: SQLite.
