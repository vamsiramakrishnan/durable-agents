# Troubleshooting

When something is wrong, work from the symptom down. Each section starts
with **the symptom**, then **what to check**, then **how to fix**.

If `tape doctor` is unhappy, fix that first — it catches half the
problems on this page.

## Installation & startup

### `tape: command not found`

```
$ tape --version
zsh: command not found: tape
```

**Check.** Did `pip install -e tape/cli` succeed? Is `pip`'s `bin/`
directory on `PATH`?

```
$ pip show tape | grep Location
$ python -c "import shutil; print(shutil.which('tape'))"
```

**Fix.** If installed into a venv, activate it. If `bin/` isn't on
`PATH`, add it. On macOS with Homebrew Python, ensure
`~/Library/Python/3.x/bin` is on `PATH`.

### `tape dev` exits immediately with "no tape-server binary"

```
✗ tape-server not found (cargo missing; docker not on PATH)
```

**Check.** Do you have either `cargo` (for native mode) or `docker`
(for the bundled image)?

**Fix.** Install one. Or point `tape dev --server-binary /path/to/tape-server`
if you've built one separately.

### Schema migration errors on startup

```
✗ migration failed: column "policy_version" already exists
```

**Check.** Did a previous server version run against this store, and
have you upgraded since?

**Fix.** `tape migrate --dry-run` shows what's pending. If the migration
state is genuinely confused (rare), open an issue with the dry-run output
and the store dump.

## Agent / SDK

### `ValueError: @tape.outbox_tool ... requires at least one of business_key=, status_check=, compensate=`

**This is intentional.** A non-idempotent tool with no UNKNOWN-resolution
strategy can't be safely retried.

**Fix.** Add at least one:

- `business_key=lambda ...` if the upstream can be queried by your
  identity.
- `status_check=callable` if you can call the upstream's status API.
- `compensate=callable` if you can undo a confirmed commit.

If you genuinely want to bypass this (after explicit review), pass
`allow_unsafe=True`. The server will still warn.

### Tool body is being re-executed on resume

**This is a contract violation.** A `CONFIRMED` effect should be replayed
from history, not re-executed.

**Check.** What status is the effect in?

```
tape doctor --run r-<id>
```

If the effect is `PENDING` (not `CONFIRMED`), the tool body legitimately
needs to re-run — your code crashed before recording the result.

**Fix.** If `PENDING` and the body has side effects you didn't want
repeated, you've either:

- Forgotten to mark the upstream as idempotent and pass
  `idempotency_key` — fix the tool body.
- Used `@tape.outbox_tool` but performed the upstream call in the body
  instead of returning intent — refactor so the body returns the
  payload only.

### Replay diverges — agent makes a different decision second time

**Check.** Is your prompt construction deterministic? Are you reading
wall-clock, randomness, or mutable external state outside `tape.sample`?
Did `policy_version` change between attempts?

**Fix.** Route nondeterministic reads through `tape.sample(tool_context,
fn)` or make them their own `@tape.effect`. Use `tape.policy_is(...)` to
branch on policy version explicitly. See
[replay & resume](../concepts/replay.md).

### `RunCancelled` raised on a tool body that shouldn't have been cancelled

**Check.** Who called `tape.cancel_run`?

```
tape doctor --run r-<id>      # has the cancellation reason + timestamp
```

**Fix.** Trace back to the caller. If `cancel_run` was called by a
timeout handler you set up, your timeout was too tight; bump it. If by
a user action, the user did the right thing.

## Reactors

### A run is `RUNNABLE` for hours and never re-driven

**Check.** Is the recovery reactor running? Is it leasing the run?

```
tape doctor                                 # local
tape doctor --gcp                           # GCP — checks each reactor SA
```

On GCP, the Cloud Run service for `tape-reactor-recovery` should have
`min-instances >= 1`. If `min-instances=0`, the reactor is sleeping.

**Fix.** Bump `min-instances`. Or, if you're event-driven, check the
Pub/Sub subscription metrics — the push endpoint might be returning
non-200 (Cloud Run cold start, missing IAM, etc.).

### A reactor is stealing leases from another reactor

**This shouldn't happen.** Leases are CAS-acquired; two reactors can't
both think they own the same row.

**Check.** Are two reactors of the *same kind* running? That's fine —
they coordinate via the lease. Are two reactors of *different* kinds
both trying to handle the same state? That's a bug; file an issue.

### Reactor lag is climbing

```
metric: tape/reactor/lag_ms = 120_000   (2 minutes behind)
```

**Check.** Is the reactor CPU-bound? Are there unusually many runs in
the watched state?

```
tape status                                # how many runs?
gcloud run services describe tape-reactor-<name>
```

**Fix.** Scale up (`gcloud run services update --max-instances=N`).
Or switch to event-driven mode for that reactor.

## Effects

### Effect stuck in `UNKNOWN`

**Check.** Is the reconciler reactor running? Does the effect's tool
have a `status_check` or its connector have an `observe`? Is the
upstream's lookup endpoint healthy?

```
tape doctor --run r-<id>           # shows the UNKNOWN effect + connector
```

**Fix.** If the reconciler is running and `observe()` keeps returning
`inconclusive`, the upstream is degraded — the effect is correctly
marked `STUCK`. Page yourself, look at the upstream's books, and
`tape resolve --effect <id> --as confirmed|failed`.

### Effect went `CONFIRMED` but the upstream never received it

**This is rare and indicates an upstream contract violation** (the upstream
acked, then dropped the request). It is *also* what happens if you
manually `tape resolve --as confirmed` when you shouldn't have.

**Check.** What was the connector's `dispatch()` result? What was the
external_ref?

```
tape doctor --run r-<id>
```

**Fix.** Re-dispatch by hand: register a fresh effect via the agent or
the API, idempotency-keyed differently so it doesn't dedup. If you
caused this with a manual `resolve`, you've added a compensation
obligation — `compensate_run`.

## Storage

### Bigtable schema isn't recognised

```
✗ table tape has no column family m
```

**Check.** The Bigtable Terraform module creates the table + family +
GC policy. Did you skip that?

**Fix.**

```bash
tape doctor --gcp --fix    # creates missing schema if possible
# or by hand:
cbt -instance <inst> createtable tape families=m
cbt -instance <inst> setgcpolicy tape m maxversions=1
```

### Spanner refuses to start

```
✗ spanner backend requested but TAPE_SPANNER_EXPERIMENTAL is not set
```

**This is intentional.** The Spanner backend is feature-flagged.

**Fix.** Either set `TAPE_SPANNER_EXPERIMENTAL=1` and accept the trade-off,
or switch to AlloyDB / Bigtable. See [stores](../stores.md).

## Auth & IAM

### `PermissionDenied: caller does not have role roles/run.invoker`

**Check.** Whose identity is the SDK using?

```bash
gcloud auth print-identity-token --audience=https://tape-server-...run.app
```

**Fix.** Grant the caller `roles/run.invoker` on `tape-server`:

```bash
gcloud run services add-iam-policy-binding tape-server \
    --member="serviceAccount:<caller>@<project>.iam.gserviceaccount.com" \
    --role="roles/run.invoker" --region=<region>
```

See [IAM cheat sheet](../deploy/iam.md).

### `Unauthenticated: invalid audience`

**Check.** Is the `tapes://` URL pointing at the Cloud Run service URL?
The ID token's audience is bound to that URL.

**Fix.** Match the URL exactly. `tapes://tape-server-...run.app` is
right; `tapes://internal-lb.example.com` (your LB) is not — re-bind the
audience to the LB URL or hit the service URL directly.

## When all else fails

```bash
tape doctor --dump-run r-<id>    # full state of one run, JSON
tape logs --service tape-server --limit 200
```

Attach both to a [GitHub issue][gh-issues] with what you expected vs.
what you saw. The maintainers' best debugging starts there.

[gh-issues]: https://github.com/vamsiramakrishnan/durable-agents/issues
