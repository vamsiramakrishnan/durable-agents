# Operator path

For: the person who has to **run** Tape in production — pick a store,
configure the reactors, get the right monitoring, know what to do at
3am when the pager fires.

Total time to read end-to-end: about 90 minutes. None of it optional if
you're going to be the one carrying the pager.

---

## 1 · Architecture in one diagram (5 min)

Start with the wiring table — what processes exist, what they talk to,
what holds the state:

→ **[Architecture](../architecture.md)** — server, stores, reactors,
agents; the gRPC contract is the line between them all

→ **[Runtime vs. framework](../runtime-vs-framework.md)** — why Tape
is a separate process, not a library; what that buys you operationally
(scale, deploy, observe each piece independently)

---

## 2 · The reactor crew (15 min)

The agent writes intent; the reactors do the actual work. They're the
processes you care most about scaling, monitoring, and restarting.

→ **[Reactors (concept)](../concepts/reactors.md)** — recovery /
reconciler / outbox / timers / compensation; what each one wakes up to;
how they share work via CAS leases

→ **[Configure reactors (how-to)](../reactors.md)** — enable / disable,
concurrency, polling vs. event-driven

→ **[Run the outbox dispatcher (any language)](../how-to/outbox-daemon.md)** —
the dispatcher binaries for Python / TS / Go / Java; the safety contract
is identical in all four

---

## 3 · The store (15 min)

The journal lives in the store. Picking the right one is mostly about
write throughput and operational fit:

→ **[Pick a storage backend](../stores.md)** — SQLite (dev only),
Postgres / AlloyDB (default for prod), Bigtable (write-heavy), Spanner
(global), and what each one costs

→ **[Leases](../leases.md)** — the CAS primitive that makes multiple
reactor processes safe; lease TTLs, lease takeover, what happens when a
reactor pod restarts

---

## 4 · Visibility (15 min)

You can't run what you can't see:

→ **[Inspect the journal (TUI)](../how-to/inspect.md)** — per-run view,
when you've got a `run_id`

→ **[Triage a live system](../how-to/triage.md)** — `tape doctor --live`
+ `--watch`; the operator dashboard; exit-code-driven monitoring

→ **[Add observability](../observability.md)** — structured logs, OTel
spans, log-based metrics, the bundled Cloud Monitoring dashboard

A common incident loop:

```bash
tape doctor --live --watch          # find the loud rows
tape inspect <run-id> --print | less  # explain how it got there
tape tail --run <run-id>            # watch what reactors do next
```

---

## 5 · Operational behaviours (20 min)

The system's failure modes — and what happens automatically when each
fires:

→ **[UNKNOWN — the third outcome](../concepts/unknown.md)** — what
happens when an ack is lost; the reconciler's job; when to compensate

→ **[Compensation & sagas](../concepts/compensation.md)** — the LIFO
obligation drain; when a `STUCK` obligation needs a human; the
`tape doctor --live` exit code

→ **[Cancel & timeout patterns](../how-to/cancel-timeout.md)** —
`tape.cancel_run`, gate timeouts, heartbeats; how the runtime
distinguishes "killed" from "stalled"

→ **[Configure tenancy](../tenancy.md)** — single-tenant vs. trusted
multi-app vs. hard multi-tenant (the row-level isolation matrix)

---

## 6 · Deploy (15 min)

→ **[Cloud Run (recommended)](../gcp-cloud-run.md)** — `tape provision gcp`,
`tape deploy gcp`, what gets created, what you own

→ **[GKE Autopilot](../gcp-gke.md)** — the alternative for heavier workloads
or when you need long-running tasks

→ **[IAM cheat sheet](../deploy/iam.md)** — every service-account and the
exact role bindings each needs

---

## 7 · Reference, always (lookup, not reading)

→ **[CLI reference](../reference/cli/index.md)** — every flag, every
command, regenerated from the live Typer app

→ **[Cheat sheet](../reference/cheatsheet.md)** — the ten things you'll
need over and over

→ **[FAQ](../help/faq.md)** + **[Troubleshooting](../help/troubleshooting.md)** —
the issues new operators hit first

---

## What's *not* on this path

* The Treatise (`Design > Treatise`). Optional. It's a wonderful read
  for *why* the runtime looks the way it does, but you don't need it to
  run the thing. The **[Systems path](systems.md)** has it as the main
  course.

* The SDK reference pages. You'll want them when wiring your agent code
  — that's the **[Beginner path](beginner.md)** territory.
