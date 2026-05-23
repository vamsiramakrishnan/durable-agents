# Triage a live system (`tape doctor --live`)

`tape doctor` (default) checks **your environment** — Python version, ADK
installed, tape-server reachable, GCP APIs enabled. `tape doctor --live`
checks **the running system** — what's stuck, what's UNKNOWN, what's lagging.
Two jobs, one verb.

Run it during an incident, in CI as a smoke gate, or under `watch`:

```bash
tape doctor --live                            # one-shot snapshot
tape doctor --live --watch --interval 1       # live-refresh; Ctrl-C to stop
tape doctor --live --pending-threshold-ms 30000   # tighter PENDING cutoff
tape doctor --live --url tape://prod:7878     # explicit target
```

## What it shows

```
┌─ tape doctor --live ────────────────────────────────────────────────────┐
│ server                  tape://localhost:7878                          │
│ checked                 2026-05-18 10:30:39 UTC                        │
│ runs needing recovery   ! 3                                            │
│ effects UNKNOWN         ! 1     ← the loud one                         │
│ effects PENDING > 60s   ✓ 0                                            │
│ obligations STUCK       ✓ 0                                            │
│ obligations PENDING/exp ✓ 0                                            │
│ outbox dispatch-ready   ! 12    ← shedding load? reactor down?         │
│ timers overdue          ✓ 0                                            │
└─ operational triage ────────────────────────────────────────────────────┘

Runs needing recovery (3)                  ← detail: who needs which reactor
Effects · UNKNOWN=1  PENDING>60s=0         ← UNKNOWN renders bold red on yellow
Obligations · STUCK=0  PENDING/expired=0
Outbox · dispatch-ready (12)
Timers · due (0)
```

The summary header is the at-a-glance view. The detail tables underneath
show the specific rows that lit up the counters — so you can copy a
`run_id` straight into `tape inspect <run-id>` to drill in.

## Exit codes (for monitoring / CI)

| Code | Meaning |
|---|---|
| `0` | clean — no UNKNOWN effects, no STUCK obligations |
| `1` | at least one UNKNOWN effect — the reconciler hasn't resolved it yet |
| `2` | at least one STUCK obligation — a compensation gave up; needs human triage |

These are intentionally narrow. Lots of runs-to-recover or outbox lag is
**normal** in a healthy busy system (it just means reactors are catching
up); only the loud failure modes — UNKNOWN and STUCK — flip the exit code.

## Wiring it into CI / monitoring

```bash
# CI smoke gate after a deploy: fail the rollout on any UNKNOWN / STUCK.
tape doctor --live --url "$TAPE_URL" || exit 1

# Prometheus-style: parse JSON if you want metrics scraping (TODO).
# For now, --watch is your operator dashboard.
```

## Compared to `tape status` and `tape inspect`

* `tape status` — older, lighter version: just runs-needing-recovery + a
  pending-effects table. `tape doctor --live` is the superset.
* `tape inspect <run-id>` — drill-in on **one** run's journal (TUI or
  rich snapshot). Triage finds the run; inspect explains it.
* `tape tail` — live cross-run journal stream. Triage answers "what's
  wrong now"; tail answers "what's happening now".

A common workflow during an incident:

```bash
tape doctor --live --watch    # find the loud rows
# (read a STUCK obligation row → grab its run_id)
tape inspect abc12345 --print | less    # explain how it got there
tape tail --run abc12345       # watch what reactors do next
```
