# The 60-second demos

Two scenarios, two failure modes, one terminal:

```bash
tape demo crash-resume        # idempotent + inline · agent crashes mid-effect
tape demo unknown-reconcile   # NON-idempotent + outbox · ack lost · reconciler resolves UNKNOWN
```

Both spin up their own server, walk through phases, end with **exactly one
wire** on disk, and exit 0 if and only if exactly-once held. Pick the one
that matches the failure you can't sleep on:

| Scenario | When it bites you | What you watch |
|---|---|---|
| `crash-resume` | the process dies mid-tool — OOM, SIGTERM, deploy roll-over | journal stays `PENDING`; recovery re-drives, reads the bank's own ledger, completes |
| `unknown-reconcile` | the bank call lands but the *ack* is lost — network glitch, timeout, intermediate-proxy crash | effect → `UNKNOWN` (bold red on yellow); reconciler observes the bank by `business_key`; flips to `CONFIRMED` |

---

## `tape demo crash-resume`

It spins up a fresh `tape-server` on a free port, walks through nine phases
of a simulated treasury agent, **actually crashes** the agent mid-effect
(real `os._exit(137)` in a subprocess — no fake), then **actually recovers**
from the journal in this same terminal. The whole thing takes under 60
seconds and exits `0` if and only if the bank ledger ends with **exactly one
wire**. If you can re-run the demo a thousand times and never see two
wires, you've watched durability work.

## What you see

Three panels, side by side, rendered into the terminal you already have:

```
┌───── demo crash-resume ──────────┐  ┌──── journal  (live) ───────────────────┐
│ ✓ tape-server up                │  │ seq +t    kind     status   details   │
│ ✓ decision recorded             │  │   1 +0ms  run      running  treasury  │
│ ✓ wire dispatched (PENDING)     │  │   2 +8ms  decision recorded gemini-2.0│
│ ✓ bank ledger gains the wire    │  │   3 +16ms effect   pending  bank.wire │
│ ✗ agent CRASHES (os._exit) ...  │  │ ─── crash + recovery ───────────────── │
│ ✓ recovery: find + re-drive run │  │   4 +1.2s effect   confirmed bank.wire│
│ ✓ replay READS the journal      │  │   5 +1.2s run      terminal  treasury │
│ ✓ effect → CONFIRMED · run → ... │  └─────────────────────────────────────────┘
│ ✓ verify: exactly ONE wire      │
└────── ✓ durability proved ──────┘
┌───── bank ledger (file-backed) ────────────────────────────────────────────┐
│ wire_id    │ amount     │ account │ idempotency_key                       │
│ wire-0001  │ $2,000,000 │ acct-1  │ <run_id>/decision-0/bank.wire/0       │
└── 1 forward wire on disk — exactly once, even though the agent crashed ───┘
```

The journal panel is **live**: it's the same `subscribe_run` stream the
Inspector uses, so what you see scroll by is exactly what `tape inspect
<run-id>` would show you afterwards.

## How it works

The demo is built to be self-contained — no `google-adk` dependency, no
LLM key, no docker. It uses `TapeClient` directly to journal what an agent
*would* journal:

1. **Phase A** spawns a subprocess that:
   - records a fresh run (`BeginRun`)
   - records `decision#0` (a fake "sweep $2m" decision)
   - opens `effect#0` (`BeginEffect`) — intent journaled as `PENDING`
   - writes the wire to a file-backed fake bank ledger
   - prints `RUN_ID=<id>` so the parent can attach the live stream
   - **calls `os._exit(137)`** before `CompleteEffect` runs

   That's the exact spec window the runtime is designed to survive: the
   counterparty (the bank's ledger) has the wire on disk, but the Tape
   journal still says `PENDING`. The "ack" never came back.

2. **Phase B** (in-process) re-drives:
   - `BeginRun` with the same `invocation_id` returns the existing
     `run_id` (idempotent on the wire)
   - `RecordDecision` is idempotent on `(run_id, decision_index)` — no
     model call
   - `BeginEffect` returns the existing `PENDING` record — no second bank
     call
   - We observe the bank ledger (in real life: the connector's `observe()`)
     and find the wire that landed during the crash
   - `CompleteEffect` flips the effect to `CONFIRMED`
   - `EndRun` closes the run

3. **Verification**: count the entries in the bank ledger. If there's not
   exactly one, the demo exits non-zero. Re-run with `--keep` to leave the
   server running so you can poke it with `tape inspect <run-id>`.

## Re-running with the Inspector attached

If you want to inspect the journal *after* the demo finishes:

```bash
tape demo crash-resume --keep        # leaves the server + ledger alive
# (note the printed URL + run id from the demo's headline)

tape inspect <run-id> --url tape://127.0.0.1:<port>
```

The Inspector's history-vs-live divider will show you the exact same
crash boundary the demo's middle panel showed: rows with `seq ≤ cursor-at-
connect` are dim history; rows above are live.

## Pause speed

The demo throttles itself with a per-phase pause so a human can read what's
happening. For CI or speedruns:

```bash
tape demo crash-resume --pause 0       # fastest — phases blip past
tape demo crash-resume --pause 0.6     # default — readable
tape demo crash-resume --pause 1.5     # theatrical
```

The demo's exit code doesn't depend on the pause — it's always 0 iff
exactly one wire lands.

---

## `tape demo unknown-reconcile`

The harder scenario. Here the agent doesn't crash — the **bank call lands,
but the acknowledgement is lost on the way back**. The dispatcher has no
way to know whether the wire happened. This is the failure mode that breaks
"just retry" — a blind retry would double-wire.

Tape's contract handles it explicitly: the effect transitions to **UNKNOWN**
(rendered in bold red on yellow — the loudest signal in the runtime), the
outbox loop refuses to re-dispatch, and the **reconciler** asks the
counterparty (via the connector's `observe()` method, keyed by the
`business_key`) what really happened. The bank's own ledger is the source
of truth; the journal flips to `CONFIRMED` only when the counterparty
agrees that the operation landed.

What you see, phase by phase:

```
✓ tape-server up
✓ agent: record decision
✓ agent: open OUTBOX effect (NON-IDEMPOTENT) → PENDING (no bank call yet)
✓ outbox dispatcher: claim dispatch lease (CAS)
✓ dispatcher → bank.wire: wire LANDS keyed by business_key
✓ ack lost (network glitch) — record_dispatch_attempt → UNKNOWN
✓ reconciler: list pending/unknown effects, find ours
✓ reconciler: observe(business_key) → bank says CONFIRMED
✓ RecordExternalObservation(CONFIRMED) — effect → CONFIRMED
✓ verify: exactly ONE wire on disk
```

The journal panel shows the effect status transition explicitly:
`pending → unknown → observed`. Each transition is a distinct journal
entry — the journal *is* the audit trail for "what did we think was
happening at this moment".

```bash
# Default — readable.
tape demo unknown-reconcile

# Fast for CI.
tape demo unknown-reconcile --pause 0.05

# Leave the server up so you can poke at it afterwards.
tape demo unknown-reconcile --keep
tape inspect <run-id> --url tape://127.0.0.1:<port>
```

The same exit-code contract applies: `0` iff exactly one wire is on disk.
The demo is the in-vivo proof that an UNKNOWN doesn't become a duplicate
wire.
