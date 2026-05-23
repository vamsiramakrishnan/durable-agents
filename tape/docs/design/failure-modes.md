# Failure modes & how they're proved

A durable runtime earns trust by being *legible under failure*. Anyone can
claim "exactly-once"; the question is what happens specifically when —
the wire dispatch's ack drops, two reactors race for the same claim, a
compensation gives up, the lease expires mid-effect. This document walks
every scenario in the runtime's life, says exactly what the runtime does,
and points to the test that proves it.

The format borrows from Jepsen: each scenario is a **question** ("what
happens when…"), an **answer** (what the runtime does, in the order it
does it), and a **proof** (the test that asserts the answer).

The scenarios are organised by *who fails*, not by what code is running.

---

## A · The agent process dies

### A.1 — The agent crashes mid-effect (inline / idempotent)

**Question.** The agent calls a tool. The tool body writes to the bank.
The Python process `os._exit(137)`s before the tool's return value can be
recorded as `CompleteEffect`. The bank's ledger has the wire; the journal
says `PENDING`. What does Tape do?

**Answer.**

1. Server already has: `decision#0` (memoized), `effect#0 PENDING` (intent
   journaled before the tool ran), no `CompleteEffect` record.
2. The agent's lease on the run expires.
3. The recovery reactor's poll picks up the run via
   `ListRunsToRecover` (status=`RUNNING` + expired lease).
4. `ResumeRun` re-leases the run; the agent is invoked again with the
   same `invocation_id`.
5. `BeginRun(invocation_id=…)` → same `run_id` (idempotent).
6. `BeginEffect(decision_index=0, tool_name=…)` → server returns the
   existing `PENDING` record. The agent's tool body sees the same intent
   it saw before the crash.
7. The tool body calls the bank again. The bank dedups on its own
   `idempotency_key` → returns the same `wire_id`. No second wire.
8. The agent calls `CompleteEffect(CONFIRMED)`. The journal closes.

**Proof.** `tape/tests/test_resume.py::test_crash_then_resume_makes_the_book_close_once`
crashes the treasury agent at `execute_sweep` and at `post_gl`, asserts
exactly one bank wire + one GL batch survive.

**Watch it.** `tape demo crash-resume` — see ["The 60-second demos"](../how-to/demo.md).

---

### A.2 — The agent crashes mid-effect (outbox / non-idempotent)

**Question.** Same as A.1, but the effect is `NON_IDEMPOTENT + OUTBOX`.
The agent crashes after `BeginEffect` but before the outbox dispatcher
got to it. What happens?

**Answer.**

1. Effect is `PENDING + OUTBOX`. The bank has NOT been called (the agent
   never calls the bank for outbox effects — the dispatcher does).
2. Recovery reactor re-leases the run. The agent's re-drive of
   `BeginEffect` returns the existing `PENDING` record. The agent
   continues past it without acting.
3. The outbox dispatcher's tick eventually claims this effect via CAS
   and dispatches it for the first time.

No second wire, no missing wire, no special-case logic. The OUTBOX
contract means the agent's progress doesn't depend on the dispatcher;
the dispatcher's progress doesn't depend on the agent.

**Proof.** `tape/tests/test_non_idempotent.py::test_outbox_reactor_drives_one_call_and_reconciler_resolves_unknown`.

---

## B · The external call lands but the ack doesn't

### B.1 — Dispatch returns UNKNOWN (the loud one)

**Question.** The dispatcher calls the bank. The bank writes the wire.
The HTTP response is lost on the way back — the dispatcher has no proof
the wire happened. What does Tape do?

**Answer.**

1. The connector's `dispatch()` returns `unknown`.
2. The dispatcher calls
   `RecordDispatchAttempt(error=…, next_dispatch_at_ms=0)`.
   The server transitions the effect to `UNKNOWN` and CLEARS the
   dispatch lease.
3. **Important: the outbox does NOT auto-retry.** `UNKNOWN` is terminal
   for the outbox loop. A blind retry would double-wire on a
   non-idempotent upstream.
4. The reconciler picks up the effect via
   `ListPendingEffects(include_unknown=true)`.
5. The reconciler calls the connector's `observe(business_key)`, which
   queries the bank by `business_key`.
6. If the bank says "yes, I have this":
   `RecordExternalObservation(CONFIRMED, external_ref=wire_id)`. Effect
   → `CONFIRMED`.
7. If the bank says "no":
   `RecordExternalObservation(ABSENT)`. For non-idempotent the effect
   stays `UNKNOWN` (operator must approve a re-issue); for idempotent
   the runtime can safely re-issue.

**Proof.**

* `tape/tests/test_non_idempotent.py::test_non_idempotent_unknown_does_not_auto_retry` — the outbox loop refuses to re-dispatch an UNKNOWN.
* `tape/tests/test_non_idempotent.py::test_outbox_reactor_drives_one_call_and_reconciler_resolves_unknown` — the full happy-path resolution.
* `tape/tests/test_non_idempotent.py::test_non_idempotent_absent_becomes_failed_not_pending` — ABSENT on non-idempotent doesn't fall back to retry.
* `tape/tests/test_non_idempotent.py::test_idempotent_unknown_can_retry_after_absent` — the *idempotent* path's different semantics.

**Watch it.** `tape demo unknown-reconcile`.

---

### B.2 — Dispatch raises (non-UNKNOWN exception)

**Question.** The dispatcher's HTTP call raises a generic exception
(connection refused, 500, JSON parse error) — distinct from "ack lost".

**Answer.**

1. The dispatcher catches the exception and calls
   `RecordDispatchAttempt(error=…, next_dispatch_at_ms=now+backoff)`.
2. The server bumps `dispatch_attempts`, sets `last_dispatch_error`,
   keeps the effect in `PENDING`, and updates `next_dispatch_at_ms`.
3. The next outbox tick (after the backoff elapses) will see the effect
   eligible again — same `business_key`, same idempotency on the
   counterparty side, no duplicate.

The runtime distinguishes "I don't know what happened" (UNKNOWN, the
reconciler resolves) from "it definitively didn't happen, retry with
backoff" (PENDING, the outbox retries). This is the contract the proto's
`EffectStatus` enum encodes.

**Proof.** `tape/tests/test_non_idempotent.py::test_outbox_reactor_confirmed_path`
combined with the dispatch-attempt counter assertions in the same suite.

---

### B.3 — Dispatcher process crashes after the call lands

**Question.** The dispatcher writes the wire to the bank, then the
process crashes before it can record the result. Lease is still held
(it hasn't expired). What does Tape do?

**Answer.**

1. The dispatcher's `dispatch_claim_expires_at_ms` is in the future
   but the process is dead — nothing is going to write the result.
2. The lease eventually expires.
3. Another dispatcher process (or the same one restarted) calls
   `ClaimEffectDispatch` and atomically reclaims the slot
   (CAS: `claim_expires_at_ms <= now`).
4. The new dispatcher calls the connector. If the connector's
   `dispatch()` is implemented correctly, it MUST be safe to call again
   (we can't tell from inside what the first dispatch did to the
   counterparty). For idempotent upstreams this is a no-op; for
   non-idempotent ones, the connector typically peeks at the
   counterparty by `business_key` before re-issuing — this is exactly
   what the bank connector does in the example.

**Proof.** `tape/tests/test_non_idempotent.py::test_expired_dispatch_lease_becomes_reclaimable`
+ `test_outbox_claim_is_single_winner`.

---

## C · Two processes try to do the same thing

### C.1 — Two dispatchers claim the same effect

**Question.** Two outbox dispatcher pods see the same dispatch-ready
effect at the same time. Both call `ClaimEffectDispatch`. Who wins?

**Answer.** Exactly one. The server's CAS is on
`(status==PENDING && dispatch_mode==OUTBOX && next_dispatch_at_ms<=now &&
 (dispatch_claimed_by=='' OR dispatch_claim_expires_at_ms<=now))` — the
first call to satisfy that predicate wins the row, the others get
`acquired=false` and skip.

**Proof.** `tape/tests/test_non_idempotent.py::test_outbox_claim_is_single_winner`.

---

### C.2 — Two compensators race for the same obligation

**Question.** Same shape, different table — two compensation reactors
both try to claim the same `PENDING` obligation.

**Answer.** Exactly one wins, via `ClaimObligation`'s CAS on
`(status==PENDING && next_attempt_at_ms<=now && (claimed_by=='' OR
 claim_expires_at_ms<=now))`.

**Proof.** `tape/tests/test_obligations.py::test_concurrent_claims_one_winner`.

---

### C.3 — Lease takeover (recovery reactor)

**Question.** A reactor process crashes while holding a run's lease. How
does the next reactor know it's safe to take over?

**Answer.** The lease has a TTL: `lease_expires_at_ms`. After that point,
the run shows up in `ListRunsToRecover` (status=RUNNING + lease expired).
The next reactor calls `ResumeRun(lease_owner=me)`, which atomically
overwrites the lease. The previous lease-holder, if it's somehow alive
and tries to write anything, has no way to assert "it's still mine" —
every mutating RPC writes through the journal under the current lease.

**Proof.**

* `tape/tests/test_obligations.py::test_expired_lease_is_reclaimable`
  (obligation-side proof of the same CAS pattern).
* The treasury kill-and-resume tests in `test_resume.py` exercise the
  recovery-reactor takeover end-to-end (the test SETS `TAPE_LEASE_MS=1500`
  so the lease genuinely expires during the test's pause).

---

## D · Compensation

### D.1 — A compensation fails repeatedly

**Question.** The compensation has `max_attempts=5`. Each attempt fails.
What does the runtime do at attempt 5?

**Answer.**

1. Each failed attempt calls
   `RecordObligationAttempt(error=…, next_attempt_at_ms=now+backoff)`.
2. The server bumps `attempts`. As long as `attempts < max_attempts`,
   the obligation stays `PENDING` and is eligible for retry after the
   backoff.
3. On the attempt where `attempts >= max_attempts` (or whenever a caller
   passes `next_attempt_at_ms=0`), the server transitions the
   obligation terminally to `STUCK`.
4. `STUCK` obligations show up in `tape doctor --live` with exit code 2
   so an operator gets paged.

**Proof.** `tape/tests/test_obligations.py::test_record_attempt_retries_then_stucks`.

---

### D.2 — `terminal-now` attempt forces STUCK

**Question.** The compensator KNOWS this one isn't recoverable (the
business says "we can't reverse this wire — it's already been reconciled
to another account") and wants to skip the rest of the retries.

**Answer.** Pass `next_attempt_at_ms=0`. The server treats `0` as
"terminal now" and marks the obligation `STUCK` even if attempts are
left.

**Proof.** `tape/tests/test_obligations.py::test_terminal_now_attempt_skips_retries`.

---

### D.3 — DUPLICATE observation creates a compensation

**Question.** The reconciler observes the counterparty and sees TWO
records matching the same `business_key`. That means a previous attempt
already wired AND the current effect's intent landed too — there's a
duplicate on the bank's side. What does Tape do?

**Answer.**

1. `RecordExternalObservation(resolution=DUPLICATE,
    compensate_on_duplicate_kind="reverse_wire")` —
   atomically, in one transaction, the server transitions the effect
   and registers a compensation obligation for the duplicate.
2. The compensation reactor picks up the new obligation and calls the
   connector's `compensate()`, which reverses the duplicate wire.

**Proof.** `tape/tests/test_non_idempotent.py::test_duplicate_observation_creates_compensation`.

---

## E · The safety contract refuses footguns

### E.1 — A NON_IDEMPOTENT + INLINE effect is refused

**Question.** A developer marks an effect `NON_IDEMPOTENT` but forgets
`dispatch_mode=OUTBOX`. What happens?

**Answer.** The server refuses to create the effect with a clear error
(`begin_effect: NON_IDEMPOTENT effects must use OUTBOX dispatch`). And
the Python SDK's `outbox_tool` decorator refuses at construction time
too — both at `import time` (when the agent module loads) and at the
gRPC boundary.

**Proof.**

* Server: `tape/tests/test_non_idempotent.py::test_non_idempotent_inline_is_refused`
* SDK decoration time: `test_business_key_without_connector_is_refused_at_decoration_time`
* Server (companion): `test_business_key_without_connector_is_refused_by_server`

This is the most load-bearing safety invariant in the runtime: an
UNKNOWN that gets blindly retried is the bug the whole project is
designed to prevent.

---

### E.2 — `business_key` dedup at effect creation

**Question.** A bug in the agent re-issues `BeginEffect` with the same
`business_key` but a fresh `idempotency_key` (e.g. a different
`call_index`). The bank would see two requests with the same business
key. What does Tape do?

**Answer.** The server enforces uniqueness on `(connector, business_key)`.
The second `BeginEffect` either returns the existing effect (if the run
matches) or fails with a clear error — the journal never carries two
intents for the same logical operation.

**Proof.** `tape/tests/test_non_idempotent.py::test_business_key_dedupes_effect_creation`.

---

## F · Time-based behaviours

### F.1 — Gate timeout

**Question.** A run is parked on `AwaitSignal("cfo_approval")` with a
`gate_timeout_ms`. No human approves. What happens?

**Answer.**

1. `AwaitSignal` registers a server-side timer with
   `kind=gate_timeout` and `fire_at_ms = now + gate_timeout_ms`.
2. The timers reactor wakes up around `fire_at_ms`, calls
   `ListDueTimers(claim=true)`, gets the timer (atomically marked
   fired).
3. The reactor records the resolution as a `gate_timeout` and
   transitions the run back to `RUNNABLE`.
4. The agent re-runs, observes the gate is closed with
   `resolution="timeout"`, and takes the timeout branch.

**Proof.** `tape/tests/test_reactors.py::test_timer_reactor_fires_a_gate_timeout`.

---

### F.2 — UNKNOWN reconciliation happens off the run's lease

**Question.** A run's lease expires while there's still an unresolved
UNKNOWN effect on it. Can the reconciler still observe and resolve?

**Answer.** Yes. The reconciler operates against `ListPendingEffects`
*cross-run* — it doesn't need a lease on the run to call
`RecordExternalObservation`. The run gets re-leased the next time an
agent (or recovery reactor) wakes it up; by then the effect is already
CONFIRMED / FAILED.

**Proof.** `tape/tests/test_reactors.py::test_reconciler_resolves_an_unknown_effect`.

---

## G · Chaos tests (the explicit ones)

The above scenarios exercise specific failures. The `tape/tests/test_chaos*.py`
suite goes wider — fault injection through proxies, deterministic-replay
through a simulated time/threading runtime (`madsim`), and stateful
property-based tests (Hypothesis). See:

→ **[Chaos & failure testing](chaos.md)** — the underlying framework

→ `tape/tests/test_chaos.py`, `test_chaos_replay.py`,
  `test_chaos_proxies.py`, `test_chaos_stateful.py`,
  `test_chaos_phase3.py`, `test_chaos_mcp_stdio.py`

The chaos harness is what catches regressions in the invariants this
document codifies: it generates fault schedules, runs them against the
real server, and asserts the journal projection ends in a legal state.

---

## What this document is *not*

* **A correctness proof.** The tests assert specific behaviours; they
  don't prove the absence of all bugs. Use it as a map of "what we know
  is right" + "where the tests live", not "what could never go wrong".

* **A SLA.** Recovery time, reconciler latency, and dispatcher
  throughput are operational properties that depend on the store, the
  reactor topology, and the deployment. The contract here is *correctness
  under failure*, not *speed under failure*.

* **An exhaustive list.** Every scenario the tests cover is reachable
  by reading the test files; this document picks the failure modes the
  contract is *designed* to handle. New scenarios go here as we add the
  tests that prove them.
