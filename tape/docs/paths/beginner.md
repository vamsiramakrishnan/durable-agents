# Beginner path

For: someone who's heard "durable execution" and "exactly-once" and wants
the smallest possible loop from *zero* to *I get it*.

Total time: about 45 minutes — much of it watching things happen rather
than reading.

---

## 1 · See it (3 min)

Run the demo first. It's self-contained, it requires zero setup, and it
proves the contract in 60 seconds:

```bash
tape demo crash-resume
```

→ **[The 60-second demos](../how-to/demo.md)** — what the demo actually does

The journal panel scrolling past you, the crash divider, the bank ledger
ending with exactly one wire — that's the entire pitch. If you skipped it,
go back. None of the rest will land as hard without it.

Then run the harder one:

```bash
tape demo unknown-reconcile
```

This is the failure mode that breaks "just retry" — and the one Tape's
contract handles explicitly. You'll see `UNKNOWN` rendered in bold red on
yellow (intentional — UNKNOWN is the loudest signal in the runtime).

---

## 2 · Drill into the journal (5 min)

Re-run the demo with `--keep`, then open the Inspector:

```bash
tape demo crash-resume --keep
# note the printed URL + run id
tape inspect <run-id> --url <printed-url>
```

→ **[Inspect the journal](../how-to/inspect.md)** — the TUI tour

Press **`R`** to switch to the replay-diff screen. Read every paired row.
This is the whole contract, side-by-side: every external call on the left
becomes a journal read on the right.

→ **[Replay & resume (concept)](../concepts/replay.md)** — the WHY

---

## 3 · Quickstart on your own code (15 min)

Now wire Tape into an actual ADK agent:

→ **[Quickstart (10 min)](../quickstart.md)** — `tape init`, install, run

→ **[ADK on Tape](../adk.md)** — what Tape adds, what stays the same

The "wire it in fifteen lines" panel on the home page is the headline; the
quickstart fills in the install + first-run details.

---

## 4 · Learn the model (20 min)

Now that you've seen it run, the concept pages will make sense — they're
descriptions of things you've already watched happen.

Read them in this order:

→ **[The journal](../concepts/journal.md)** — the append-only WAL is the
contract; everything else is a projection

→ **[Effects & idempotency](../concepts/effects.md)** — `PENDING /
CONFIRMED / FAILED / UNKNOWN`; the short-circuit on replay; why
idempotency keys are derived from the run

→ **[UNKNOWN — the third outcome](../concepts/unknown.md)** — why blind
retry on a wire breaks; what `observe()` does; the reconciler's job

→ **[Replay & resume](../concepts/replay.md)** — what re-driving an
agent actually does; deterministic-replay constraints; the in-vivo and
in-vitro proofs you ran in step 1 and 2

→ **[First crash-survival](../start/first-crash-survival.md)** — write
your own crash and watch your own recovery

---

## 5 · What's next?

You've seen the inspector, you've watched the journal during a crash,
you've read the model. Two natural next moves:

- **Going to production?** Switch to the **[Operator path](operator.md)** —
  stores, reactors, Cloud Run, monitoring, triage.
- **Want the underlying argument?** Switch to the **[Systems path](systems.md)** —
  why a *runtime* (not a *framework*), the WAL/projection split, the
  event-bus rebuild.

You don't have to pick one. You just have to know which one you need
right now.
