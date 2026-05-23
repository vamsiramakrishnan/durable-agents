# Inspect the journal

The whole point of Tape is that every model decision and every external effect
is in the *journal* — an append-only, durably-stored record of what the
runtime did. The `tape inspect` command makes that journal visible, in real
time, in your terminal.

If you've ever wondered:

* "did the wire actually go out, or did we just *try* and lose the ack?"
* "the agent crashed mid-run — did anything happen, or do we re-drive?"
* "the reconciler is stuck on this run — what does the journal say?"
* "is the lease being held? by whom? for how much longer?"

`tape inspect` answers those without you ever opening a SQL prompt.

---

## The Textual app (default)

```bash
tape inspect <run-id>
```

When stdout is a TTY, this launches a full-screen **Textual app** with four
moving parts:

```
┌─ RUN abc12345 ─────────────────────────────────────────────────────────────┐
│ app/user/session  treasury / cfo / sess-x       status      RUNNING        │
│ lease             reactor-2 (~27s)              seq cursor  9 (live)       │
│ duration          3.4s                          entries     12             │
│ gate              cfo_approval                                             │
└────────────────────────────────────────────────────────────────────────────┘
┌─ timeline ─────────────────────────────────┬─ detail ────────────────────┐
│  seq  +t      type        status   details │ ◆ effect#4 — bank.wire     │
│   1  +0ms    ▶ run        running          │   seq:        4            │
│   2  +12ms   ● decision   recorded         │   global_seq: 47           │
│   3  +18ms   ◆ effect     pending          │   kind:       ◆ effect     │
│ ─── live ─────────────────────────────────│   status:     UNKNOWN      │
│   4  +250ms  ◆ effect     UNKNOWN          │   phase:      live         │
│   5  +1.2s   ◆ effect     confirmed        │   payload: { … }            │
│   6  +1.2s   ↩ obligation pending          │                             │
│   7  +1.3s   ⏸ gate       waiting          │                             │
│   8  +9.4s   ⏸ gate       released         │                             │
│   9  +9.5s   ◆ effect     confirmed        │                             │
└────────────────────────────────────────────┴─────────────────────────────┘
[q] quit  [f] follow  [d/e/o/g] filter  [/] search  [r] raw  [c] copy
```

The live moments worth calling out:

* **The history / live divider.** Entries that were already in the journal
  when you connected are dimmed; entries arriving after are full color, with
  a "── live ──" rule between them. That is what replay-vs-live looks like
  in a real run — you can SEE the boundary instead of having to mentally diff
  the seq cursor.

* **The lease countdown.** The header re-renders every 500ms with the current
  lease-expiration delta. When the lease is about to expire, you'll watch it
  tick down; when the recovery reactor takes over, you'll see the owner
  string change in place.

* **The UNKNOWN status badge.** Failed (FAILED) is bad; UNKNOWN is *worse* —
  the ack was lost, the act may or may not have happened, and the
  reconciler has to ask the counterparty. We render UNKNOWN in **bold red on
  yellow** so it screams at you. That's the failure mode the whole
  non-idempotent contract is designed to make survivable; the inspector makes
  it visible.

### Keyboard

| Key | What |
|---|---|
| `q` / `ctrl+c` | quit |
| `f` | toggle auto-follow (jump-to-bottom on new entries) |
| `a` / `d` / `e` / `o` / `g` / `v` | filter to **a**ll / **d**ecisions / **e**ffects / **o**bligations / **g**ates / **v**alues |
| `/` | open the search bar — live-filters the timeline as you type |
| `escape` | clear the search |
| `r` | preview the selected entry's payload (notification toast) |
| `c` | copy the selected entry's full JSON to the clipboard |
| `home` / `end` | jump to first / last entry |
| `↑` / `↓` / `pgup` / `pgdn` | navigate the timeline |

### When the run finishes

When the server closes the stream (the run reached TERMINAL / FAILED /
STUCK), the header shows a dim "stream closed" note and the TUI stays open
so you can scroll back through the timeline. Press `q` to exit.

### The replay diff (`R`)

Replay is the hardest concept in a durable runtime. People read "the SDK
reads the journal instead of re-calling the model" and nod, but nodding
isn't understanding. Press **`R`** in the Inspector (or launch with
`tape inspect <id> --replay`) and you get a side-by-side teaching screen:

```
┌─────────────────── REPLAY IS READS, NOT WRITES ──────────────────────┐
│ Every external action on the left becomes a journal read on the right.│
└───────────────────────────────────────────────────────────────────────┘
┌─── FIRST RUN ─────────────────────┬─── REPLAY ───────────────────────┐
│ seq  +t   call          what       │ seq  +t   call    what          │
│  1  +0ms  write         BeginRun  │  1  +0ms  read    BeginRun       │
│              minted fresh run_id   │             returns existing     │
│  2  +8ms  model call    Record... │  2  +8ms  read    GetDecision     │
│              called gemini-2.0     │             reads recorded resp  │
│              persisted response    │             without calling model│
│  3  +16ms external call CompleteEff│  3  +16ms read    BeginEffect     │
│              tool returned         │             short-circuits on    │
│              status=CONFIRMED      │             CONFIRMED — no tool  │
└────────────────────────────────────┴────────────────────────────────────┘
[escape] back to timeline   [home/end] jump
```

Move the cursor on either side — the other follows. Every "external call"
on the left has a corresponding "read" on the right. The whole replay
contract in one screen: re-driving an agent is safe because the runtime
**memoizes**. One wire, one model call, one signal — no matter how many
times the agent crashes and resumes.

The 60-second crash-resume demo (`tape demo crash-resume`) is the matching
in-vivo proof.

---

## Non-interactive modes

The TUI is great for humans. For pipelines, CI, screenshots, and operator
muscle memory, four non-TTY modes share the same decoder:

```bash
# Snapshot — rich-rendered table, one-shot, exits.
tape inspect <id> --print

# JSONL — one JournalEntry per line; pipe to jq.
tape inspect <id> --raw --no-follow | jq .
tape inspect <id> --raw            | jq -c '.kind, .ts_ms'    # live stream

# Stats — counts by status, duration. Non-zero exit on UNKNOWN/STUCK
# — usable as a CI smoke gate after a treasury-style integration test.
tape inspect <id> --summary

# Listing — recoverable runs (RUNNABLE, expired leases, released WAITING).
tape inspect
tape inspect --list      # same; explicit alias
```

`--print` is the default when stdout is not a TTY (so `tape inspect <id> >
log.txt` captures the rendered snapshot, not the Textual app).

---

## Cross-run view: `tape tail`

`tape inspect` follows one run. `tape tail` follows them all:

```bash
# Everything across every run, live.
tape tail

# Just effects, across every run.
tape tail --subject '/tape/effect/**'

# Just effects that went UNKNOWN — the "what's broken" view.
tape tail --subject '/tape/effect/*/unknown'

# Server-side CEL filter on the event payload.
tape tail --predicate 'subject.contains("treasury")'

# One run, simpler renderer than inspect.
tape tail --run abc123

# JSONL for jq pipelines.
tape tail --raw --subject '/tape/effect/**' | jq -c '{run: .run_id, kind, status: .payload.status}'
```

Under the hood, `tape tail` calls `SubscribeBySubject` (subject patterns)
or the legacy `SubscribeEvents` (kind/run filters) — both flow through the
same WAL.

---

## Cookbook

### Watch a single run from the moment it starts

```bash
# In one terminal:
tape dev                                  # local server + reactors + agent

# In another terminal:
tape inspect                              # find the run id
tape inspect <id>                         # follow it live
```

### Verify a treasury demo in CI

```bash
tape inspect "$RUN_ID" --summary || {
  echo "non-clean treasury run — see journal:"
  tape inspect "$RUN_ID" --print
  exit 1
}
```

### Stream every UNKNOWN effect to your alerts pipeline

```bash
tape tail --subject '/tape/effect/*/unknown' --raw \
  | jq -c '{run: .run_id, key: .payload.idempotency_key, ts: .ts_ms}' \
  | your-alerts-shipper
```

### Replay one run's journal into a debugger

```bash
tape inspect "$RUN_ID" --raw --no-follow > journal.jsonl
jq -s '.' journal.jsonl | less          # full structured dump
```

---

## What gets shown

Every journal **kind** (proto: `JournalEntry.kind`) has a glyph + a decoder:

| Kind | Glyph | What it represents |
|---|---|---|
| `run`        | ▶ | run-lifecycle transitions (start / terminal / failed / stuck) |
| `decision`   | ● | a recorded model decision (the choice point) |
| `effect`     | ◆ | a tool call's intent + result (status: pending / confirmed / failed / UNKNOWN) |
| `obligation` | ↩ | a compensation (status: pending / committed / compensated / stuck) |
| `gate`       | ⏸ | a human-in-the-loop signal gate (waiting / released) |
| `timer`      | ⏱ | a server-side timer (set / fired) |
| `value`      | ≡ | a reactive KV write (the "coordinate through state" surface) |
| `event`      | ✦ | generic event-bus entry |

The status badge maps to a colour: `confirmed` / `released` / `terminal` are
green; `pending` / `cancelled` are yellow; `failed` / `STUCK` are red;
`UNKNOWN` is **bold red on yellow** — the loudest signal we have, because it
is the loudest situation in the runtime.

---

## What it doesn't do (yet)

* **A web UI.** The terminal is the source of truth right now (priority 10);
  a browser-based equivalent will share the same gRPC contract.
* **A richer `ls`.** `tape inspect` (no args) shows runs needing recovery —
  the operator's hot set. A "show me every recent run regardless of state"
  needs a new server RPC; we'll add it when it's earning its keep.
