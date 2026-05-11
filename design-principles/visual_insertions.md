# Visual insertions for “Agents That Act”

Use the SVGs as full-width figures in a web or slide version. Use the ASCII blocks inside the essay when you want the argument to stay textual and sharp. The captions are written in the same register as the article.

---

## 01 — Answer vs Act

**Insert after:** Prelude or Section I, after “A chatbot produces words. An agent produces acts.”  
**SVG:** `01_answer_vs_act.svg`  
**Caption:** The chatbot’s failure remains in language. The agent’s failure crosses into systems that do not know they are inside a conversation.

```text
CHATBOT                                      AGENT
-------                                      -----
user asks                                    user delegates
   │                                            │
   ▼                                            ▼
model produces words                         model chooses act
   │                                            │
   ▼                                            ▼
transcript changes                           world changes
   │                                            │
wrong sentence                                wrong charge / trade / message
   │                                            │
fix: correct / regenerate                     fix: reconcile / compensate / apologize
```

---

## 02 — Retry vs Resume

**Insert after:** Section II, after “The important word is not retry. The important word is resume.”  
**SVG:** `02_retry_vs_resume.svg`  
**Caption:** Retry repeats the story. Resume remembers the story.

```text
NAIVE RETRY

search ──► authorize ──► CRASH
                         state lost
search ──► authorize ──► checkout
             ▲
             └── duplicate risk now belongs to the merchant


DURABLE RESUME

workflow W
   │
   ├─ search        ✓ journaled
   ├─ authorize     ✓ response recorded, key = W/authorize
   ├─ checkout      ○ unfinished
   │
   └─ resume here ──► checkout
```

---

## 03 — The Three Ledgers

**Insert after:** Section III.  
**SVG:** `03_three_ledgers.svg`  
**Caption:** State is not enough. An acting system must remember why, what, and what is owed.

```text
                 AGENT WORKFLOW
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼

DECISION LEDGER   EFFECT LEDGER   OBLIGATION LEDGER
why it acted      what it did     what it owes

prompt            request         approval pending
policy            response        quote expiry
context           idempotency key customer promise
choice            order id        incident lock
alternative       trade id        rollback due

miss it:          miss it:        miss it:
cannot explain    repeats itself  breaks promises
```

---

## 04 — Floor and Ceiling

**Insert after:** Section IX, or as the central figure of the article.  
**SVG:** `04_floor_and_ceiling.svg`  
**Caption:** The old patterns are the floor. The new agent problems are the ceiling.

```text
                 THE CEILING
   decision journaling · adaptive compensation
   action gates · budgets · provenance · replay semantics
                 ───────────────────────────────

        old tools hold the structure up

      │ idempotency │ sagas │ outboxes │ events │ locks │

                 ───────────────────────────────
                  THE FLOOR
   durable effects · retries · timers · signals · reconciliation
```

---

## 05 — Travel Saga: Partial Commit

**Insert after:** Section VII.  
**SVG:** `05_travel_partial_commit.svg`  
**Caption:** A travel agent is not finished when it can book. It is finished when it knows what to do after half a trip exists.

```text
search ─► hold quote ─► wait for approval ─► re-quote ─► book outbound ✓ ─► book return ✗
             │                    │
             │                    └── approval arrives after quote expiry
             ▼
          TTL expires

PARTIAL STATE
  outbound: booked
  return:   rejected
  hotel:    held
  payment:  pre-authorized

NEXT STEP IS NOT “RETRY”
  ├─ compensate: cancel outbound
  ├─ recover:    re-quote return
  └─ escalate:   ask user / policy
```

---

## 06 — Graph vs Trajectory

**Insert after:** Section IX, after “Sagas are graphs. Agents are trajectories.”  
**SVG:** `06_graph_vs_trajectory.svg`  
**Caption:** Classical orchestration is authored. Agent orchestration is discovered.

```text
CLASSICAL SAGA                         LLM AGENT TRAJECTORY

A ──► B ──► C                          observe ─► plan ─► act
│     │     │                              ▲        │       │
│     │     └─ fail                        │        ▼       ▼
│     └──────► ¬B ──► ¬A               verify ◄─ re-plan ◄─ state changed

forward path known                    forward path sampled
rollback path known                   compensation may be decision-bearing
idempotency unit known                idempotency unit must be chosen
```

---

## 07 — The Idempotency Gap

**Insert after:** Section XVII.  
**SVG:** `07_idempotency_gap.svg`  
**Caption:** The merchant can be correct, the agent can be correct, and the workflow can still be ambiguous.

```text
AGENT LEDGER                                      MERCHANT LEDGER
protects user workflow                           protects merchant boundary

workflow W                                       key W/authorize
step authorize                                   request seen once
request sent ─────────────────────────────────► response stored

                   CRASH
                   response lost

agent after restart:
  did the call happen?
  did only the response disappear?
  should I retry?
  should I resume?

without the agent’s journal, the answer is guesswork
```

---

## 08 — Layered Architecture

**Insert after:** Section XXI.  
**SVG:** `08_architecture_layers.svg`  
**Caption:** Composition belongs above. Consequence belongs below.

```text
┌────────────────────────────────────────────────────┐
│ PRODUCT                                            │
│ user intention · UX · business policy              │
├────────────────────────────────────────────────────┤
│ AGENT FRAMEWORK                                    │
│ graphs · prompts · tools · roles · routing         │
├────────────────────────────────────────────────────┤
│ AGENT RUNTIME                                      │
│ decisions · gates · budgets · authority · provenance│
├────────────────────────────────────────────────────┤
│ DURABLE EXECUTION                                  │
│ identity · journal · timers · signals · recovery   │
├────────────────────────────────────────────────────┤
│ SYSTEMS OF RECORD                                  │
│ payments · brokers · CRMs · email · cloud · DBs    │
└────────────────────────────────────────────────────┘
```

---

## 09 — Audit Spine

**Insert after:** Section XXII.  
**SVG:** `09_audit_spine.svg`  
**Caption:** Logs are written for operators. Journals are written for disputes.

```text
AUDIT JOURNAL

1. mandate received
        │
2. fresh state read
        │
3. model decision recorded
        │
4. policy / authority checked
        │
5. external effect submitted
        │
6. obligation created
        │
7. compensation or close

When the dispute arrives, order matters.
```

---

## 10 — Replay Is Not One Thing

**Insert after:** Section XVIII.  
**SVG:** `10_replay_semantics.svg`  
**Caption:** Good runtimes declare what replay means before the crash teaches them.

```text
AGENT TYPE     REPLAY MEANS
----------     ------------
payments       deterministic replay; skip completed effects
browser        re-verify page state before repeating action
coding         restore workspace, diffs, tests, failed hypotheses
research       resume the search tree and provenance
support        reconstruct commitments and obligations

One word, many semantics.
```

---

## Small inline ASCII motifs

These are meant to be sprinkled inside prose as sentence-breakers.

```text
retry  = again
resume = after what already happened
```

```text
words fail in the transcript
acts fail in the ledger
```

```text
merchant ledger ≠ agent ledger
```

```text
a saga is a graph
an agent is a trajectory
```

```text
checkpointed state is not journaled consequence
```

```text
the prompt is advice
the gate is authority
```

```text
the log tells
the journal proves
```

```text
the floor is solved
the ceiling is not
```
