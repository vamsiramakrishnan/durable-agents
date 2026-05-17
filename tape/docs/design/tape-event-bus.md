# Tape — the event bus rebuild

*Companion to [`tape.md`](tape.md).* This document is the spec for the rebuild
of Tape's reactive surface from a 250 ms polling loop into a proper event bus:
subject-routed, server-pushed, globally-ordered, back-pressured, and with a
clean split between **runs** (durable, multi-step agent work) and **tasks**
(cheap, ephemeral handler work). The motivation is the critique that opens this
PR; this doc is the answer.

---

## 1. The diagnosis

The Tape J/K shape we shipped earlier conflated three things that a senior
team would keep separate:

1. **The journal** is the *source of truth* — append-only, per-run-ordered.
   That stays.
2. **The bus** is the *transport* over the journal — server-pushed change
   feed, subject routing, predicates, back-pressure. This is what we're
   rebuilding.
3. **The dispatcher** is where reactions actually run — in-process for dev,
   Cloud Pub/Sub for cloud-native, NATS/Kafka for self-hosted. Tape ships
   the contract and a reference in-proc dispatcher; the broker does the
   brokering.

The mistakes we're fixing, in order of consequence:

| Mistake | Fix |
|---|---|
| `SubscribeEvents` is a `tokio::sleep(250 ms)` poll loop | Postgres `LISTEN`/`NOTIFY` for wake-ups; SQLite/Bigtable retain a short poll as the fallback; the streaming RPC stays the same shape on the wire. |
| Cursor is `(ts_ms, run_id, seq)` — fragile under clock skew + concurrent writers | `global_seq BIGINT` allocated by a Postgres sequence (`BIGSERIAL`) / SQLite `INTEGER PRIMARY KEY AUTOINCREMENT` / Bigtable change-stream commit timestamp. One integer per cursor. |
| "Reactions are runs" — 10 k/s state changes ⇒ 10 k runs/s | Two handler kinds: `agent` ⇒ creates a `tape_run` (heavy, durable, multi-step); `task` and `publish` ⇒ creates a `tape_task` (cheap, ephemeral, dispatcher-retried). |
| No subjects: every consumer scans the WAL | Every journal entry gets a `subject` (path-style: `/tape/effect/confirmed/execute_sweep/<run_id>`). Reactions subscribe to subject patterns (`/tape/effect/confirmed/**`). |
| No back-pressure | Per-reaction `max_concurrency`, `rate_limit_per_s`, `debounce_ms`, `retry_policy`, `dlq_after_n` — all stored on the reaction row, enforced by the dispatcher. |
| Filter predicates were unspecified | **CEL** (Google's Common Expression Language) evaluated server-side, against the envelope `{run_id, subject, kind, payload}`. |
| No OTel propagation | `trace_id`, `span_id`, `parent_span_id` on every journal entry; reaction-triggered work is a child span of the triggering entry. |
| No payload schema versioning | `schema_version SMALLINT` on every journal row, defaulting to 1, set by the producer. |
| Single-threaded per-reaction consumption | `num_shards` on each reaction; cursor is per-`(reaction_id, shard)`; tasks hashed by `hash(run_id) % num_shards`. |

---

## 2. The surfaces

### 2.1 The journal — augmented, not replaced

```
tape_journal:
  global_seq      BIGINT PRIMARY KEY  -- Postgres BIGSERIAL / SQLite INTEGER PK AUTOINCREMENT
  run_id          TEXT
  seq             BIGINT              -- the per-run sequence (kept for back-compat / replay)
  kind            TEXT                -- decision | effect | obligation | gate | value | run | event
  subject         TEXT                -- /tape/<kind>/<verb>/<dim1>/<dim2>...
  payload_json    TEXT
  schema_version  SMALLINT NOT NULL DEFAULT 1
  trace_id        TEXT NOT NULL DEFAULT ''
  span_id         TEXT NOT NULL DEFAULT ''
  parent_span_id  TEXT NOT NULL DEFAULT ''
  ts_ms           BIGINT
  UNIQUE (run_id, seq)
```

`global_seq` is the durable cursor for every cross-run consumer (outbox relay,
reactors, fan-out). It is **strictly monotonic** within a database. Across
multiple Tape replicas pointed at the same Postgres / AlloyDB, it is
allocated by the sequence — collisions impossible. On Bigtable, change-stream
commit timestamps play the same role (see §6).

The `(run_id, seq)` UNIQUE constraint is preserved so per-run replay
(`SubscribeRun`, recovery) still works.

### 2.2 Subjects

Path-style, `/`-delimited, lower-case. The grammar:

```
/tape/<kind>/<verb>/<dim1>/<dim2>...
```

Wildcards in subscription patterns:
* `*`  — matches exactly one path segment
* `**` — matches zero or more trailing segments (only at the end of a pattern)

Canonical subjects emitted by Tape:

| Journal event | Subject |
|---|---|
| Run started / lifecycle change | `/tape/run/<status_lower>/<app>/<user>/<session>/<run_id>` |
| Decision recorded | `/tape/decision/recorded/<run_id>/<decision_index>` |
| Effect pending | `/tape/effect/pending/<tool>/<run_id>` |
| Effect confirmed | `/tape/effect/confirmed/<tool>/<run_id>` |
| Effect failed | `/tape/effect/failed/<tool>/<run_id>` |
| Effect unknown | `/tape/effect/unknown/<tool>/<run_id>` |
| Effect reconciled | `/tape/effect/reconciled/<tool>/<run_id>` |
| Obligation registered | `/tape/obligation/registered/<kind>/<run_id>` |
| Obligation resolved | `/tape/obligation/<status>/<kind>/<run_id>` |
| Gate waiting | `/tape/gate/waiting/<gate>/<run_id>` |
| Gate released | `/tape/gate/released/<gate>/<run_id>` |
| Value changed | `/tape/value/changed/<namespace>/<key>` |
| Value deleted | `/tape/value/deleted/<namespace>/<key>` |
| Session event appended | `/tape/event/appended/<app>/<user>/<session>` |

Path segments are URL-segment-encoded (slashes, spaces, `*`, `**` get
percent-encoded so user-chosen keys can't break the grammar).

Examples of subscription patterns:

```
/tape/value/changed/treasury/**           any value in the "treasury" namespace
/tape/effect/confirmed/execute_sweep/*    every confirmed sweep effect, any run
/tape/effect/**                           every effect event of any kind
/tape/decision/recorded/*/0               every first-decision (decision_index=0) of any run
```

### 2.3 Reactions

A *reaction* is a server-side registration: "for journal entries matching this
subject pattern (and optionally this CEL predicate), produce work." The work
is one of three kinds:

* `agent`  — re-invoke an agent (creates a `tape_run`, journaled, durable);
* `task`   — enqueue an ephemeral task into `tape_tasks` for the dispatcher
  to claim and run a local Python function on (no run row, no lease, no
  recovery rescan);
* `publish`— enqueue an ephemeral task whose dispatcher publishes it to an
  external broker (Pub/Sub topic, NATS subject, …).

```
tape_reactions:
  reaction_id      TEXT PRIMARY KEY    -- stable; SDK-provided or server-generated
  name             TEXT                -- human label
  subject_pattern  TEXT NOT NULL       -- /tape/.../<seg>/*/<...>/**
  predicate_cel    TEXT NOT NULL DEFAULT ''  -- empty = always-true
  handler_kind     SMALLINT NOT NULL   -- 1=agent, 2=task, 3=publish
  agent_app        TEXT NOT NULL DEFAULT ''  -- kind=agent: which app to re-invoke
  publish_target   TEXT NOT NULL DEFAULT ''  -- kind=publish: e.g. pubsub://proj/topic
  max_concurrency  INTEGER NOT NULL DEFAULT 1
  rate_limit_per_s INTEGER NOT NULL DEFAULT 0     -- 0 = unbounded
  debounce_ms      INTEGER NOT NULL DEFAULT 0     -- coalesce repeated keys
  retry_max        INTEGER NOT NULL DEFAULT 5
  retry_backoff_ms INTEGER NOT NULL DEFAULT 1000  -- starting backoff; doubles
  dlq_after_n      INTEGER NOT NULL DEFAULT 5     -- after this many attempts, move to DLQ
  num_shards       INTEGER NOT NULL DEFAULT 1
  created_at_ms    BIGINT NOT NULL
  deleted          INTEGER NOT NULL DEFAULT 0
```

```
tape_reaction_cursors:
  reaction_id        TEXT
  shard              INTEGER
  last_global_seq    BIGINT NOT NULL DEFAULT 0
  last_processed_at_ms BIGINT NOT NULL DEFAULT 0
  PRIMARY KEY (reaction_id, shard)
```

The matcher (a server-side tokio task) tails `tape_journal` ordered by
`global_seq`, evaluates each entry against every active reaction, and:

* `kind=agent`  → idempotently calls `BeginRun` for the target app, threading
  the source `(run_id, global_seq)` as `invocation_id` so a duplicate match
  doesn't re-launch.
* `kind=task` / `publish` → inserts a row into `tape_tasks` keyed by
  `(reaction_id, shard, source_global_seq)` so duplicate matches collapse.

### 2.4 Tasks

```
tape_tasks:
  task_id              TEXT PRIMARY KEY
  reaction_id          TEXT NOT NULL
  shard                INTEGER NOT NULL
  source_run_id        TEXT NOT NULL
  source_global_seq    BIGINT NOT NULL
  subject              TEXT NOT NULL
  payload_json         TEXT NOT NULL DEFAULT ''   -- the triggering journal entry payload
  status               SMALLINT NOT NULL          -- 1=pending 2=claimed 3=done 4=failed 5=dlq
  attempts             INTEGER NOT NULL DEFAULT 0
  next_attempt_at_ms   BIGINT NOT NULL DEFAULT 0
  lease_owner          TEXT NOT NULL DEFAULT ''
  lease_expires_at_ms  BIGINT NOT NULL DEFAULT 0
  last_error           TEXT NOT NULL DEFAULT ''
  created_at_ms        BIGINT NOT NULL
  completed_at_ms      BIGINT NOT NULL DEFAULT 0
  trace_id             TEXT NOT NULL DEFAULT ''
  span_id              TEXT NOT NULL DEFAULT ''
  UNIQUE (reaction_id, shard, source_global_seq)
```

A task is the unit of work for the dispatcher:

```
loop:
  tasks = ClaimTasks(reaction_id=..., shard=..., owner=..., lease_ms=..., max=N)
  for t in tasks:
    fire_handler(t); CompleteTask(...) or NackTask(...)
```

`NackTask` with `permanent=true` after `attempts >= dlq_after_n` flips status
to `DLQ` and stops re-leasing.

### 2.5 CEL predicates

CEL expressions are evaluated server-side against an `Envelope` map:

```
envelope = {
  "global_seq": int,
  "run_id":     str,
  "seq":        int,
  "kind":       str,
  "subject":    str,
  "ts_ms":      int,
  "schema_version": int,
  "payload":    parsed payload_json,
  "trace_id":   str,
}
```

Example:

```
RegisterReaction(
  subject_pattern = "/tape/value/changed/treasury/**",
  predicate_cel   = "payload.value.value_json != prev_value_json && double(payload.value.value_json) > 100.0",
  handler_kind    = "agent",
  agent_app       = "treasury-pricing",
  max_concurrency = 32,
  debounce_ms     = 500,
)
```

CEL keeps the predicate language *sandboxed*, *serializable* (it is just a
string), and *fast* (millions of evals/sec). We use `cel-interpreter` on the
server and `celpy` in the Python SDK so identical expressions work locally.

If `predicate_cel` is the empty string, the predicate is treated as `true`
(matches everything that hit the subject pattern). If a predicate throws an
evaluation error, the entry is *skipped* (logged as a reaction error metric);
errors don't pile up in the queue.

### 2.6 OTel propagation

`trace_id` / `span_id` / `parent_span_id` are W3C trace-context bytes encoded
as hex strings on every journal entry. The SDK reads them off the current
OTel context when calling the journaling RPC; if there's no current span, they
are empty. The matcher copies `(trace_id, span_id)` from the source entry
into the task's `(trace_id, parent_span_id)`, so a reaction-triggered run /
handler is a *child span* of the journal entry that triggered it.

### 2.7 Schema versioning

`schema_version SMALLINT NOT NULL DEFAULT 1`. Producers set it when they
write a payload; consumers branch on it when they decode. Bumping the version
of a payload type does *not* require a schema migration — the column exists
day 1, and code that decodes the new version can fall back to the old. The
journal is forever, the wire shape is not.

---

## 3. RPC contract (proto, additions only)

```
service Tape {
  // ... all existing RPCs preserved ...

  // ── reactions ──
  rpc RegisterReaction   (Reaction)                  returns (Reaction);
  rpc DeregisterReaction (DeregisterReactionRequest) returns (DeregisterReactionResponse);
  rpc ListReactions      (ListReactionsRequest)      returns (ListReactionsResponse);

  // ── tasks ──
  rpc ClaimTasks    (ClaimTasksRequest)    returns (ClaimTasksResponse);
  rpc CompleteTask  (CompleteTaskRequest)  returns (CompleteTaskResponse);
  rpc NackTask      (NackTaskRequest)      returns (NackTaskResponse);
  rpc ListTasks     (ListTasksRequest)     returns (ListTasksResponse);    // observability / DLQ inspection

  // ── subject-filtered, global-seq-cursored WAL stream ──
  rpc SubscribeBySubject (SubscribeBySubjectRequest) returns (stream EventEntry);
  // SubscribeEvents is preserved for back-compat; it now also accepts
  // `from_global_seq` and `subject_pattern` fields (the old `from_ts_ms`
  // still works).
}

message Reaction {
  string reaction_id      = 1;
  string name             = 2;
  string subject_pattern  = 3;
  string predicate_cel    = 4;
  HandlerKind handler_kind = 5;
  string agent_app        = 6;
  string publish_target   = 7;
  int32  max_concurrency  = 8;
  int32  rate_limit_per_s = 9;
  int32  debounce_ms      = 10;
  int32  retry_max        = 11;
  int32  retry_backoff_ms = 12;
  int32  dlq_after_n      = 13;
  int32  num_shards       = 14;
  int64  created_at_ms    = 15;
  bool   deleted          = 16;
}

enum HandlerKind {
  HANDLER_KIND_UNSPECIFIED = 0;
  HANDLER_KIND_AGENT       = 1;
  HANDLER_KIND_TASK        = 2;
  HANDLER_KIND_PUBLISH     = 3;
}

message Task {
  string task_id             = 1;
  string reaction_id         = 2;
  int32  shard               = 3;
  string source_run_id       = 4;
  int64  source_global_seq   = 5;
  string subject             = 6;
  string payload_json        = 7;
  TaskStatus status          = 8;
  int32  attempts            = 9;
  int64  next_attempt_at_ms  = 10;
  string lease_owner         = 11;
  int64  lease_expires_at_ms = 12;
  string last_error          = 13;
  int64  created_at_ms       = 14;
  string trace_id            = 15;
  string parent_span_id      = 16;
}

enum TaskStatus {
  TASK_STATUS_UNSPECIFIED = 0;
  TASK_STATUS_PENDING     = 1;
  TASK_STATUS_CLAIMED     = 2;
  TASK_STATUS_DONE        = 3;
  TASK_STATUS_FAILED      = 4;
  TASK_STATUS_DLQ         = 5;
}

message ClaimTasksRequest {
  string reaction_id = 1;
  int32  shard       = 2;
  string owner       = 3;
  int64  lease_ms    = 4;
  int32  max         = 5;
}
message ClaimTasksResponse { repeated Task tasks = 1; }
message CompleteTaskRequest { string task_id = 1; string owner = 2; }
message CompleteTaskResponse { Task task = 1; }
message NackTaskRequest { string task_id = 1; string owner = 2; string error = 3; bool permanent = 4; }
message NackTaskResponse { Task task = 1; }
message ListTasksRequest { string reaction_id = 1; TaskStatus status = 2; int32 limit = 3; }
message ListTasksResponse { repeated Task tasks = 1; }

message SubscribeBySubjectRequest {
  string subject_pattern = 1;     // /tape/effect/confirmed/**
  string predicate_cel   = 2;     // optional, may be empty
  int64  from_global_seq = 3;     // 0 => from earliest available
}

// EventEntry / JournalEntry gain global_seq, subject, schema_version, OTel cols.
message EventEntry {
  string run_id          = 1;
  int64  seq             = 2;
  string kind            = 3;
  string payload_json    = 4;
  int64  ts_ms           = 5;
  int64  global_seq      = 6;
  string subject         = 7;
  int32  schema_version  = 8;
  string trace_id        = 9;
  string span_id         = 10;
  string parent_span_id  = 11;
}

message DeregisterReactionRequest { string reaction_id = 1; }
message DeregisterReactionResponse { bool deregistered = 1; }
message ListReactionsRequest { string subject_pattern = 1; }
message ListReactionsResponse { repeated Reaction reactions = 1; }
```

The existing `SubscribeEventsRequest` gains two fields (`subject_pattern`,
`from_global_seq`) — old callers that pass only `from_ts_ms` keep working.

---

## 4. Python SDK shape

```python
import tape

# ── declare reactions at startup ──
@tape.on_value_change("treasury", key="fx_rate",
                      predicate="double(payload.value.value_json) > 1.10",
                      max_concurrency=8, debounce_ms=500)
def reprice_book(envelope):
    # ephemeral task; this Python fn runs in the dispatcher process.
    ...

@tape.on_effect_confirmed("execute_sweep",
                          agent="treasury-followup",     # ⇒ HandlerKind.AGENT
                          max_concurrency=4)
def _():  # body is unused for agent handlers; the decorator registers the reaction
    ...

@tape.on("/tape/effect/failed/**",
         publish="pubsub://my-proj/incident-stream")    # ⇒ HandlerKind.PUBLISH
def _():
    ...

# ── run the dispatcher (in-proc reference impl) ──
tape.reactions.run_dispatcher(url="tape://localhost:7878")

# ── or, run the Pub/Sub bridge: pull tasks → publish to Pub/Sub ──
tape.reactions.run_pubsub_bridge(url="tape://localhost:7878",
                                 project="my-proj", topic="tape-tasks")
```

The decorator builds a `Reaction` proto and calls `RegisterReaction` on
import. `run_dispatcher` loops `ClaimTasks`/`CompleteTask`, enforcing
`max_concurrency` with a local semaphore, `rate_limit_per_s` with a token
bucket, `debounce_ms` by coalescing `(reaction_id, subject)` within the
window, and the retry/DLQ policy by setting `NackTask(permanent=...)` once
`attempts >= dlq_after_n`.

OTel: the dispatcher opens a span as a child of `(trace_id, parent_span_id)`
on every task; the handler runs inside that span.

### 4.1 Outbox relay — upgraded

`run_outbox_relay` now reads journal entries via `SubscribeBySubject`
(with subject_pattern `/tape/**` by default) using `from_global_seq` instead
of `from_ts_ms`. The cursor file becomes:

```json
{"last_global_seq": 8421}
```

A one-line migration helper transparently converts the old `{from_ts_ms,
last_run_id, last_seq}` cursor.

---

## 5. The dispatcher contract (so other brokers can implement it)

A dispatcher is anything that:

1. calls `ClaimTasks(reaction_id, shard, owner, lease_ms, max)`,
2. runs the handler (or publishes to its broker),
3. calls `CompleteTask` on success or `NackTask(permanent=…)` on failure,
4. extends the lease via re-claim (idempotent on `task_id`) for long-running
   handlers.

That's it. The reference dispatcher in `tape.reactions` is in-process Python.
The Cloud Pub/Sub bridge (`run_pubsub_bridge`) is a dispatcher that does
step 2 = "publish to Pub/Sub topic". A NATS / Kafka bridge would be a few
dozen lines following the same pattern.

---

## 6. Backend specifics

### 6.1 Postgres / AlloyDB

* `tape_journal.global_seq BIGSERIAL`. A `pg_notify('tape_journal', subject)`
  trigger fires on every insert. The server-side `SubscribeBySubject` /
  `SubscribeEvents` handler `LISTEN`s on `tape_journal` per channel, wakes
  on `NOTIFY`, and drains `WHERE global_seq > $cursor AND subject LIKE
  $pattern_to_sql_like` (the matcher translates `/tape/effect/confirmed/**`
  → `/tape/effect/confirmed/%`).
* `tape_tasks` claim uses `FOR UPDATE SKIP LOCKED` for safe concurrent
  dispatchers.

### 6.2 SQLite

* `tape_journal.global_seq INTEGER PRIMARY KEY AUTOINCREMENT`.
* No `LISTEN/NOTIFY`. The stream handler polls every 200 ms; an in-process
  `tokio::sync::Notify` (`bus_tick`) is pulsed on every journal write so
  same-process subscribers wake immediately. SQLite is the dev/single-node
  story; the polling fallback is fine there.

### 6.3 Bigtable

* `global_seq` is allocated via `ReadModifyWrite` on a `meta#global_seq`
  counter row; ordering is preserved by writing it into the journal row
  key (`j#<global_seq:020>`) as well as keeping the per-run `j#<run_id>#<seq>`
  key for back-compat.
* The matcher / `SubscribeBySubject` is stubbed on Bigtable for v1 — the
  documented path is a Bigtable change-stream sidecar (see §12 of
  `tape.md`). The SQL backends are the production path until that lands.

---

## 7. What's in this PR

* schema migrations `0002_event_bus.{sqlite,postgres}.sql`;
* `tape_journal` extensions (global_seq, subject, schema_version, OTel cols);
* `tape_reactions`, `tape_reaction_cursors`, `tape_tasks` tables;
* the new proto messages and RPCs;
* Rust server: subject derivation in every journal-write path, CEL evaluator
  (`cel-interpreter`), in-server matcher, `LISTEN/NOTIFY` on Postgres,
  `Notify`-based same-process wake-ups on SQLite, the new RPC handlers;
* Python SDK: `@tape.on*` decorators, `run_dispatcher`, `run_pubsub_bridge`,
  upgraded `run_outbox_relay`, OTel propagation;
* end-to-end tests covering subject derivation, matching, predicate
  evaluation, dispatcher backpressure, retry/DLQ, and the existing kill-and-
  resume scenario still passing.

What's **deferred** (called out so reviewers don't ask):

* Bigtable change-stream wiring (the SQL backends are the production path
  until then; the proto is forward-compatible).
* Decorator parity in the TS / Go / Java SDKs (they re-generate from the
  updated proto and can call the new RPCs raw; ergonomic decorators are a
  follow-up).
* NATS and Kafka bridges (the dispatcher contract is fixed; implementations
  are a few dozen lines each).
* Server-side debounce queue (debounce is enforced in the dispatcher for
  v1 — the simpler implementation; moving it server-side is a follow-up
  only worthwhile if cross-dispatcher coalescing matters at scale).
