// Package embedded — a standalone, in-process port of the Python
// `tape_adk` SQL ledgers + reactors. No gRPC server, no ADK plug-in.
// Bring your own `*sql.DB` (SQLite via modernc.org/sqlite for pure-Go
// builds, or Postgres via lib/pq / jackc/pgx) and the package gives you
// the same four ledgers, the same fourteen service methods, the same
// four reactor loops, the same decorator semantics — exactly the
// schema the Python sibling writes, so the two implementations can
// read each other's SQLite files.
//
// Mental model: the same one-WAL-many-projections shape Tape ships with
// (decisions, effects, obligations, timers, KV), expressed as four SQL
// tables. The CAS-on-UPDATE primitive ADK's session lock can't express
// lives on each table's lease columns (`dispatch_claimed_by`,
// `dispatch_claim_expires_at_ms` on effects; `claimed_by`,
// `claim_expires_at_ms` on obligations).
//
// SQLite caveat — *load-bearing*: a single shared connection (which
// `?cache=shared` and an in-memory DSN both encourage) cannot serialise
// the BEGIN/UPDATE/COMMIT of two concurrent CAS attempts against the
// same row. The Python reference catches this with an `asyncio.Lock`;
// we catch it with `sync.Mutex` on `TapeSessionService` and gate it on
// the driver name. Postgres needs no such gate — its row-level locking
// makes the conditional UPDATE atomic by construction.
//
// What's NOT in this package:
//
//   - ADK-Go integration. The Go ADK port (github.com/google/adk-go)
//     has its own session-service shape; a follow-up will deliver the
//     `*Plugin` equivalent. Until then, callers wire the four reactor
//     functions into their own scheduler.
//
//   - The live-tail / SubscribeEvents WAL stream. The embedded form
//     models effects, obligations, timers and KV — not the decision
//     journal or the event bus.
package embedded
