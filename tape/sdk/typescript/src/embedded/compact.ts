// The fifth reactor: compaction.
//
// Where the existing reactors move rows FORWARD through the state machine
// (PENDING → CONFIRMED, etc.), this one moves them OUT — the journal isn't
// free, and a long-running agent accumulates terminal rows that have no
// replay value. Same shape as `dispatchOutboxOnce` / `reconcileOnce` /
// `drainObligationsOnce` / `fireDueTimersOnce`: a plain async function
// the caller invokes on a tick.
//
// The mechanism is one composite SQL DELETE per category with the safety
// invariants encoded as WHERE-clause predicates — not application
// pre-checks:
//
//   DELETE FROM tape_effects
//    WHERE status IN ('confirmed','failed')
//      AND ts_ms < :cutoff
//      AND NOT EXISTS (                          ← compensable-window pinning
//        SELECT 1 FROM tape_obligations o
//         WHERE o.session_id  = tape_effects.session_id
//           AND o.effect_key  = tape_effects.idempotency_key
//           AND o.status IN ('pending','committed'))
//
// The `NOT EXISTS` clause IS the pinning mechanism (primitive #5 in the
// roadmap). It runs at SQL level under the same lock all other tape-ts
// writes use, so concurrent compaction and concurrent register_compensation
// can't race a pinned effect into the trash.
//
// Surface:
//
//   * `CompactionPolicy{effectTtlMs, sessionTtlMs, maxPerTick, …}` — the knobs.
//   * `CompactionResult{effectsPruned, obligationsPruned, timersPruned,
//      sessionsArchived}` — what one tick did.
//   * `compactOnce(svc, {policy, nowMs}) -> Promise<CompactionResult>` — the
//      reactor function. Idempotent across ticks; safe to run alongside
//      the other reactors.
//
// Compaction is intentionally LOSSY: once an effect row is pruned, a future
// `beginEffect` with the same idempotency_key won't short-circuit (it'll
// create a new PENDING row). The TTL is the contract: rows older than the
// policy SHOULD be safe to forget.

import {
  EffectStatus,
  ObligationStatus,
  type TapeSessionService,
} from './service.ts';

const _DAY_MS = 24 * 60 * 60 * 1000;

export interface CompactionPolicy {
  /** Prune CONFIRMED/FAILED effects older than this AND not pinned by an
   *  active obligation. Default: 7 days. */
  effectTtlMs: number;
  /** Archive an entire session's tape rows when its latest tape row is
   *  older than this AND there are no active obligations or unfired
   *  timers on it. Default: 30 days. */
  sessionTtlMs: number;
  /** When true, prune COMPENSATED obligations older than `effectTtlMs`.
   *  STUCK is kept regardless — it's the operator-triage signal. */
  archiveTerminalObligations: boolean;
  /** When true, prune fired timers older than `effectTtlMs`. */
  archiveFiredTimers: boolean;
  /** Cap on rows touched per call. The compactor is meant to nibble;
   *  a runaway DELETE is the bug it's designed to prevent. */
  maxPerTick: number;
}

export const DEFAULT_COMPACTION_POLICY: CompactionPolicy = Object.freeze({
  effectTtlMs: 7 * _DAY_MS,
  sessionTtlMs: 30 * _DAY_MS,
  archiveTerminalObligations: true,
  archiveFiredTimers: true,
  maxPerTick: 1000,
});

export function compactionPolicy(opts: Partial<CompactionPolicy> = {}): CompactionPolicy {
  return { ...DEFAULT_COMPACTION_POLICY, ...opts };
}

export interface CompactionResult {
  effectsPruned: number;
  obligationsPruned: number;
  timersPruned: number;
  sessionsArchived: number;
  total: () => number;
}

function newResult(): CompactionResult {
  const r: CompactionResult = {
    effectsPruned: 0,
    obligationsPruned: 0,
    timersPruned: 0,
    sessionsArchived: 0,
    total() {
      return this.effectsPruned + this.obligationsPruned
        + this.timersPruned + this.sessionsArchived;
    },
  };
  return r;
}

// Access to the service's underlying db + write lock without touching
// the public surface. `TapeSessionService` deliberately exposes `db`
// (used by the embedded chaos invariants too). The CAS mutex is private,
// but compaction's per-row DELETEs are serialised at the SQL level on
// SQLite already; we wrap the whole pass in a single transaction so
// session-archival and per-row prune commit atomically (matches Python's
// `async with svc._write_lock(), svc._rollback_on_exception_session()`).

function nowMs(): number { return Date.now(); }

// Ensure DDL is in place before we DELETE — tape tables are created
// lazily on the first mutating service call; for compaction-on-an-
// otherwise-idle service the tables may not exist yet.
async function ensureTables(svc: TapeSessionService): Promise<void> {
  await svc.getEffect({
    appName: '__compact__', userId: '__compact__', sessionId: '__compact__',
    idempotencyKey: '__compact__',
  });
}

interface SessionKey {
  app_name: string;
  user_id: string;
  session_id: string;
}

function findArchivableSessions(
  svc: TapeSessionService,
  sessionCutoff: number,
  limit: number,
): SessionKey[] {
  // Latest effect ts per session. A session with no effects at all has
  // nothing to archive — we only consider sessions with at least one
  // tape row.
  const candidates = svc.db.prepare(`
    SELECT app_name, user_id, session_id, MAX(ts_ms) AS max_ts
      FROM tape_effects
     GROUP BY app_name, user_id, session_id
    HAVING max_ts < ?
     LIMIT ?
  `).all(sessionCutoff, limit) as Array<SessionKey & { max_ts: number }>;

  const out: SessionKey[] = [];
  for (const c of candidates) {
    // Active obligations (PENDING/COMMITTED) keep the session alive.
    const activeOb = svc.db.prepare(`
      SELECT COUNT(*) AS n FROM tape_obligations
       WHERE app_name = ? AND user_id = ? AND session_id = ?
         AND status IN (?, ?)
    `).get(
      c.app_name, c.user_id, c.session_id,
      ObligationStatus.PENDING, ObligationStatus.COMMITTED,
    ) as { n: number };
    if (Number(activeOb?.n ?? 0) > 0) continue;
    // STUCK obligations keep the session alive (triage).
    const stuckOb = svc.db.prepare(`
      SELECT COUNT(*) AS n FROM tape_obligations
       WHERE app_name = ? AND user_id = ? AND session_id = ?
         AND status = ?
    `).get(
      c.app_name, c.user_id, c.session_id, ObligationStatus.STUCK,
    ) as { n: number };
    if (Number(stuckOb?.n ?? 0) > 0) continue;
    // Any unfired timer (past or future) keeps the session alive.
    const liveTimers = svc.db.prepare(`
      SELECT COUNT(*) AS n FROM tape_timers
       WHERE app_name = ? AND user_id = ? AND session_id = ?
         AND fired = 0
    `).get(
      c.app_name, c.user_id, c.session_id,
    ) as { n: number };
    if (Number(liveTimers?.n ?? 0) > 0) continue;
    out.push({
      app_name: c.app_name, user_id: c.user_id, session_id: c.session_id,
    });
  }
  return out;
}

/**
 * One pass of the compactor. Four atomic DELETEs (session-archival,
 * terminal obligations, fired timers, terminal-and-unpinned effects).
 *
 * Order matters: session-archival runs FIRST. It's the superset
 * operation — when a whole session qualifies (all rows old + no active
 * obligations + no unfired timers), one round of DELETEs wipes its
 * three tape_* tables. Per-row pruning in steps 2-4 then handles
 * surviving rows in still-active sessions. The Python file's comments
 * call this out — preserve the reason: an interleaved tick that
 * inserts a new obligation between the obligation prune and the
 * effect prune cannot re-pin a freshly-deleted effect.
 */
export async function compactOnce(
  svc: TapeSessionService,
  opts: { policy?: CompactionPolicy; nowMs?: number } = {},
): Promise<CompactionResult> {
  const policy = opts.policy ?? DEFAULT_COMPACTION_POLICY;
  await ensureTables(svc);
  const now = opts.nowMs ?? nowMs();
  const effectCutoff = now - policy.effectTtlMs;
  const sessionCutoff = now - policy.sessionTtlMs;
  const result = newResult();

  // One transaction so all four steps commit atomically (Python uses
  // `_write_lock() + _rollback_on_exception_session()` for the same
  // guarantee).
  const txn = svc.db.transaction(() => {
    // 1) Session-level archival FIRST.
    const sessions = findArchivableSessions(svc, sessionCutoff, policy.maxPerTick);
    for (const s of sessions) {
      const tables: Array<['effects' | 'obligations' | 'timers', string]> = [
        ['effects', 'tape_effects'],
        ['obligations', 'tape_obligations'],
        ['timers', 'tape_timers'],
      ];
      for (const [kind, tbl] of tables) {
        const r = svc.db.prepare(`
          DELETE FROM ${tbl}
           WHERE app_name = ? AND user_id = ? AND session_id = ?
        `).run(s.app_name, s.user_id, s.session_id);
        const count = Number(r.changes ?? 0);
        if (kind === 'effects') result.effectsPruned += count;
        else if (kind === 'obligations') result.obligationsPruned += count;
        else result.timersPruned += count;
      }
      result.sessionsArchived += 1;
    }

    // 2) Terminal obligations older than the effect TTL — keep STUCK.
    if (policy.archiveTerminalObligations) {
      const r = svc.db.prepare(`
        DELETE FROM tape_obligations
         WHERE status = ?
           AND ts_ms < ?
      `).run(ObligationStatus.COMPENSATED, effectCutoff);
      result.obligationsPruned += Number(r.changes ?? 0);
    }

    // 3) Fired timers older than the effect TTL.
    if (policy.archiveFiredTimers) {
      const r = svc.db.prepare(`
        DELETE FROM tape_timers
         WHERE fired = 1
           AND created_at_ms < ?
      `).run(effectCutoff);
      result.timersPruned += Number(r.changes ?? 0);
    }

    // 4) Effects in a terminal status, old enough, with NO active
    //    obligation referencing them — the compensable-window pinning
    //    invariant, encoded as a NOT EXISTS subquery rather than an
    //    application-level loop. THIS is the load-bearing predicate
    //    that keeps a row whose compensator still needs the
    //    external_ref. Copy the SQL shape exactly from Python.
    const r = svc.db.prepare(`
      DELETE FROM tape_effects
       WHERE status IN (?, ?)
         AND ts_ms < ?
         AND NOT EXISTS (
           SELECT 1 FROM tape_obligations o
            WHERE o.session_id = tape_effects.session_id
              AND o.effect_key = tape_effects.idempotency_key
              AND o.status IN (?, ?)
         )
    `).run(
      EffectStatus.CONFIRMED, EffectStatus.FAILED, effectCutoff,
      ObligationStatus.PENDING, ObligationStatus.COMMITTED,
    );
    result.effectsPruned += Number(r.changes ?? 0);
  });
  txn();

  return result;
}
