// `TapeSessionService` — embedded SQL parity with `tape_adk/service.py`.
//
// 14 async methods over the four tables in `schema.ts`. The CAS primitives
// (`claimEffectDispatch`, `claimObligation`) use the same `UPDATE … WHERE`
// rowcount==1 pattern with the eligibility predicate inline. For SQLite,
// we wrap CAS in a small in-process Mutex to avoid the same concurrent-on-
// -shared-connection bug Phase 2 caught in Python (a Mutex per service
// instance; gated to the SQLite dialect).

import {
  createAllTables,
  type EffectRow,
  type EmbeddedDb,
  type ObligationRow,
  type TimerRow,
  type ValueRow,
} from './schema.ts';

// ── status enums (string-typed; mirror proto enums) ────────────────────────

export const EffectStatus = {
  PENDING: 'pending',
  CONFIRMED: 'confirmed',
  FAILED: 'failed',
  UNKNOWN: 'unknown',
} as const;
export type EffectStatusT = (typeof EffectStatus)[keyof typeof EffectStatus];

export const EffectSemantics = {
  IDEMPOTENT: 'idempotent',
  NON_IDEMPOTENT: 'non_idempotent',
  OBSERVE_ONLY: 'observe_only',
} as const;
export type EffectSemanticsT = (typeof EffectSemantics)[keyof typeof EffectSemantics];

export const EffectDispatchMode = {
  INLINE: 'inline',
  OUTBOX: 'outbox',
} as const;
export type EffectDispatchModeT = (typeof EffectDispatchMode)[keyof typeof EffectDispatchMode];

export const EffectResolution = {
  CONFIRMED: 'confirmed',
  FAILED: 'failed',
  ABSENT: 'absent',
  DUPLICATE: 'duplicate',
  STUCK: 'stuck',
} as const;
export type EffectResolutionT = (typeof EffectResolution)[keyof typeof EffectResolution];

export const ObligationStatus = {
  PENDING: 'pending',
  COMMITTED: 'committed',
  COMPENSATED: 'compensated',
  STUCK: 'stuck',
} as const;
export type ObligationStatusT = (typeof ObligationStatus)[keyof typeof ObligationStatus];

// ── records (what callers get back) ────────────────────────────────────────

export interface EffectRecord {
  appName: string;
  userId: string;
  sessionId: string;
  idempotencyKey: string;
  invocationId: string;
  decisionIndex: number;
  toolName: string;
  callIndex: number;
  status: string;
  semantics: string;
  dispatchMode: string;
  businessKey: string | null;
  connector: string | null;
  externalRef: string | null;
  dispatchAttempts: number;
  nextDispatchAtMs: number;
  dispatchClaimedBy: string | null;
  dispatchClaimExpiresAtMs: number;
  lastDispatchError: unknown;
  requestJson: unknown;
  responseJson: unknown;
  errorJson: unknown;
  tsMs: number;
}

export interface ObligationRecord {
  seq: number;
  appName: string;
  userId: string;
  sessionId: string;
  invocationId: string;
  effectKey: string;
  kind: string;
  payloadJson: unknown;
  status: string;
  attempts: number;
  maxAttempts: number;
  nextAttemptAtMs: number;
  lastError: unknown;
  claimedBy: string | null;
  claimExpiresAtMs: number;
  compensatorRef: string | null;
  resultJson: unknown;
  tsMs: number;
}

export interface TimerRecord {
  appName: string;
  userId: string;
  sessionId: string;
  timerId: string;
  fireAtMs: number;
  kind: string;
  payloadJson: unknown;
  fired: boolean;
  createdAtMs: number;
}

export interface ValueRecord {
  namespace: string;
  key: string;
  valueJson: unknown;
  version: number;
  tsMs: number;
  writer: string | null;
  deleted: boolean;
}

// ── row → record converters ────────────────────────────────────────────────

function parseJson(s: string | null): unknown {
  if (s === null || s === undefined) return null;
  try { return JSON.parse(s); } catch { return s; }
}

function stringifyJson(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  if (typeof v === 'string') return v;
  return JSON.stringify(v);
}

function effectFromRow(r: EffectRow): EffectRecord {
  return {
    appName: r.app_name,
    userId: r.user_id,
    sessionId: r.session_id,
    idempotencyKey: r.idempotency_key,
    invocationId: r.invocation_id,
    decisionIndex: Number(r.decision_index),
    toolName: r.tool_name,
    callIndex: Number(r.call_index),
    status: r.status,
    semantics: r.semantics,
    dispatchMode: r.dispatch_mode,
    businessKey: r.business_key,
    connector: r.connector,
    externalRef: r.external_ref,
    dispatchAttempts: Number(r.dispatch_attempts),
    nextDispatchAtMs: Number(r.next_dispatch_at_ms),
    dispatchClaimedBy: r.dispatch_claimed_by,
    dispatchClaimExpiresAtMs: Number(r.dispatch_claim_expires_at_ms),
    lastDispatchError: parseJson(r.last_dispatch_error),
    requestJson: parseJson(r.request_json),
    responseJson: parseJson(r.response_json),
    errorJson: parseJson(r.error_json),
    tsMs: Number(r.ts_ms),
  };
}

function obligationFromRow(r: ObligationRow): ObligationRecord {
  return {
    seq: Number(r.seq),
    appName: r.app_name,
    userId: r.user_id,
    sessionId: r.session_id,
    invocationId: r.invocation_id,
    effectKey: r.effect_key,
    kind: r.kind,
    payloadJson: parseJson(r.payload_json),
    status: r.status,
    attempts: Number(r.attempts),
    maxAttempts: Number(r.max_attempts),
    nextAttemptAtMs: Number(r.next_attempt_at_ms),
    lastError: parseJson(r.last_error),
    claimedBy: r.claimed_by,
    claimExpiresAtMs: Number(r.claim_expires_at_ms),
    compensatorRef: r.compensator_ref,
    resultJson: parseJson(r.result_json),
    tsMs: Number(r.ts_ms),
  };
}

function timerFromRow(r: TimerRow): TimerRecord {
  return {
    appName: r.app_name,
    userId: r.user_id,
    sessionId: r.session_id,
    timerId: r.timer_id,
    fireAtMs: Number(r.fire_at_ms),
    kind: r.kind,
    payloadJson: parseJson(r.payload_json),
    fired: Boolean(r.fired),
    createdAtMs: Number(r.created_at_ms),
  };
}

function valueFromRow(r: ValueRow): ValueRecord {
  return {
    namespace: r.namespace,
    key: r.key,
    valueJson: parseJson(r.value_json),
    version: Number(r.version),
    tsMs: Number(r.ts_ms),
    writer: r.writer,
    deleted: Boolean(r.deleted),
  };
}

function nowMs(): number { return Date.now(); }

// ── a tiny async mutex (no deps) ──────────────────────────────────────────
//
// The TS dialect of `asyncio.Lock`. Promise-chain based: each acquire()
// returns a release function. Used to serialize CAS attempts on SQLite,
// where `better-sqlite3`'s synchronous API + the shared connection bug
// means concurrent CAS attempts can interleave their BEGIN/UPDATE/COMMIT
// in surprising ways under contention from event-loop scheduling.

class Mutex {
  private tail: Promise<void> = Promise.resolve();

  async acquire(): Promise<() => void> {
    let release!: () => void;
    const next = new Promise<void>((res) => { release = res; });
    const wait = this.tail;
    this.tail = next;
    await wait;
    return release;
  }
}

// ── the service ─────────────────────────────────────────────────────────────

/**
 * `TapeSessionService` — effect ledger, obligation ledger, server-side
 * timer registry, and reactive KV — backed by an `EmbeddedDb`.
 *
 * 14 async methods. Same semantics as `tape_adk.service.TapeSessionService`.
 * No host agent framework integration — this is the standalone Node.js
 * parity. (ADK-TypeScript does not exist.)
 */
export class TapeSessionService {
  readonly db: EmbeddedDb;
  private readonly casLock: Mutex;
  private prepared = false;

  constructor(db: EmbeddedDb) {
    this.db = db;
    this.casLock = new Mutex();
  }

  private async ensureTables(): Promise<void> {
    if (this.prepared) return;
    createAllTables(this.db);
    this.prepared = true;
  }

  private async withCasLock<T>(fn: () => T | Promise<T>): Promise<T> {
    if (this.db.dialect !== 'sqlite') {
      // On Postgres the per-row SQL-level locking does this for us.
      return await fn();
    }
    const release = await this.casLock.acquire();
    try {
      return await fn();
    } finally {
      release();
    }
  }

  // ── effect ledger ────────────────────────────────────────────────────

  /**
   * Idempotent. If an effect with this idempotency_key already exists,
   * returns the existing record (the replay-time short-circuit).
   * Otherwise creates a fresh PENDING row.
   *
   * Server-side safety: refuses NON_IDEMPOTENT + INLINE — that combination
   * is the bug the whole project exists to prevent. Also refuses OUTBOX
   * without a connector name.
   */
  async beginEffect(args: {
    appName: string;
    userId: string;
    sessionId: string;
    invocationId: string;
    decisionIndex: number;
    toolName: string;
    callIndex?: number;
    requestJson?: unknown;
    customKey?: string;
    semantics?: string;
    dispatchMode?: string;
    businessKey?: string | null;
    connector?: string | null;
  }): Promise<EffectRecord> {
    const semantics = args.semantics ?? EffectSemantics.IDEMPOTENT;
    const dispatchMode = args.dispatchMode ?? EffectDispatchMode.INLINE;
    const callIndex = args.callIndex ?? 0;

    if (semantics === EffectSemantics.NON_IDEMPOTENT
        && dispatchMode === EffectDispatchMode.INLINE) {
      throw new Error(
        'beginEffect: NON_IDEMPOTENT effects must use OUTBOX dispatch');
    }
    if (dispatchMode === EffectDispatchMode.OUTBOX && !args.connector) {
      throw new Error('beginEffect: OUTBOX dispatch requires a `connector` name');
    }

    const key = args.customKey
      || `${args.invocationId}/decision-${args.decisionIndex}/${args.toolName}/${callIndex}`;

    await this.ensureTables();

    // Read first — idempotent on replay.
    const existing = this.db.prepare(`
      SELECT * FROM tape_effects
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
    `).get(args.appName, args.userId, args.sessionId, key) as EffectRow | undefined;
    if (existing) return effectFromRow(existing);

    const now = nowMs();
    try {
      this.db.prepare(`
        INSERT INTO tape_effects (
          app_name, user_id, session_id, idempotency_key,
          invocation_id, decision_index, tool_name, call_index,
          status, semantics, dispatch_mode,
          business_key, connector, external_ref,
          dispatch_attempts, next_dispatch_at_ms,
          dispatch_claimed_by, dispatch_claim_expires_at_ms,
          last_dispatch_error,
          request_json, response_json, error_json, ts_ms
        ) VALUES (
          ?, ?, ?, ?,
          ?, ?, ?, ?,
          ?, ?, ?,
          ?, ?, NULL,
          0, 0,
          NULL, 0,
          NULL,
          ?, NULL, NULL, ?
        )
      `).run(
        args.appName, args.userId, args.sessionId, key,
        args.invocationId, args.decisionIndex, args.toolName, callIndex,
        EffectStatus.PENDING, semantics, dispatchMode,
        args.businessKey ?? null, args.connector ?? null,
        stringifyJson(args.requestJson), now,
      );
    } catch (ex) {
      // Most likely: (connector, business_key) UNIQUE clash — another run
      // already journaled this logical operation.
      const msg = ex instanceof Error ? ex.message : String(ex);
      if (/UNIQUE|unique/.test(msg)) {
        throw new Error(
          `beginEffect: business_key already exists for connector=${JSON.stringify(args.connector)}: ${msg}`);
      }
      throw ex;
    }
    const row = this.db.prepare(`
      SELECT * FROM tape_effects
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
    `).get(args.appName, args.userId, args.sessionId, key) as EffectRow;
    return effectFromRow(row);
  }

  /** Flip an effect's terminal status. Idempotent — if the effect is
   *  already CONFIRMED/FAILED/UNKNOWN, this is a no-op that returns the
   *  current row. */
  async completeEffect(args: {
    appName: string;
    userId: string;
    sessionId: string;
    idempotencyKey: string;
    status: string;
    responseJson?: unknown;
    errorJson?: unknown;
  }): Promise<EffectRecord | null> {
    if (![EffectStatus.CONFIRMED, EffectStatus.FAILED, EffectStatus.UNKNOWN]
        .includes(args.status as EffectStatusT)) {
      throw new Error(`completeEffect: invalid status ${JSON.stringify(args.status)}`);
    }
    await this.ensureTables();
    const row = this.db.prepare(`
      SELECT * FROM tape_effects
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
    `).get(args.appName, args.userId, args.sessionId, args.idempotencyKey) as EffectRow | undefined;
    if (!row) return null;
    if (row.status !== EffectStatus.PENDING) {
      return effectFromRow(row);
    }
    const now = nowMs();
    this.db.prepare(`
      UPDATE tape_effects
      SET status = ?, response_json = ?, error_json = ?,
          dispatch_claimed_by = NULL, dispatch_claim_expires_at_ms = 0,
          ts_ms = ?
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
    `).run(
      args.status, stringifyJson(args.responseJson), stringifyJson(args.errorJson),
      now,
      args.appName, args.userId, args.sessionId, args.idempotencyKey,
    );
    const after = this.db.prepare(`
      SELECT * FROM tape_effects
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
    `).get(args.appName, args.userId, args.sessionId, args.idempotencyKey) as EffectRow;
    return effectFromRow(after);
  }

  async getEffect(args: {
    appName: string;
    userId: string;
    sessionId: string;
    idempotencyKey: string;
  }): Promise<EffectRecord | null> {
    await this.ensureTables();
    const row = this.db.prepare(`
      SELECT * FROM tape_effects
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
    `).get(args.appName, args.userId, args.sessionId, args.idempotencyKey) as EffectRow | undefined;
    return row ? effectFromRow(row) : null;
  }

  // ── outbox: dispatch claim (CAS) + attempt recording ──────────────────

  /**
   * Atomic CAS lease on the dispatch slot.
   *
   * The predicate: row is PENDING + OUTBOX + dispatch-eligible
   * (next_dispatch_at_ms <= now) + the existing lease (if any) has expired.
   * Implementation is one UPDATE with the predicate inline and rowcount==1
   * means we won.
   */
  async claimEffectDispatch(args: {
    appName: string;
    userId: string;
    sessionId: string;
    idempotencyKey: string;
    claimer: string;
    leaseTtlMs?: number;
    nowMs?: number;
  }): Promise<[boolean, EffectRecord | null]> {
    await this.ensureTables();
    const now = args.nowMs || nowMs();
    const expires = now + (args.leaseTtlMs ?? 60_000);

    return await this.withCasLock(() => {
      const res = this.db.prepare(`
        UPDATE tape_effects
        SET dispatch_claimed_by = ?, dispatch_claim_expires_at_ms = ?
        WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
          AND status = ?
          AND dispatch_mode = ?
          AND next_dispatch_at_ms <= ?
          AND (dispatch_claimed_by IS NULL
               OR dispatch_claimed_by = ''
               OR dispatch_claim_expires_at_ms <= ?)
      `).run(
        args.claimer, expires,
        args.appName, args.userId, args.sessionId, args.idempotencyKey,
        EffectStatus.PENDING,
        EffectDispatchMode.OUTBOX,
        now,
        now,
      );
      const acquired = res.changes === 1;
      const row = this.db.prepare(`
        SELECT * FROM tape_effects
        WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
      `).get(args.appName, args.userId, args.sessionId, args.idempotencyKey) as EffectRow | undefined;
      return [acquired, row ? effectFromRow(row) : null];
    });
  }

  /**
   * Report a failed dispatch. `nextDispatchAtMs = 0` is the load-bearing
   * case: PENDING → UNKNOWN so the reconciler takes over. Positive value
   * reschedules a retry; effect stays PENDING.
   */
  async recordDispatchAttempt(args: {
    appName: string;
    userId: string;
    sessionId: string;
    idempotencyKey: string;
    error: string;
    nextDispatchAtMs: number;
  }): Promise<EffectRecord | null> {
    await this.ensureTables();
    const row = this.db.prepare(`
      SELECT * FROM tape_effects
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
    `).get(args.appName, args.userId, args.sessionId, args.idempotencyKey) as EffectRow | undefined;
    if (!row) return null;

    const attempts = (Number(row.dispatch_attempts) || 0) + 1;
    const now = nowMs();
    let newStatus: string = row.status;
    let nextAt = args.nextDispatchAtMs;
    if (args.nextDispatchAtMs <= 0) {
      newStatus = EffectStatus.UNKNOWN;
      nextAt = 0;
    }
    this.db.prepare(`
      UPDATE tape_effects
      SET dispatch_attempts = ?,
          last_dispatch_error = ?,
          dispatch_claimed_by = NULL,
          dispatch_claim_expires_at_ms = 0,
          status = ?,
          next_dispatch_at_ms = ?,
          ts_ms = ?
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
    `).run(
      attempts, stringifyJson(args.error), newStatus, nextAt, now,
      args.appName, args.userId, args.sessionId, args.idempotencyKey,
    );
    const after = this.db.prepare(`
      SELECT * FROM tape_effects
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
    `).get(args.appName, args.userId, args.sessionId, args.idempotencyKey) as EffectRow;
    return effectFromRow(after);
  }

  /**
   * The reconciler's write path. Maps `EffectResolution` → `EffectStatus`
   * and, on DUPLICATE, atomically registers a compensation obligation if
   * `compensateOnDuplicateKind` is set.
   */
  async recordExternalObservation(args: {
    appName: string;
    userId: string;
    sessionId: string;
    idempotencyKey: string;
    resolution: string;
    externalRef?: string;
    responseJson?: unknown;
    errorJson?: unknown;
    compensateOnDuplicateKind?: string;
  }): Promise<EffectRecord | null> {
    await this.ensureTables();
    const row = this.db.prepare(`
      SELECT * FROM tape_effects
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
    `).get(args.appName, args.userId, args.sessionId, args.idempotencyKey) as EffectRow | undefined;
    if (!row) return null;

    const now = nowMs();
    let newStatus = row.status;
    let externalRef = row.external_ref;
    let responseJson = row.response_json;
    let errorJson = row.error_json;
    let shouldInsertObligation = false;

    if (args.resolution === EffectResolution.CONFIRMED) {
      newStatus = EffectStatus.CONFIRMED;
      externalRef = args.externalRef || row.external_ref;
      responseJson = stringifyJson(args.responseJson);
    } else if (args.resolution === EffectResolution.FAILED) {
      newStatus = EffectStatus.FAILED;
      errorJson = stringifyJson(args.errorJson);
    } else if (args.resolution === EffectResolution.ABSENT) {
      if (row.semantics === EffectSemantics.NON_IDEMPOTENT) {
        newStatus = EffectStatus.UNKNOWN;
      }
      if (args.errorJson !== undefined && args.errorJson !== null) {
        errorJson = stringifyJson(args.errorJson);
      }
    } else if (args.resolution === EffectResolution.DUPLICATE) {
      newStatus = EffectStatus.CONFIRMED;
      externalRef = args.externalRef || row.external_ref;
      responseJson = stringifyJson(args.responseJson);
      if (args.compensateOnDuplicateKind) {
        shouldInsertObligation = true;
      }
    } else if (args.resolution === EffectResolution.STUCK) {
      newStatus = EffectStatus.FAILED;
      errorJson = stringifyJson(args.errorJson ?? {
        resolution: 'stuck',
        detail: "reconciler couldn't resolve",
      });
    } else {
      throw new Error(`unknown resolution: ${JSON.stringify(args.resolution)}`);
    }

    // Run the UPDATE + the optional INSERT in a single SQLite transaction —
    // this mirrors the Python "same SQLAlchemy session" guarantee.
    const txn = this.db.transaction(() => {
      this.db.prepare(`
        UPDATE tape_effects
        SET status = ?, external_ref = ?, response_json = ?, error_json = ?, ts_ms = ?
        WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
      `).run(
        newStatus, externalRef, responseJson, errorJson, now,
        args.appName, args.userId, args.sessionId, args.idempotencyKey,
      );
      if (shouldInsertObligation) {
        this.db.prepare(`
          INSERT INTO tape_obligations (
            app_name, user_id, session_id, invocation_id,
            effect_key, kind, payload_json,
            status, attempts, max_attempts, next_attempt_at_ms,
            last_error, claimed_by, claim_expires_at_ms,
            compensator_ref, result_json, ts_ms
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 5, ?, NULL, NULL, 0, NULL, NULL, ?)
          ON CONFLICT DO NOTHING
        `).run(
          args.appName, args.userId, args.sessionId, row.invocation_id,
          row.idempotency_key, args.compensateOnDuplicateKind,
          stringifyJson({
            external_ref: args.externalRef || row.external_ref,
            reason: 'duplicate observed by reconciler',
          }),
          ObligationStatus.PENDING, now, now,
        );
      }
    });
    txn();
    const after = this.db.prepare(`
      SELECT * FROM tape_effects
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
    `).get(args.appName, args.userId, args.sessionId, args.idempotencyKey) as EffectRow;
    return effectFromRow(after);
  }

  // ── reconciler / outbox queues ────────────────────────────────────────

  /** The reconciler's hot set: PENDING (older than `olderThanMs`) plus
   *  UNKNOWN. Cross-session. */
  async listPendingEffects(args: {
    olderThanMs?: number;
    includePending?: boolean;
    includeUnknown?: boolean;
    limit?: number;
  } = {}): Promise<EffectRecord[]> {
    await this.ensureTables();
    const includePending = args.includePending ?? true;
    const includeUnknown = args.includeUnknown ?? true;
    const olderThanMs = args.olderThanMs ?? 0;
    const limit = args.limit ?? 200;

    const statuses: string[] = [];
    if (includePending) statuses.push(EffectStatus.PENDING);
    if (includeUnknown) statuses.push(EffectStatus.UNKNOWN);
    if (statuses.length === 0) return [];

    const placeholders = statuses.map(() => '?').join(',');
    let sql = `SELECT * FROM tape_effects WHERE status IN (${placeholders})`;
    const params: unknown[] = [...statuses];

    if (olderThanMs > 0 && includePending && !includeUnknown) {
      sql += ` AND ts_ms < ?`;
      params.push(olderThanMs);
    } else if (olderThanMs > 0 && includePending) {
      sql += ` AND (status = ? OR (status = ? AND ts_ms < ?))`;
      params.push(EffectStatus.UNKNOWN, EffectStatus.PENDING, olderThanMs);
    }
    sql += ` ORDER BY ts_ms LIMIT ?`;
    params.push(limit);

    const rows = this.db.prepare(sql).all(...params) as EffectRow[];
    return rows.map(effectFromRow);
  }

  /** The outbox dispatcher's hot set: PENDING + OUTBOX +
   *  next_dispatch_at_ms <= now + (lease free or expired). */
  async listEffectsToDispatch(args: {
    nowMs?: number;
    connector?: string;
    limit?: number;
  } = {}): Promise<EffectRecord[]> {
    await this.ensureTables();
    const now = args.nowMs || nowMs();
    const limit = args.limit ?? 200;
    let sql = `
      SELECT * FROM tape_effects
      WHERE status = ?
        AND dispatch_mode = ?
        AND next_dispatch_at_ms <= ?
        AND (dispatch_claimed_by IS NULL
             OR dispatch_claimed_by = ''
             OR dispatch_claim_expires_at_ms <= ?)
    `;
    const params: unknown[] = [
      EffectStatus.PENDING, EffectDispatchMode.OUTBOX, now, now,
    ];
    if (args.connector) {
      sql += ` AND connector = ?`;
      params.push(args.connector);
    }
    sql += ` ORDER BY ts_ms LIMIT ?`;
    params.push(limit);
    const rows = this.db.prepare(sql).all(...params) as EffectRow[];
    return rows.map(effectFromRow);
  }

  // ── obligation ledger ─────────────────────────────────────────────────

  /** Idempotent on (session, effect_key, kind). */
  async registerCompensation(args: {
    appName: string;
    userId: string;
    sessionId: string;
    invocationId?: string;
    effectKey: string;
    kind: string;
    payloadJson?: unknown;
    compensatorRef?: string;
    maxAttempts?: number;
  }): Promise<ObligationRecord> {
    await this.ensureTables();
    const existing = this.db.prepare(`
      SELECT * FROM tape_obligations
      WHERE app_name = ? AND user_id = ? AND session_id = ?
        AND effect_key = ? AND kind = ?
    `).get(args.appName, args.userId, args.sessionId, args.effectKey, args.kind) as ObligationRow | undefined;
    if (existing) return obligationFromRow(existing);

    const now = nowMs();
    const max = args.maxAttempts || 5;
    const res = this.db.prepare(`
      INSERT INTO tape_obligations (
        app_name, user_id, session_id, invocation_id,
        effect_key, kind, payload_json,
        status, attempts, max_attempts, next_attempt_at_ms,
        last_error, claimed_by, claim_expires_at_ms,
        compensator_ref, result_json, ts_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, NULL, NULL, 0, ?, NULL, ?)
    `).run(
      args.appName, args.userId, args.sessionId, args.invocationId ?? '',
      args.effectKey, args.kind, stringifyJson(args.payloadJson),
      ObligationStatus.PENDING, max, now,
      args.compensatorRef ?? null, now,
    );
    const row = this.db.prepare(`SELECT * FROM tape_obligations WHERE seq = ?`)
      .get(res.lastInsertRowid) as ObligationRow;
    return obligationFromRow(row);
  }

  /** Per-session, LIFO (seq DESC). */
  async listObligations(args: {
    appName: string;
    userId: string;
    sessionId: string;
    onlyUnresolved?: boolean;
    statusFilter?: string;
  }): Promise<ObligationRecord[]> {
    await this.ensureTables();
    const onlyUnresolved = args.onlyUnresolved ?? true;
    let sql = `SELECT * FROM tape_obligations
               WHERE app_name = ? AND user_id = ? AND session_id = ?`;
    const params: unknown[] = [args.appName, args.userId, args.sessionId];
    if (args.statusFilter) {
      sql += ` AND status = ?`;
      params.push(args.statusFilter);
    } else if (onlyUnresolved) {
      sql += ` AND status IN (?, ?)`;
      params.push(ObligationStatus.PENDING, ObligationStatus.COMMITTED);
    }
    sql += ` ORDER BY seq DESC`;
    const rows = this.db.prepare(sql).all(...params) as ObligationRow[];
    return rows.map(obligationFromRow);
  }

  /**
   * Cross-session drainer feed. PENDING-ready + COMMITTED-expired by
   * default; flip `includeStuck` for triage.
   */
  async listUnresolvedObligations(args: {
    nowMs?: number;
    limit?: number;
    includePending?: boolean;
    includeStuck?: boolean;
    includeCommittedExpired?: boolean;
  } = {}): Promise<ObligationRecord[]> {
    await this.ensureTables();
    const now = args.nowMs || nowMs();
    const limit = args.limit ?? 500;
    const includePending = args.includePending ?? true;
    const includeStuck = args.includeStuck ?? false;
    const includeCommittedExpired = args.includeCommittedExpired ?? true;

    const conds: string[] = [];
    const params: unknown[] = [];
    if (includePending) {
      conds.push(`(status = ? AND next_attempt_at_ms <= ?)`);
      params.push(ObligationStatus.PENDING, now);
    }
    if (includeCommittedExpired) {
      conds.push(`(status = ? AND claim_expires_at_ms <= ?)`);
      params.push(ObligationStatus.COMMITTED, now);
    }
    if (includeStuck) {
      conds.push(`status = ?`);
      params.push(ObligationStatus.STUCK);
    }
    if (conds.length === 0) return [];
    const sql = `SELECT * FROM tape_obligations
                 WHERE ${conds.join(' OR ')}
                 ORDER BY seq DESC
                 LIMIT ?`;
    params.push(limit);
    const rows = this.db.prepare(sql).all(...params) as ObligationRow[];
    return rows.map(obligationFromRow);
  }

  /** Atomic CAS — single winner, same shape as `claimEffectDispatch`.
   *  Also reclaims COMMITTED rows whose claim_expires_at_ms <= now. */
  async claimObligation(args: {
    seq: number;
    claimer: string;
    leaseTtlMs?: number;
    nowMs?: number;
  }): Promise<[boolean, ObligationRecord | null]> {
    await this.ensureTables();
    const now = args.nowMs || nowMs();
    const expires = now + (args.leaseTtlMs ?? 60_000);
    return await this.withCasLock(() => {
      const res = this.db.prepare(`
        UPDATE tape_obligations
        SET status = ?, claimed_by = ?, claim_expires_at_ms = ?
        WHERE seq = ?
          AND (
            (status = ? AND next_attempt_at_ms <= ?)
            OR (status = ? AND claim_expires_at_ms <= ?)
          )
      `).run(
        ObligationStatus.COMMITTED, args.claimer, expires,
        args.seq,
        ObligationStatus.PENDING, now,
        ObligationStatus.COMMITTED, now,
      );
      const acquired = res.changes === 1;
      const row = this.db.prepare(`SELECT * FROM tape_obligations WHERE seq = ?`)
        .get(args.seq) as ObligationRow | undefined;
      return [acquired, row ? obligationFromRow(row) : null];
    });
  }

  /** Report a failed compensation attempt. `nextAttemptAtMs=0` forces
   *  STUCK (terminal-now). Otherwise: bump attempts; if attempts >=
   *  maxAttempts mark STUCK; else reschedule PENDING. */
  async recordObligationAttempt(args: {
    seq: number;
    error: string;
    nextAttemptAtMs: number;
  }): Promise<ObligationRecord | null> {
    await this.ensureTables();
    const row = this.db.prepare(`SELECT * FROM tape_obligations WHERE seq = ?`)
      .get(args.seq) as ObligationRow | undefined;
    if (!row) return null;

    const attempts = (Number(row.attempts) || 0) + 1;
    const now = nowMs();
    let status: string = ObligationStatus.PENDING;
    let nextAt = args.nextAttemptAtMs;
    if (args.nextAttemptAtMs <= 0 || attempts >= Number(row.max_attempts)) {
      status = ObligationStatus.STUCK;
      nextAt = 0;
    }
    this.db.prepare(`
      UPDATE tape_obligations
      SET attempts = ?, last_error = ?, claimed_by = NULL,
          claim_expires_at_ms = 0, status = ?,
          next_attempt_at_ms = ?, ts_ms = ?
      WHERE seq = ?
    `).run(
      attempts, stringifyJson(args.error), status, nextAt, now,
      args.seq,
    );
    const after = this.db.prepare(`SELECT * FROM tape_obligations WHERE seq = ?`)
      .get(args.seq) as ObligationRow;
    return obligationFromRow(after);
  }

  /** Terminal transition: COMPENSATED (success) or STUCK (failure). */
  async resolveObligation(args: {
    seq: number;
    status: string;
    resultJson?: unknown;
  }): Promise<ObligationRecord | null> {
    if (![ObligationStatus.COMPENSATED, ObligationStatus.STUCK]
        .includes(args.status as ObligationStatusT)) {
      throw new Error(
        `resolveObligation: status must be COMPENSATED or STUCK, got ${JSON.stringify(args.status)}`);
    }
    await this.ensureTables();
    const row = this.db.prepare(`SELECT * FROM tape_obligations WHERE seq = ?`)
      .get(args.seq) as ObligationRow | undefined;
    if (!row) return null;
    const now = nowMs();
    this.db.prepare(`
      UPDATE tape_obligations
      SET status = ?, result_json = ?, claimed_by = NULL,
          claim_expires_at_ms = 0, ts_ms = ?
      WHERE seq = ?
    `).run(args.status, stringifyJson(args.resultJson), now, args.seq);
    const after = this.db.prepare(`SELECT * FROM tape_obligations WHERE seq = ?`)
      .get(args.seq) as ObligationRow;
    return obligationFromRow(after);
  }

  // ── timers ────────────────────────────────────────────────────────────

  /** Idempotent on (session, timer_id) — a second `setTimer` with the same
   *  id returns the existing record. */
  async setTimer(args: {
    appName: string;
    userId: string;
    sessionId: string;
    timerId: string;
    fireAtMs: number;
    kind: string;
    payloadJson?: unknown;
  }): Promise<TimerRecord> {
    await this.ensureTables();
    const existing = this.db.prepare(`
      SELECT * FROM tape_timers
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND timer_id = ?
    `).get(args.appName, args.userId, args.sessionId, args.timerId) as TimerRow | undefined;
    if (existing) return timerFromRow(existing);

    const now = nowMs();
    this.db.prepare(`
      INSERT INTO tape_timers (
        app_name, user_id, session_id, timer_id,
        fire_at_ms, kind, payload_json, fired, created_at_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
    `).run(
      args.appName, args.userId, args.sessionId, args.timerId,
      args.fireAtMs, args.kind, stringifyJson(args.payloadJson),
      now,
    );
    const row = this.db.prepare(`
      SELECT * FROM tape_timers
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND timer_id = ?
    `).get(args.appName, args.userId, args.sessionId, args.timerId) as TimerRow;
    return timerFromRow(row);
  }

  /** Returns timers with `fire_at_ms <= now` and `fired == false`. With
   *  `claim=true`, atomically marks them fired so peer timer reactors don't
   *  re-fire. */
  async listDueTimers(args: {
    nowMs?: number;
    limit?: number;
    claim?: boolean;
  } = {}): Promise<TimerRecord[]> {
    await this.ensureTables();
    const now = args.nowMs || nowMs();
    const limit = args.limit ?? 200;
    const rows = this.db.prepare(`
      SELECT * FROM tape_timers
      WHERE fired = 0 AND fire_at_ms <= ?
      ORDER BY fire_at_ms
      LIMIT ?
    `).all(now, limit) as TimerRow[];
    const result = rows.map(timerFromRow);
    if (args.claim && rows.length > 0) {
      // Atomically mark fired in the same transaction.
      const updateOne = this.db.prepare(`
        UPDATE tape_timers SET fired = 1
        WHERE app_name = ? AND user_id = ? AND session_id = ? AND timer_id = ?
      `);
      const txn = this.db.transaction(() => {
        for (const r of rows) {
          updateOne.run(r.app_name, r.user_id, r.session_id, r.timer_id);
        }
      });
      txn();
    }
    return result;
  }

  async cancelTimer(args: {
    appName: string;
    userId: string;
    sessionId: string;
    timerId: string;
  }): Promise<boolean> {
    await this.ensureTables();
    const res = this.db.prepare(`
      DELETE FROM tape_timers
      WHERE app_name = ? AND user_id = ? AND session_id = ? AND timer_id = ?
    `).run(args.appName, args.userId, args.sessionId, args.timerId);
    return res.changes > 0;
  }

  // ── reactive KV (proto §WriteValue / GetValue) ──────────────────────────

  /** Optimistic-CAS write. `ifVersion < 0` disables CAS (last writer wins).
   *  `ifVersion == current_version` advances; mismatch throws. */
  async writeValue(args: {
    namespace: string;
    key: string;
    valueJson: unknown;
    ifVersion?: number;
    writer?: string;
  }): Promise<ValueRecord> {
    await this.ensureTables();
    const ifVersion = args.ifVersion ?? -1;
    const now = nowMs();
    const existing = this.db.prepare(`
      SELECT * FROM tape_values WHERE namespace = ? AND key = ?
    `).get(args.namespace, args.key) as ValueRow | undefined;

    if (!existing) {
      if (ifVersion >= 0 && ifVersion !== 0) {
        throw new Error(
          `writeValue: ifVersion=${ifVersion} but no prior row exists (version 0)`);
      }
      this.db.prepare(`
        INSERT INTO tape_values (namespace, key, value_json, version, ts_ms, writer, deleted)
        VALUES (?, ?, ?, 1, ?, ?, 0)
      `).run(
        args.namespace, args.key, stringifyJson(args.valueJson),
        now, args.writer ?? null,
      );
      const row = this.db.prepare(`
        SELECT * FROM tape_values WHERE namespace = ? AND key = ?
      `).get(args.namespace, args.key) as ValueRow;
      return valueFromRow(row);
    }

    if (ifVersion >= 0 && ifVersion !== Number(existing.version)) {
      throw new Error(
        `writeValue: stale CAS — ifVersion=${ifVersion}, current=${existing.version}`);
    }
    const newVersion = (Number(existing.version) || 0) + 1;
    this.db.prepare(`
      UPDATE tape_values
      SET value_json = ?, version = ?, ts_ms = ?,
          writer = COALESCE(?, writer), deleted = 0
      WHERE namespace = ? AND key = ?
    `).run(
      stringifyJson(args.valueJson), newVersion, now,
      args.writer ?? null,
      args.namespace, args.key,
    );
    const after = this.db.prepare(`
      SELECT * FROM tape_values WHERE namespace = ? AND key = ?
    `).get(args.namespace, args.key) as ValueRow;
    return valueFromRow(after);
  }

  async getValue(args: { namespace: string; key: string }): Promise<ValueRecord | null> {
    await this.ensureTables();
    const row = this.db.prepare(`
      SELECT * FROM tape_values WHERE namespace = ? AND key = ?
    `).get(args.namespace, args.key) as ValueRow | undefined;
    return row ? valueFromRow(row) : null;
  }
}
