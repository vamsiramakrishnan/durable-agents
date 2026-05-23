// Reactors as plain async functions over `TapeSessionService`. Mirrors
// `tape_adk/reactors.py` semantics-for-semantics. Each `*Once` does at most
// `limit` items per tick, so a busy loop self-rate-limits naturally.
// Crash-safety is built in: claims have TTLs, so a process that dies mid-tick
// releases its work to the next runner.

import type {
  CompensationResult,
  Connector,
  DispatchResult,
  ObservationResult,
} from './connectors.ts';
import {
  EffectStatus,
  ObligationStatus,
  type EffectRecord,
  type ObligationRecord,
  type TapeSessionService,
  type TimerRecord,
} from './service.ts';

function nowMs(): number { return Date.now(); }

export interface TickAuditEntry {
  key?: string;
  seq?: number;
  timerId?: string;
  skip?: string;
  outcome?: string;
  externalRef?: string;
  backoffMs?: number;
}

// ── outbox dispatcher ──────────────────────────────────────────────────────

export async function dispatchOutboxOnce(
  svc: TapeSessionService,
  opts: {
    connectors: Record<string, Connector>;
    claimer: string;
    limit?: number;
    leaseTtlMs?: number;
    defaultBackoffMs?: number;
    maxBackoffMs?: number;
  },
): Promise<TickAuditEntry[]> {
  const limit = opts.limit ?? 50;
  const leaseTtlMs = opts.leaseTtlMs ?? 60_000;
  const defaultBackoffMs = opts.defaultBackoffMs ?? 5_000;
  const maxBackoffMs = opts.maxBackoffMs ?? 300_000;

  const results: TickAuditEntry[] = [];
  const now = nowMs();
  const effects = await svc.listEffectsToDispatch({ nowMs: now, limit });
  for (const eff of effects) {
    const connectorName = eff.connector ?? '';
    if (!(connectorName in opts.connectors)) {
      results.push({ key: eff.idempotencyKey, skip: `no connector for ${JSON.stringify(eff.connector)}` });
      continue;
    }
    const [acquired] = await svc.claimEffectDispatch({
      appName: eff.appName, userId: eff.userId, sessionId: eff.sessionId,
      idempotencyKey: eff.idempotencyKey,
      claimer: opts.claimer, leaseTtlMs, nowMs: now,
    });
    if (!acquired) {
      results.push({ key: eff.idempotencyKey, skip: 'lost the claim' });
      continue;
    }
    // Re-read after winning the lease in case the row mutated.
    const fresh = await svc.getEffect({
      appName: eff.appName, userId: eff.userId, sessionId: eff.sessionId,
      idempotencyKey: eff.idempotencyKey,
    });
    if (!fresh || fresh.status !== EffectStatus.PENDING) {
      results.push({ key: eff.idempotencyKey, skip: 'not PENDING after claim' });
      continue;
    }

    const connector = opts.connectors[fresh.connector ?? ''];
    let outcome: DispatchResult;
    try {
      outcome = await connector.dispatch(fresh);
    } catch (ex) {
      const attempts = fresh.dispatchAttempts + 1;
      const backoff = Math.min(
        defaultBackoffMs * 2 ** Math.max(0, attempts - 1),
        maxBackoffMs,
      );
      await svc.recordDispatchAttempt({
        appName: fresh.appName, userId: fresh.userId, sessionId: fresh.sessionId,
        idempotencyKey: fresh.idempotencyKey,
        error: `${ex instanceof Error ? ex.constructor.name : 'Error'}: ${ex instanceof Error ? ex.message : String(ex)}`,
        nextDispatchAtMs: now + backoff,
      });
      results.push({ key: fresh.idempotencyKey, outcome: 'exception', backoffMs: backoff });
      continue;
    }

    if (outcome.status === 'confirmed') {
      await svc.completeEffect({
        appName: fresh.appName, userId: fresh.userId, sessionId: fresh.sessionId,
        idempotencyKey: fresh.idempotencyKey,
        status: EffectStatus.CONFIRMED,
        responseJson: outcome.response,
      });
      if (outcome.externalRef) {
        // Direct UPDATE to attach external_ref (the effect is already CONFIRMED
        // and completeEffect's terminal-idempotency would not touch it again).
        await attachExternalRef(svc, fresh, outcome.externalRef);
      }
      results.push({ key: fresh.idempotencyKey, outcome: 'confirmed', externalRef: outcome.externalRef });
    } else if (outcome.status === 'unknown') {
      await svc.recordDispatchAttempt({
        appName: fresh.appName, userId: fresh.userId, sessionId: fresh.sessionId,
        idempotencyKey: fresh.idempotencyKey,
        error: String((outcome.error as { message?: string } | undefined)?.message ?? outcome.error ?? 'ack lost'),
        nextDispatchAtMs: 0,
      });
      results.push({ key: fresh.idempotencyKey, outcome: 'unknown' });
    } else if (outcome.status === 'failed') {
      const retryAfter = outcome.retryAfterMs ?? 0;
      if (retryAfter < 0) {
        await svc.completeEffect({
          appName: fresh.appName, userId: fresh.userId, sessionId: fresh.sessionId,
          idempotencyKey: fresh.idempotencyKey,
          status: EffectStatus.FAILED,
          errorJson: outcome.error,
        });
        results.push({ key: fresh.idempotencyKey, outcome: 'failed-terminal' });
      } else {
        const attempts = fresh.dispatchAttempts + 1;
        const backoff = retryAfter || Math.min(
          defaultBackoffMs * 2 ** Math.max(0, attempts - 1),
          maxBackoffMs,
        );
        await svc.recordDispatchAttempt({
          appName: fresh.appName, userId: fresh.userId, sessionId: fresh.sessionId,
          idempotencyKey: fresh.idempotencyKey,
          error: String((outcome.error as { message?: string } | undefined)?.message ?? outcome.error ?? 'dispatch failed'),
          nextDispatchAtMs: now + backoff,
        });
        results.push({ key: fresh.idempotencyKey, outcome: 'failed-retry', backoffMs: backoff });
      }
    }
  }
  return results;
}

// Helper: attach external_ref directly to an effect via a one-row UPDATE.
// Mirrors the Python reactor's same trick: complete_effect already set the
// status to CONFIRMED; we just want to stamp the ref.
async function attachExternalRef(
  svc: TapeSessionService, fresh: EffectRecord, externalRef: string,
): Promise<void> {
  svc.db.prepare(`
    UPDATE tape_effects
    SET external_ref = ?
    WHERE app_name = ? AND user_id = ? AND session_id = ? AND idempotency_key = ?
  `).run(
    externalRef,
    fresh.appName, fresh.userId, fresh.sessionId, fresh.idempotencyKey,
  );
}

// ── reconciler ─────────────────────────────────────────────────────────────

export async function reconcileOnce(
  svc: TapeSessionService,
  opts: {
    connectors: Record<string, Connector>;
    stalePendingMs?: number;
    limit?: number;
  },
): Promise<TickAuditEntry[]> {
  const stalePendingMs = opts.stalePendingMs ?? 0;
  const limit = opts.limit ?? 50;
  const cutoff = stalePendingMs > 0 ? nowMs() - stalePendingMs : 0;
  const effects = await svc.listPendingEffects({
    olderThanMs: cutoff,
    includePending: stalePendingMs > 0,
    includeUnknown: true,
    limit,
  });
  const results: TickAuditEntry[] = [];
  for (const eff of effects) {
    const connector = opts.connectors[eff.connector ?? ''];
    if (!connector) {
      results.push({ key: eff.idempotencyKey, skip: `no connector for ${JSON.stringify(eff.connector)}` });
      continue;
    }
    let obs: ObservationResult;
    try {
      obs = await connector.observe(eff);
    } catch (ex) {
      results.push({ key: eff.idempotencyKey, skip: `observe raised: ${ex instanceof Error ? ex.message : String(ex)}` });
      continue;
    }
    await svc.recordExternalObservation({
      appName: eff.appName, userId: eff.userId, sessionId: eff.sessionId,
      idempotencyKey: eff.idempotencyKey,
      resolution: obs.status,
      externalRef: obs.externalRef,
      responseJson: obs.response,
      errorJson: obs.error,
      compensateOnDuplicateKind: obs.compensateKind,
    });
    results.push({ key: eff.idempotencyKey, outcome: obs.status, externalRef: obs.externalRef });
  }
  return results;
}

// ── compensation drainer ───────────────────────────────────────────────────

export async function drainObligationsOnce(
  svc: TapeSessionService,
  opts: {
    connectors: Record<string, Connector>;
    claimer?: string;
    limit?: number;
    leaseTtlMs?: number;
    defaultBackoffMs?: number;
    maxBackoffMs?: number;
  },
): Promise<TickAuditEntry[]> {
  const claimer = opts.claimer ?? 'drainer';
  const limit = opts.limit ?? 50;
  const leaseTtlMs = opts.leaseTtlMs ?? 60_000;
  const defaultBackoffMs = opts.defaultBackoffMs ?? 5_000;
  const maxBackoffMs = opts.maxBackoffMs ?? 300_000;

  const results: TickAuditEntry[] = [];
  const now = nowMs();
  const obligations = await svc.listUnresolvedObligations({
    nowMs: now, limit,
    includePending: true, includeCommittedExpired: true,
    includeStuck: false,
  });
  for (const ob of obligations) {
    // Find the effect (if any) to resolve the connector name; fall back to
    // ob.kind. Mirrors the Python reactor's lookup.
    let eff: EffectRecord | null = null;
    if (ob.effectKey) {
      eff = await svc.getEffect({
        appName: ob.appName, userId: ob.userId, sessionId: ob.sessionId,
        idempotencyKey: ob.effectKey,
      });
    }
    const connectorName = (eff?.connector) || ob.kind;
    const connector = opts.connectors[connectorName];
    if (!connector) {
      results.push({ seq: ob.seq, skip: `no connector for ${JSON.stringify(connectorName)}` });
      continue;
    }

    const [acquired] = await svc.claimObligation({
      seq: ob.seq, claimer, leaseTtlMs, nowMs: now,
    });
    if (!acquired) {
      results.push({ seq: ob.seq, skip: 'lost the claim' });
      continue;
    }

    let outcome: CompensationResult;
    try {
      outcome = await connector.compensate(ob);
    } catch (ex) {
      const attempts = ob.attempts + 1;
      const backoff = Math.min(
        defaultBackoffMs * 2 ** Math.max(0, attempts - 1),
        maxBackoffMs,
      );
      await svc.recordObligationAttempt({
        seq: ob.seq,
        error: `${ex instanceof Error ? ex.constructor.name : 'Error'}: ${ex instanceof Error ? ex.message : String(ex)}`,
        nextAttemptAtMs: now + backoff,
      });
      results.push({ seq: ob.seq, outcome: 'exception', backoffMs: backoff });
      continue;
    }

    if (outcome.status === 'compensated') {
      await svc.resolveObligation({
        seq: ob.seq, status: ObligationStatus.COMPENSATED,
        resultJson: outcome.response,
      });
      results.push({ seq: ob.seq, outcome: 'compensated' });
    } else if (outcome.status === 'failed') {
      const backoff = (outcome.retryAfterMs ?? 0) || Math.min(
        defaultBackoffMs * 2 ** Math.max(0, ob.attempts),
        maxBackoffMs,
      );
      await svc.recordObligationAttempt({
        seq: ob.seq,
        error: String((outcome.error as { message?: string } | undefined)?.message ?? outcome.error ?? 'compensate failed'),
        nextAttemptAtMs: now + backoff,
      });
      results.push({ seq: ob.seq, outcome: 'failed-retry', backoffMs: backoff });
    }
  }
  return results;
}

// ── timer firer ────────────────────────────────────────────────────────────

export async function fireDueTimersOnce(
  svc: TapeSessionService,
  opts: {
    dispatcher?: (t: TimerRecord) => Promise<void>;
    limit?: number;
  } = {},
): Promise<TickAuditEntry[]> {
  const limit = opts.limit ?? 100;
  const timers = await svc.listDueTimers({
    nowMs: nowMs(), limit, claim: true,
  });
  const out: TickAuditEntry[] = [];
  for (const t of timers) {
    if (opts.dispatcher) {
      try {
        await opts.dispatcher(t);
        out.push({ timerId: t.timerId, outcome: 'fired' });
      } catch (ex) {
        out.push({ timerId: t.timerId, outcome: `dispatcher raised: ${ex instanceof Error ? ex.message : String(ex)}` });
      }
    } else {
      out.push({ timerId: t.timerId, outcome: 'marked-fired' });
    }
  }
  return out;
}
