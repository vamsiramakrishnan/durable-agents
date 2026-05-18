// Invariants — predicates over Tape's journal projections. The journal IS
// the oracle. Mirrors `tape.chaos.invariants` from the Python SDK.

import {
  TapeClient, EffectStatus, EffectSemantics, ObligationStatus,
} from '../client.ts';
import type { Invariant, InvariantResult } from './scenarios.ts';

function ok(name: string, detail = ''): InvariantResult {
  return { name, passed: true, detail };
}

function fail(name: string, detail: string): InvariantResult {
  return { name, passed: false, detail };
}

// ── exactly_one ────────────────────────────────────────────────────────────

/**
 * For every CONFIRMED effect under `connector` (or `tool`) that has a
 * non-empty business_key, `count == 1`. The "one wire, one record" claim
 * the treasury test demonstrates.
 *
 * Walks the cross-run journal via `subscribeEvents` with a subject prefix.
 */
export function exactlyOne(opts: { connector?: string; tool?: string;
                                     by?: string; deadlineMs?: number }): Invariant {
  if (!opts.connector && !opts.tool) {
    throw new Error('exactlyOne: needs `connector` or `tool`');
  }
  const by = opts.by ?? 'business_key';
  const deadlineMs = opts.deadlineMs ?? 2_000;
  const name = `exactly_one(${JSON.stringify(opts.connector ?? opts.tool)}, by=${JSON.stringify(by)})`;
  return {
    name,
    async check({ client }) {
      const pattern = opts.tool
        ? `/tape/effect/confirmed/${opts.tool}/**`
        : '/tape/effect/confirmed/**';
      const counts = new Map<string, number>();
      try {
        const stream = client.subscribeEvents({
          fromGlobalSeq: 1, subjectPattern: pattern, timeoutMs: deadlineMs,
        });
        for await (const evt of stream) {
          let payload: any;
          try { payload = JSON.parse((evt as any).payloadJson ?? '{}'); }
          catch { continue; }
          if (opts.connector && payload.connector !== opts.connector) continue;
          const k = String(payload[by] ?? '');
          if (!k) continue;
          counts.set(k, (counts.get(k) ?? 0) + 1);
        }
      } catch (ex) {
        return fail(name, `subscribeEvents failed: ${ex instanceof Error ? ex.message : String(ex)}`);
      }
      const dupes = [...counts.entries()].filter(([, v]) => v > 1);
      if (dupes.length) {
        return fail(name, `duplicate business keys: ${JSON.stringify(Object.fromEntries(dupes))}`);
      }
      return ok(name, `unique business keys: ${counts.size}`);
    },
  };
}

// ── no_stuck_obligations ───────────────────────────────────────────────────

export const noStuckObligations: Invariant = {
  name: 'no_stuck_obligations',
  async check({ client, runId }) {
    try {
      const resp = await client.listUnresolvedObligations({
        includeStuck: true, includePending: false,
        includeCommittedExpired: false, limit: 500,
      });
      const obligations = (resp as any).obligations ?? [];
      const stuck = obligations.filter((o: any) =>
        o.status === ObligationStatus.STUCK
        && (!runId || o.runId === runId));
      if (stuck.length) return fail('no_stuck_obligations', `${stuck.length} stuck obligation(s)`);
      return ok('no_stuck_obligations', '0 stuck');
    } catch (ex) {
      return fail('no_stuck_obligations',
                   `listUnresolvedObligations failed: ${ex instanceof Error ? ex.message : String(ex)}`);
    }
  },
};

// ── no_blind_non_idempotent_retry ──────────────────────────────────────────

/**
 * For every NON_IDEMPOTENT effect, dispatch_attempts <= 1 OR the
 * reconciler has recorded an external_ref. The unsafe case is
 * (semantics=NON_IDEMPOTENT, attempts>1, status=PENDING, external_ref="").
 */
export const noBlindNonIdempotentRetry: Invariant = {
  name: 'no_blind_non_idempotent_retry',
  async check({ client, runId }) {
    try {
      const resp = await client.listPendingEffects({
        includePending: true, includeUnknown: true, limit: 500,
      });
      const effects = (resp as any).effects ?? [];
      const bad = effects.filter((e: any) =>
        (!runId || e.runId === runId)
        && e.semantics === EffectSemantics.NON_IDEMPOTENT
        && (e.dispatchAttempts ?? 0) > 1
        && e.status === EffectStatus.PENDING
        && !(e.externalRef ?? ''));
      if (bad.length) {
        const head = bad.slice(0, 3).map((e: any) => `${e.runId}/${e.idempotencyKey}@${e.dispatchAttempts}`);
        return fail('no_blind_non_idempotent_retry',
                     `${bad.length} non-idempotent effect(s) re-dispatched without observation: ${head.join(', ')}`);
      }
      return ok('no_blind_non_idempotent_retry', 'no blind retries on non-idempotent effects');
    } catch (ex) {
      return fail('no_blind_non_idempotent_retry',
                   `listPendingEffects failed: ${ex instanceof Error ? ex.message : String(ex)}`);
    }
  },
};

// ── no_orphan_compensation ─────────────────────────────────────────────────

export const noOrphanCompensation: Invariant = {
  name: 'no_orphan_compensation',
  async check({ client, runId }) {
    if (!runId) return ok('no_orphan_compensation', 'no runId; skipped');
    try {
      const resp = await client.listObligations({ runId, onlyUnresolved: false });
      const obligations = (resp as any).obligations ?? [];
      const orphans: string[] = [];
      for (const o of obligations) {
        try {
          const got: any = await client.getEffect({ runId, idempotencyKey: o.effectKey });
          if (!got.found) orphans.push(o.effectKey);
        } catch { orphans.push(o.effectKey); }
      }
      if (orphans.length) {
        return fail('no_orphan_compensation',
                     `${orphans.length} obligation(s) with no effect: ${orphans.slice(0, 3).join(', ')}`);
      }
      return ok('no_orphan_compensation', `all ${obligations.length} obligation(s) have an effect`);
    } catch (ex) {
      return fail('no_orphan_compensation',
                   `listObligations failed: ${ex instanceof Error ? ex.message : String(ex)}`);
    }
  },
};

// ── no_budget_overrun (v1 stub; full check is Phase 3.5) ───────────────────

export const noBudgetOverrun: Invariant = {
  name: 'no_budget_overrun',
  async check({ runId }) {
    if (!runId) return ok('no_budget_overrun', 'no runId; skipped');
    return ok('no_budget_overrun', 'budget projection check is a Phase-3 invariant; v1 stub');
  },
};
