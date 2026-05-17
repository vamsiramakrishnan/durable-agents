// Reactors — the WAL-driven side. Each is idempotent (the lease + replay
// properties make a double-run harmless), so run as many copies as you like.

import { TapeClient, EffectStatus, DEFAULT_URL } from './client.ts';
import { getStatusCheck } from './effect.ts';

export type RedriveFn = (run: any) => Promise<void> | void;

// ── recovery ────────────────────────────────────────────────────────────────
export async function recoverOnce(opts: { url?: string; redriveFn: RedriveFn; limit?: number }): Promise<Array<{ runId: string; invocationId: string }>> {
  const c = new TapeClient(opts.url ?? DEFAULT_URL);
  try {
    const resp: any = await c.listRunsToRecover({ limit: opts.limit ?? 50 });
    const out: Array<{ runId: string; invocationId: string }> = [];
    for (const r of resp.runs ?? []) {
      await opts.redriveFn(r);
      out.push({ runId: r.runId, invocationId: r.invocationId });
    }
    return out;
  } finally { c.close(); }
}

// ── reconciler ──────────────────────────────────────────────────────────────
export async function reconcileOnce(opts: { url?: string; reconcilePendingAfterMs?: number } = {}): Promise<Array<{ key: string; resolved: string }>> {
  const c = new TapeClient(opts.url ?? DEFAULT_URL);
  try {
    const includePending = (opts.reconcilePendingAfterMs ?? 0) > 0;
    const olderThanMs = includePending ? Date.now() - (opts.reconcilePendingAfterMs ?? 0) : 0;
    const resp: any = await c.listPendingEffects({ olderThanMs, includePending, includeUnknown: true, limit: 500 });
    const out: Array<{ key: string; resolved: string }> = [];
    for (const e of resp.effects ?? []) {
      const check = getStatusCheck(e.toolName);
      if (!check) continue;
      let res: any;
      try { res = await check(e.idempotencyKey); }
      catch (ex) { out.push({ key: e.idempotencyKey, resolved: `check-error: ${ex}` }); continue; }
      const found = typeof res === 'object' && res !== null ? Boolean(res.found ?? true) : Boolean(res);
      if (found) {
        await c.reconcileEffect({ runId: e.runId, idempotencyKey: e.idempotencyKey,
          resolvedStatus: EffectStatus.CONFIRMED, responseJson: JSON.stringify(res ?? {}) });
        out.push({ key: e.idempotencyKey, resolved: 'confirmed' });
      } else if (e.status === EffectStatus.UNKNOWN) {
        await c.reconcileEffect({ runId: e.runId, idempotencyKey: e.idempotencyKey,
          resolvedStatus: EffectStatus.FAILED, errorJson: JSON.stringify({ reconciled: 'absent at counterparty' }) });
        out.push({ key: e.idempotencyKey, resolved: 'failed' });
      }
    }
    return out;
  } finally { c.close(); }
}

// ── timer reactor ───────────────────────────────────────────────────────────
export async function fireDueTimersOnce(opts: { url?: string; redriveFn?: RedriveFn; onTimer?: (t: any) => Promise<void> | void } = {}): Promise<Array<{ runId: string; timerId: string; kind: string; action: string }>> {
  const c = new TapeClient(opts.url ?? DEFAULT_URL);
  try {
    const resp: any = await c.listDueTimers({ claim: true, limit: 500 });
    const out: Array<{ runId: string; timerId: string; kind: string; action: string }> = [];
    for (const t of resp.timers ?? []) {
      let payload: any = {};
      try { payload = t.payloadJson ? JSON.parse(t.payloadJson) : {}; } catch {}
      let action = 'ignored';
      try {
        if (t.kind === 'gate_timeout') {
          await c.sendSignal({ runId: t.runId, gateName: payload.gate ?? '',
            resolutionJson: JSON.stringify({ timedOut: true, ...(payload.resolution ?? {}) }) });
          action = `signalled ${payload.gate} (timeout)`;
        } else if (t.kind === 'redrive' && opts.redriveFn) {
          const run = await c.getRun(t.runId);
          await opts.redriveFn(run);
          action = 're-driven';
        } else if (opts.onTimer) {
          await opts.onTimer(t); action = 'delegated';
        }
      } catch (e) { action = `error: ${e}`; }
      out.push({ runId: t.runId, timerId: t.timerId, kind: t.kind, action });
    }
    return out;
  } finally { c.close(); }
}

// ── the loop ────────────────────────────────────────────────────────────────
export interface RunReactorsOptions {
  url?: string;
  redriveFn?: RedriveFn;
  recover?: boolean;
  reconcile?: boolean;
  timers?: boolean;
  intervalMs?: number;
  reconcilePendingAfterMs?: number;
  once?: boolean;
  onTick?: (tick: any) => void;
}

export async function runReactors(opts: RunReactorsOptions = {}): Promise<void> {
  const intervalMs = opts.intervalMs ?? 2000;
  const recover = opts.recover ?? true;
  const reconcile = opts.reconcile ?? true;
  const timers = opts.timers ?? true;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const tick: any = {};
    if (recover && opts.redriveFn) {
      try { tick.recovered = await recoverOnce({ url: opts.url, redriveFn: opts.redriveFn }); }
      catch (e) { tick.recoverError = String(e); }
    }
    if (reconcile) {
      try { tick.reconciled = await reconcileOnce({ url: opts.url, reconcilePendingAfterMs: opts.reconcilePendingAfterMs }); }
      catch (e) { tick.reconcileError = String(e); }
    }
    if (timers) {
      try { tick.timersFired = await fireDueTimersOnce({ url: opts.url, redriveFn: opts.redriveFn }); }
      catch (e) { tick.timerError = String(e); }
    }
    opts.onTick?.(tick);
    if (opts.once) return;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

// ── WAL fan-out ─────────────────────────────────────────────────────────────
export async function runEventFanout(opts: { url?: string; sink: (entry: any) => Promise<void> | void; fromTsMs?: number; runId?: string; kind?: string }): Promise<void> {
  const c = new TapeClient(opts.url ?? DEFAULT_URL);
  try {
    for await (const entry of c.subscribeEvents({ fromTsMs: opts.fromTsMs, runId: opts.runId, kind: opts.kind })) {
      await opts.sink(entry);
    }
  } finally { c.close(); }
}
