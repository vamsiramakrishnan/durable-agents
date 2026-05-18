// Outbox-reactor daemon — TS counterpart of Python's `tape.reactors.outbox`.
//
// One pass:
//   list effects to dispatch (PENDING + OUTBOX + due)
//   for each:
//     claim (atomic CAS lease)
//     look up the connector
//     dispatch through it
//     record result:
//       confirmed → completeEffect(CONFIRMED)
//       failed    → recordDispatchAttempt(next_at = backoff) → eventually FAILED
//       unknown   → recordDispatchAttempt(next_at = 0) → status UNKNOWN
//                   (the reconciler resolves; do NOT blindly retry —
//                    that is the entire safety claim for non-idempotent
//                    upstreams)
//
// Safety: the server's CAS on claim_effect_dispatch enforces non-blind-retry;
// this reactor double-checks (refuses to act on anything not PENDING after
// the claim).

import * as os from 'node:os';
import {
  TapeClient, DEFAULT_URL,
  EffectStatus, EffectSemantics, EffectResolution,
} from './client.ts';
import {
  CONNECTORS, ConnectorRegistry, type Connector, type EffectRecord, type DispatchResult,
} from './connectors/index.ts';

export interface OutboxReactorOptions {
  /** TapeClient — pass an existing one or let the loop create its own. */
  client?: TapeClient;
  /** URL when constructing a new client. Honoured iff `client` is undefined. */
  url?: string;
  /** Restrict to one connector name (matches @outboxTool({ connector }) on the tool). */
  connector?: string;
  /** Custom registry; defaults to CONNECTORS (the process-global). */
  registry?: ConnectorRegistry;
  /** Identity recorded as `dispatch_claimed_by`. */
  claimer?: string;
  /** Page size. */
  limit?: number;
  /** Give up on a connector failure after N attempts (then mark FAILED). */
  dispatchMaxAttempts?: number;
}

export interface OutboxOutcome {
  runId: string;
  idempotencyKey: string;
  connector: string;
  tool: string;
  status: 'confirmed' | 'unknown' | 'failed' | 'retry-scheduled' | 'skipped' | 'error';
  reason?: string;
  externalRef?: string;
  error?: string;
  nextAtMs?: number;
  attempts?: number;
}

function claimerId(): string {
  return process.env.TAPE_DISPATCH_CLAIMER ?? `${os.hostname()}:${process.pid}`;
}

function backoffMs(attempt: number, baseS = 1.0, maxS = 60.0): number {
  const delayS = Math.min(baseS * Math.pow(2, Math.max(attempt - 1, 0)), maxS);
  return Math.floor(delayS * 1000);
}

function toConnectorEffect(eff: any): EffectRecord {
  let payload: unknown = eff.requestJson ?? '';
  if (typeof payload === 'string' && payload.length > 0) {
    try { payload = JSON.parse(payload); } catch { /* leave string */ }
  }
  return {
    runId: eff.runId,
    idempotencyKey: eff.idempotencyKey,
    toolName: eff.toolName,
    connector: eff.connector,
    payload,
    businessKey: eff.businessKey ?? '',
    attempt: (eff.dispatchAttempts ?? 0) + 1,
    semantics: eff.semantics === EffectSemantics.NON_IDEMPOTENT ? 'non_idempotent' : 'idempotent',
    tenantId: eff.tenantId ?? '',
    appName: eff.appName ?? '',
  };
}

export async function dispatchOne(
  eff: any,
  opts: Required<Pick<OutboxReactorOptions, 'client' | 'claimer' | 'dispatchMaxAttempts'>>
    & { registry: ConnectorRegistry },
): Promise<OutboxOutcome> {
  const out: OutboxOutcome = {
    runId: eff.runId,
    idempotencyKey: eff.idempotencyKey,
    connector: eff.connector,
    tool: eff.toolName,
    status: 'skipped',
  };

  if (!opts.registry.has(eff.connector)) {
    out.reason = `no connector registered: '${eff.connector}'`;
    return out;
  }
  const connector: Connector = opts.registry.get(eff.connector);

  const claim: any = await opts.client.claimEffectDispatch({
    runId: eff.runId, idempotencyKey: eff.idempotencyKey,
    claimer: opts.claimer, leaseTtlMs: 60_000,
  });
  if (!claim.acquired) { out.reason = 'lease contended'; return out; }

  const cur = claim.effect;
  if (cur.status !== EffectStatus.PENDING) {
    out.reason = `unexpected status after claim: ${cur.status}`;
    return out;
  }

  const isNonIdem = cur.semantics === EffectSemantics.NON_IDEMPOTENT;
  let result: DispatchResult;
  try {
    result = await connector.dispatch(toConnectorEffect(cur));
  } catch (ex: any) {
    if (isNonIdem) {
      await opts.client.recordDispatchAttempt({
        runId: cur.runId, idempotencyKey: cur.idempotencyKey,
        error: `connector raised: ${ex?.name ?? 'Error'}: ${ex?.message ?? String(ex)}`,
        nextDispatchAtMs: 0,
      });
      out.status = 'unknown'; out.error = String(ex);
      return out;
    }
    const attempts = (cur.dispatchAttempts ?? 0) + 1;
    const nextAt = Date.now() + backoffMs(attempts);
    await opts.client.recordDispatchAttempt({
      runId: cur.runId, idempotencyKey: cur.idempotencyKey,
      error: `connector raised: ${ex?.name ?? 'Error'}: ${ex?.message ?? String(ex)}`,
      nextDispatchAtMs: nextAt,
    });
    out.status = 'retry-scheduled'; out.error = String(ex); out.nextAtMs = nextAt; out.attempts = attempts;
    return out;
  }

  if (result.outcome === 'confirmed') {
    const responseBody = result.response ?? {};
    const responseJson = JSON.stringify({
      external_ref: result.dispatchId ?? '',
      ...(typeof responseBody === 'object' && responseBody !== null ? responseBody : { value: responseBody }),
    });
    await opts.client.completeEffect({
      runId: cur.runId, idempotencyKey: cur.idempotencyKey,
      status: EffectStatus.CONFIRMED, responseJson,
    });
    out.status = 'confirmed'; out.externalRef = result.dispatchId ?? '';
    return out;
  }

  if (result.outcome === 'unknown') {
    await opts.client.recordDispatchAttempt({
      runId: cur.runId, idempotencyKey: cur.idempotencyKey,
      error: JSON.stringify(result.error ?? { reason: 'ack lost' }),
      nextDispatchAtMs: 0,
    });
    out.status = 'unknown';
    return out;
  }

  // failed / pending → retry or terminal-FAILED
  const attempts = (cur.dispatchAttempts ?? 0) + 1;
  if (attempts >= opts.dispatchMaxAttempts) {
    await opts.client.recordExternalObservation({
      runId: cur.runId, idempotencyKey: cur.idempotencyKey,
      resolution: EffectResolution.FAILED,
      errorJson: JSON.stringify({ final: true, attempts, last: result.error ?? null }),
    });
    out.status = 'failed'; out.attempts = attempts;
    return out;
  }
  const nextAt = (result.retryAfterMs ?? 0) > 0
    ? Date.now() + result.retryAfterMs!
    : Date.now() + backoffMs(attempts);
  await opts.client.recordDispatchAttempt({
    runId: cur.runId, idempotencyKey: cur.idempotencyKey,
    error: JSON.stringify(result.error ?? {}),
    nextDispatchAtMs: nextAt,
  });
  out.status = 'retry-scheduled'; out.nextAtMs = nextAt; out.attempts = attempts;
  return out;
}

export async function outboxDispatchOnce(opts: OutboxReactorOptions = {}): Promise<OutboxOutcome[]> {
  const ownsClient = opts.client === undefined;
  const client = opts.client ?? new TapeClient(opts.url ?? DEFAULT_URL);
  const registry = opts.registry ?? CONNECTORS;
  const claimer = opts.claimer || claimerId();
  const limit = opts.limit ?? 200;
  const dispatchMaxAttempts = opts.dispatchMaxAttempts ?? 5;
  const outcomes: OutboxOutcome[] = [];
  try {
    const resp: any = await client.listEffectsToDispatch({ connector: opts.connector ?? '', limit });
    for (const eff of resp.effects ?? []) {
      try {
        outcomes.push(await dispatchOne(eff, { client, registry, claimer, dispatchMaxAttempts }));
      } catch (ex: any) {
        outcomes.push({
          runId: eff.runId, idempotencyKey: eff.idempotencyKey,
          connector: eff.connector, tool: eff.toolName,
          status: 'error', error: String(ex),
        });
      }
    }
  } finally {
    if (ownsClient) client.close();
  }
  return outcomes;
}

export interface RunOutboxOptions extends OutboxReactorOptions {
  intervalMs?: number;
  once?: boolean;
  onTick?: (outcomes: OutboxOutcome[]) => void;
}

export async function runOutboxDispatcher(opts: RunOutboxOptions = {}): Promise<void> {
  const ownsClient = opts.client === undefined;
  const client = opts.client ?? new TapeClient(opts.url ?? DEFAULT_URL);
  const intervalMs = opts.intervalMs ?? 1000;
  try {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      let outcomes: OutboxOutcome[] = [];
      try {
        outcomes = await outboxDispatchOnce({ ...opts, client });
      } catch (ex: any) {
        process.stderr.write(`[tape outbox] tick error: ${ex?.message ?? ex}\n`);
      }
      opts.onTick?.(outcomes);
      if (opts.once) return;
      await new Promise((r) => setTimeout(r, intervalMs));
    }
  } finally {
    if (ownsClient) client.close();
  }
}
