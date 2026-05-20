// Connector protocol — how the outbox dispatcher talks to a counterparty,
// and how the reconciler asks the counterparty what really happened.
// Mirrors `tape_adk/connectors.py` shape-for-shape.
//
// A connector implements three async operations against one upstream system
// (`bank.wire`, `payment.charge`, `email.send`, …):
//
// * `dispatch(effect)` — actually call the upstream. Returns CONFIRMED,
//   UNKNOWN (call may have landed but the ack was lost), FAILED, or
//   throws (treated as retry-after-backoff).
// * `observe(effect)` — ask the upstream by `business_key` whether the
//   operation lives in its records. Returns CONFIRMED + external_ref,
//   FAILED, ABSENT, or DUPLICATE.
// * `compensate(obligation)` — run the inverse. Returns COMPENSATED or FAILED.

import type { EffectRecord, ObligationRecord } from './service.ts';

// ── result shapes ──────────────────────────────────────────────────────────

/** What a single dispatch attempt produced. */
export interface DispatchResult {
  /** 'confirmed' | 'unknown' | 'failed' */
  status: 'confirmed' | 'unknown' | 'failed';
  externalRef?: string;
  response?: unknown;
  error?: unknown;
  /** Backoff hint; only honored when status === 'failed'. 0 = default. */
  retryAfterMs?: number;
}

/** What observe(business_key) found on the counterparty's side. */
export interface ObservationResult {
  /** 'confirmed' | 'failed' | 'absent' | 'duplicate' | 'stuck' */
  status: 'confirmed' | 'failed' | 'absent' | 'duplicate' | 'stuck';
  externalRef?: string;
  response?: unknown;
  error?: unknown;
  /** When status === 'duplicate', the obligation kind to register. */
  compensateKind?: string;
}

/** What the inverse-operation call did. */
export interface CompensationResult {
  /** 'compensated' | 'failed' */
  status: 'compensated' | 'failed';
  response?: unknown;
  error?: unknown;
  retryAfterMs?: number;
}

// ── the protocol ───────────────────────────────────────────────────────────

/**
 * Implement three async methods and you're a connector. The reactor
 * library does the rest — claiming, transitioning, retrying with
 * backoff, and recording the result against the journal.
 *
 * `name` is the registry key — the same string used in `outboxTool({connector: 'bank.wire'})`.
 */
export interface Connector {
  name: string;
  dispatch(effect: EffectRecord): Promise<DispatchResult>;
  observe(effect: EffectRecord): Promise<ObservationResult>;
  compensate(obligation: ObligationRecord): Promise<CompensationResult>;
}

// ── tiny built-in connectors for tests + smoke runs ────────────────────────

/** A no-op connector that logs every call. Useful for tests + demos. */
export class LogConnector implements Connector {
  name: string;
  dispatches: EffectRecord[] = [];
  observations: EffectRecord[] = [];
  compensations: ObligationRecord[] = [];

  constructor(name = 'log') {
    this.name = name;
  }

  async dispatch(effect: EffectRecord): Promise<DispatchResult> {
    this.dispatches.push(effect);
    return {
      status: 'confirmed',
      externalRef: `log-${effect.idempotencyKey.slice(0, 8)}`,
    };
  }

  async observe(effect: EffectRecord): Promise<ObservationResult> {
    this.observations.push(effect);
    return { status: 'absent' };
  }

  async compensate(obligation: ObligationRecord): Promise<CompensationResult> {
    this.compensations.push(obligation);
    return { status: 'compensated' };
  }
}
