// `@tape.outbox_tool` — the JS/TS surface. There are no Python-style
// decorators by default; you annotate by calling `outboxTool(fn, opts)`,
// which returns a wrapped function that produces an *intent envelope* the
// outbox reactor will dispatch via the named connector.
//
// Rules — enforced at construction time:
//
//   * `semantics: 'non_idempotent'` MUST declare at least one of
//     `businessKey`, `statusCheck`, `compensate`, or `humanGate: true`.
//
//   * The wrapped body MUST be synchronous and pure (no IO). The
//     connector does IO; the body builds the intent.

import { registerCompensator, registerStatusCheck } from './effect.ts';

export type OutboxSemantics = 'idempotent' | 'at_least_once' | 'non_idempotent';

export class OutboxConfigError extends Error {
  constructor(message: string) {
    super('tape.outbox: ' + message);
    this.name = 'OutboxConfigError';
  }
}

export interface OutboxToolOpts {
  /** Tool name (as it appears to the model + in the journal). */
  name: string;
  /** Registered capability connector name (e.g. "bank.wire"). */
  connector: string;
  /** Defaults to 'idempotent'. */
  semantics?: OutboxSemantics;
  /** Given the intent payload, derive a stable dedup key. */
  businessKey?: (payload: Record<string, unknown>) => string;
  /** Resolves UNKNOWN via a counterparty lookup. */
  statusCheck?: (idempotencyKey: string) => Promise<unknown> | unknown;
  /** Reverses a duplicate (or any registered obligation). */
  compensate?: (payload: Record<string, unknown>) => Promise<unknown> | unknown;
  /** Park the run until dispatch resolves (the ADK-TS adapter reads this). */
  waitForResult?: boolean;
  /** Park the run on a human gate before dispatch. */
  humanGate?: boolean;
  /** Soft deadline for the connector (0 = connector default). */
  dispatchTimeoutMs?: number;
  /** Outbox-reactor retry budget (server default 5 when 0). */
  maxAttempts?: number;
}

export interface OutboxEnvelope {
  __outbox__: true;
  connector: string;
  tool: string;
  semantics: OutboxSemantics;
  wait_for_result: boolean;
  human_gate: boolean;
  dispatch_timeout_ms?: number;
  business_key?: string;
  payload: Record<string, unknown>;
}

const META = Symbol.for('tape.outbox.meta');

/** Read the outbox meta off a wrapped function (used by adapters). */
export function outboxMetaOf(fn: any): Required<Pick<OutboxToolOpts, 'name' | 'connector' | 'semantics'>>
    & OutboxToolOpts | undefined {
  return fn && fn[META];
}

/** True iff `value` looks like an `OutboxEnvelope`. */
export function isOutboxEnvelope(value: unknown): value is OutboxEnvelope {
  return !!value && typeof value === 'object' && (value as any).__outbox__ === true;
}

/**
 * Mark a tool body as outbox-dispatched. Returns a wrapped function that
 * produces an `OutboxEnvelope` from its arguments.
 *
 * ```ts
 * const wireMoney = outboxTool(
 *   ({ account, amount, beneficiary, date }: {
 *      account: string; amount: number; beneficiary: string; date: string;
 *   }) => ({ account, amount, beneficiary, date }),
 *   {
 *     name: 'wire_money',
 *     connector: 'bank.wire',
 *     semantics: 'non_idempotent',
 *     businessKey: (p) => `${p.account}:${p.amount}:${p.date}`,
 *     waitForResult: true,
 *   },
 * );
 * ```
 */
export function outboxTool<A extends Record<string, unknown>>(
  fn: (args: A) => Record<string, unknown>,
  opts: OutboxToolOpts,
): (args: A) => OutboxEnvelope {
  if (!opts.name) throw new OutboxConfigError('opts.name is required');
  if (!opts.connector) throw new OutboxConfigError('opts.connector is required');
  const semantics: OutboxSemantics = opts.semantics ?? 'idempotent';
  if (!['idempotent', 'at_least_once', 'non_idempotent'].includes(semantics)) {
    throw new OutboxConfigError(`unknown semantics ${semantics}`);
  }
  if (semantics === 'non_idempotent') {
    if (!opts.businessKey && !opts.statusCheck && !opts.compensate && !opts.humanGate) {
      throw new OutboxConfigError(
        'non_idempotent tools must declare at least one of businessKey, statusCheck, ' +
        'compensate, or humanGate=true — otherwise an UNKNOWN dispatch could be blindly retried',
      );
    }
  }
  // Side-effect: register status_check / compensate so the reactors see them.
  if (opts.statusCheck) registerStatusCheck(opts.name, opts.statusCheck);
  if (opts.compensate) registerCompensator(opts.compensate.name || opts.name, opts.compensate);

  const wrapped = (args: A): OutboxEnvelope => {
    const payload = fn(args);
    if (payload === null || typeof payload !== 'object') {
      throw new OutboxConfigError(
        `outbox tool ${opts.name} returned ${typeof payload}; must return an object intent.`,
      );
    }
    const env: OutboxEnvelope = {
      __outbox__: true,
      connector: opts.connector,
      tool: opts.name,
      semantics,
      wait_for_result: !!opts.waitForResult,
      human_gate: !!opts.humanGate,
      payload: payload as Record<string, unknown>,
    };
    if (opts.dispatchTimeoutMs && opts.dispatchTimeoutMs > 0) {
      env.dispatch_timeout_ms = opts.dispatchTimeoutMs;
    }
    if (opts.businessKey) env.business_key = String(opts.businessKey(payload as Record<string, unknown>));
    return env;
  };
  (wrapped as any)[META] = { ...opts, semantics };
  Object.defineProperty(wrapped, 'name', { value: opts.name });
  return wrapped;
}
