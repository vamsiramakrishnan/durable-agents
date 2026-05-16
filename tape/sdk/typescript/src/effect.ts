// @tape.effect-style annotations + RetryPolicy for TypeScript tools.
//
// On JS/TS there are no Python-style decorators by default; you annotate by
// calling `effect(fn, { ...meta })` which returns a wrapped function (with the
// retry loop if `retry` is given) and registers any compensator / status_check.
// In a future ADK-JS adapter (see ./adk.ts), the equivalent of TapePlugin uses
// `effectMetaOf(fn)` to retrieve the annotation.

export interface RetryPolicy {
  maxAttempts: number;
  initialIntervalMs?: number;
  backoffCoefficient?: number;
  maxIntervalMs?: number;
  jitter?: number;
  retryOn?: Array<new (...a: any[]) => Error>;
  nonRetryable?: Array<new (...a: any[]) => Error>;
}

const _compensators = new Map<string, Function>();
const _statusChecks = new Map<string, Function>();

export function registerCompensator(name: string, fn: Function): void { _compensators.set(name, fn); }
export function getCompensator(name: string): Function | undefined { return _compensators.get(name); }
export function registerStatusCheck(toolName: string, fn: Function): void { _statusChecks.set(toolName, fn); }
export function getStatusCheck(toolName: string): Function | undefined { return _statusChecks.get(toolName); }

export interface EffectMeta {
  compensate?: Function;
  statusCheck?: Function;
  keyFrom?: Function;
  compensationPayload?: Function;
  retry?: RetryPolicy;
}

const META = Symbol.for('tape.effect.meta');

export function effectMetaOf(fn: any): EffectMeta | undefined { return fn && fn[META]; }

export function effect<F extends (...a: any[]) => any>(fn: F, meta: EffectMeta = {}): F {
  if (meta.compensate) registerCompensator(meta.compensate.name || 'compensate', meta.compensate);
  if (meta.statusCheck) registerStatusCheck(fn.name, meta.statusCheck);
  if (!meta.retry) {
    (fn as any)[META] = meta;
    return fn;
  }
  const p = meta.retry;
  const shouldRetry = (e: unknown, attempt: number): boolean => {
    if (attempt >= p.maxAttempts) return false;
    if (p.nonRetryable?.some((C) => e instanceof C)) return false;
    return !p.retryOn || p.retryOn.some((C) => e instanceof C);
  };
  const delayFor = (attempt: number): number => {
    const i = p.initialIntervalMs ?? 1000;
    const k = p.backoffCoefficient ?? 2.0;
    const m = p.maxIntervalMs ?? 60_000;
    let d = Math.min(i * Math.pow(k, Math.max(0, attempt - 1)), m);
    const j = p.jitter ?? 0.1;
    if (j > 0) d *= 1 + (Math.random() * 2 - 1) * j;
    return Math.max(0, d);
  };
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
  const wrapped = async function (this: any, ...args: any[]) {
    let attempt = 0;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      attempt++;
      try { return await fn.apply(this, args); }
      catch (e) {
        if (!shouldRetry(e, attempt)) throw e;
        await sleep(delayFor(attempt));
      }
    }
  };
  Object.defineProperty(wrapped, 'name', { value: fn.name });
  (wrapped as any)[META] = meta;
  return wrapped as unknown as F;
}

// Read the idempotency key the plugin stashed in tool_context.state. (In the
// JS port this surface mirrors the Python one — see ../README.md.)
export function idempotencyKey(toolContext: any): string {
  try { return toolContext?.state?.get?.('temp:_tape_idempotency_key') ?? ''; } catch { return ''; }
}
export function runIdOf(toolContext: any): string {
  try { return toolContext?.state?.get?.('temp:_tape_run_id') ?? ''; } catch { return ''; }
}

export class AckLost extends Error { constructor(message = 'ack lost') { super(message); this.name = 'AckLost'; } }
