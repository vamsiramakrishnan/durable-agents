// Observability — structured-log helper + OTel span-name constants.
// Mirrors `tape.obs` in Python and `tape/sdk/go/obs.go`.

export const SPAN_BEGIN_RUN = 'tape.begin_run';
export const SPAN_RESUME_RUN = 'tape.resume_run';
export const SPAN_RECORD_DECISION = 'tape.record_decision';
export const SPAN_BEGIN_EFFECT = 'tape.begin_effect';
export const SPAN_COMPLETE_EFFECT = 'tape.complete_effect';
export const SPAN_RECONCILE_EFFECT = 'tape.reconcile_effect';
export const SPAN_DISPATCH_EFFECT = 'tape.dispatch_effect';
export const SPAN_COMPENSATE = 'tape.compensate';
export const SPAN_REDRIVE = 'tape.redrive';
export const SPAN_AWAIT_SIGNAL = 'tape.await_signal';
export const SPAN_SEND_SIGNAL = 'tape.send_signal';

export const ALL_SPANS: readonly string[] = Object.freeze([
  SPAN_BEGIN_RUN, SPAN_RESUME_RUN, SPAN_RECORD_DECISION,
  SPAN_BEGIN_EFFECT, SPAN_COMPLETE_EFFECT,
  SPAN_RECONCILE_EFFECT, SPAN_DISPATCH_EFFECT,
  SPAN_COMPENSATE, SPAN_REDRIVE,
  SPAN_AWAIT_SIGNAL, SPAN_SEND_SIGNAL,
]);

export const STRUCTURED_FIELDS: readonly string[] = Object.freeze([
  'ts', 'level', 'msg',
  'tenant_id', 'app_name', 'run_id', 'invocation_id', 'session_id',
  'seq', 'effect_key', 'decision_index', 'reactor', 'lease_owner',
]);

/** Emit one structured JSON line to stderr in canonical field order. */
export function logJson(msg: string, fields: Record<string, unknown> = {}, level: string = 'INFO'): void {
  const rec: Record<string, unknown> = { ts: Date.now() / 1000, level, msg };
  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined || v === null || v === '') continue;
    rec[k] = v;
  }
  const ordered: Record<string, unknown> = {};
  for (const k of STRUCTURED_FIELDS) if (k in rec) { ordered[k] = rec[k]; delete rec[k]; }
  for (const [k, v] of Object.entries(rec)) ordered[k] = v;
  process.stderr.write(JSON.stringify(ordered) + '\n');
}

export type SpanEnd = (err?: Error) => void;
export type SpanHook = (name: string, attrs: Record<string, unknown>) => SpanEnd;

let _hook: SpanHook | null = null;

/** Install a span hook. Tracing adapters (e.g. opentelemetry) set this. */
export function setSpanHook(h: SpanHook | null): void { _hook = h; }

/** Open a span via the installed hook (no-op if none). */
export function span(name: string, attrs: Record<string, unknown> = {}): SpanEnd {
  if (!_hook) return () => undefined;
  return _hook(name, attrs);
}
