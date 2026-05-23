// `effect` and `outboxTool` — attach Tape metadata to a tool function so a
// hosting framework (or a user's own dispatcher) can recognise it at call
// time and journal accordingly. Mirrors `tape_adk/decorators.py`.
//
// Since TypeScript doesn't have true decorators on plain functions (the
// `@` syntax requires class members), we expose these as higher-order
// wrappers: `effect(fn)` and `outboxTool({…}, fn)`.
//
// Construction-time refusal:
// * `outboxTool` requires `businessKey` AND `connector`. Missing either
//   throws `Error` *immediately* — the bug never makes it past import.

import { EffectDispatchMode, EffectSemantics } from './service.ts';

/** Metadata stamped on a wrapped function. */
export interface EffectMeta {
  semantics: string;
  dispatchMode: string;
  businessKeyFn?: (args: Record<string, unknown>) => string;
  businessKeyStatic?: string;
  connector?: string;
  compensate?: string;
  customKeyFn?: (args: Record<string, unknown>) => string;
}

// We stash the metadata on the function under a Symbol so it doesn't
// collide with user properties.
const TAPE_META = Symbol.for('tape.embedded.effect_meta');

type FnWithMeta<F extends (...a: never[]) => unknown> = F & {
  [TAPE_META]?: EffectMeta;
};

/** Read Tape metadata off a wrapped function. Returns undefined if not Tape-tracked. */
export function metaOf<F extends (...a: never[]) => unknown>(fn: F): EffectMeta | undefined {
  return (fn as FnWithMeta<F>)[TAPE_META];
}

/**
 * Mark a tool as idempotent + inline-journaled. The host records the intent
 * before the call, the result after, and short-circuits on replay.
 *
 * The tool body MUST be safe to call multiple times — if the agent crashes
 * between intent and result, the re-drive will call the body again. The
 * upstream is expected to dedupe via its own idempotency key, or the body
 * itself must be a no-op on repeat.
 *
 * Usage:
 *   const myTool = effect(async (args) => { ... });
 *   const myTool = effect(async (args) => { ... }, { customKey: a => a.id });
 */
export function effect<F extends (...a: never[]) => unknown>(
  fn: F,
  opts: { customKey?: (args: Record<string, unknown>) => string } = {},
): F {
  const meta: EffectMeta = {
    semantics: EffectSemantics.IDEMPOTENT,
    dispatchMode: EffectDispatchMode.INLINE,
    customKeyFn: opts.customKey,
  };
  (fn as FnWithMeta<F>)[TAPE_META] = meta;
  return fn;
}

/**
 * Mark a tool as NON-IDEMPOTENT — its dispatch lives in the outbox.
 *
 * The agent calls the tool conceptually; the host intercepts and journals
 * an intent. The actual upstream call is made later by the outbox dispatcher
 * reactor, against the connector named here.
 *
 * Required (refused at construction time if missing):
 * * `businessKey` — string or callable that derives the upstream's dedupe key
 *   from the tool's args. The (connector, business_key) tuple is UNIQUE
 *   across the journal — no two effects can share it for the same connector.
 * * `connector` — registry name the outbox dispatcher resolves at runtime.
 *
 * Optional:
 * * `compensate` — obligation `kind` to register on duplicate observation.
 * * `customKey` — override the derived idempotency_key.
 *
 * Construction-time refusal: omitting `businessKey` or `connector` throws
 * `Error` here — the bug never makes it past `import`.
 */
export function outboxTool<F extends (...a: never[]) => unknown>(
  opts: {
    businessKey: string | ((args: Record<string, unknown>) => string);
    connector: string;
    compensate?: string;
    customKey?: (args: Record<string, unknown>) => string;
  },
  fn: F,
): F {
  if (!opts.connector) {
    throw new Error(
      'outboxTool: `connector` is required — the outbox dispatcher needs '
      + 'to know which connector to dispatch through.');
  }
  if (opts.businessKey === undefined || opts.businessKey === null) {
    throw new Error(
      'outboxTool: `businessKey` is required — non-idempotent operations '
      + 'must declare the key the upstream uses to dedupe. Pass a string '
      + 'OR a callable that derives it from the tool args.');
  }
  const meta: EffectMeta = {
    semantics: EffectSemantics.NON_IDEMPOTENT,
    dispatchMode: EffectDispatchMode.OUTBOX,
    connector: opts.connector,
    compensate: opts.compensate,
    customKeyFn: opts.customKey,
  };
  if (typeof opts.businessKey === 'string') {
    meta.businessKeyStatic = opts.businessKey;
  } else {
    meta.businessKeyFn = opts.businessKey;
  }
  (fn as FnWithMeta<F>)[TAPE_META] = meta;
  return fn;
}
