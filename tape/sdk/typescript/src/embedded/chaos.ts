// Chaos / fault-injection for the embedded (tape-ts) tier.
//
// Mirrors `tape_adk/chaos.py` against the TS `TapeSessionService`. Where
// the gRPC chaos package (`src/chaos/`) targets the Rust tape-server's
// failpoints + the global CONNECTORS registry, this module targets the
// in-process Connector dict the embedded reactor loop dispatches through.
//
// Surface (the single mechanism, applied at three composable layers):
//
//   * `Fault`  — data describing one fault. Same shape as the SDK.
//   * `loseAck({connector|tool, probability})`, `duplicate({...})`,
//     `delayConnector({connector, ms, jitter})` — fault constructors.
//   * `ChaosConnector(inner, faults, rng)` — the actual wrapper: speaks
//     the embedded `Connector` protocol, decorates `inner` with `faults`.
//   * `Invariant` + `noStuckObligations`, `exactlyOne({connector|tool})`,
//     `noBlindNonIdempotentRetry` — predicates that read the embedded
//     SQL tables directly (no gRPC client).
//   * `Scenario` — `{name, faults, invariants, seed, strictFaults}`.
//   * `chaosRun(scen, body, {connectors, svc})` — open a session, run
//     `body(connectors)`, then check invariants. The session() generator
//     is also exported for callers who want explicit lifecycle.
//
// The orchestration is the mechanism: opening a session ATOMICALLY
// validates that every declared connector-targeted fault has a connector
// to attach to (with `strictFaults: true`, the default — same as the
// Python SDK and the gRPC SDK). No silent-skip false-positives.
//
// Same logical schema as `tape_adk.chaos`; the wire format is the
// embedded SQL store rather than gRPC.

import type {
  CompensationResult,
  Connector,
  DispatchResult,
  ObservationResult,
} from './connectors.ts';
import {
  EffectSemantics,
  EffectStatus,
  ObligationStatus,
  type EffectRecord,
  type ObligationRecord,
  type TapeSessionService,
} from './service.ts';

// ── data: Fault + Scenario (same shape as src/chaos/scenarios.ts) ─────────

const LAYER_CONNECTOR = 'connector' as const;

export interface Fault {
  readonly layer: 'connector';
  readonly target: string; // connector name when target-scoped
  readonly tool: string;   // tool name when tool-scoped
  readonly action: 'lose_ack' | 'duplicate' | 'delay';
  readonly probability: number;
  readonly ms: number;
  readonly jitter: number;
}

/** Dispatch returns CONFIRMED → flipped to UNKNOWN. Pass `connector` or
 *  `tool`, not both. */
export function loseAck(opts: {
  connector?: string;
  tool?: string;
  probability?: number;
}): Fault {
  if (opts.connector && opts.tool) {
    throw new Error('loseAck: pass connector or tool, not both');
  }
  if (!opts.connector && !opts.tool) {
    throw new Error('loseAck requires connector or tool');
  }
  return {
    layer: LAYER_CONNECTOR,
    target: opts.connector ?? '',
    tool: opts.tool ?? '',
    action: 'lose_ack',
    probability: opts.probability ?? 0.3,
    ms: 0,
    jitter: 0,
  };
}

/** observe() returns DUPLICATE — the reconciler should register a
 *  compensation. */
export function duplicate(opts: {
  connector?: string;
  tool?: string;
  probability?: number;
}): Fault {
  if (opts.connector && opts.tool) {
    throw new Error('duplicate: pass connector or tool, not both');
  }
  if (!opts.connector && !opts.tool) {
    throw new Error('duplicate requires connector or tool');
  }
  return {
    layer: LAYER_CONNECTOR,
    target: opts.connector ?? '',
    tool: opts.tool ?? '',
    action: 'duplicate',
    probability: opts.probability ?? 0.05,
    ms: 0,
    jitter: 0,
  };
}

/** Sleep `ms` (± `jitter` as a fraction) before dispatch. */
export function delayConnector(opts: {
  connector: string;
  ms: number;
  jitter?: number;
}): Fault {
  return {
    layer: LAYER_CONNECTOR,
    target: opts.connector,
    tool: '',
    action: 'delay',
    probability: 1.0,
    ms: opts.ms,
    jitter: opts.jitter ?? 0,
  };
}

// ── seeded PRNG (mulberry32) — same shape as src/chaos/scenarios.ts ───────

function makeRng(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ── wrapper: ChaosConnector ────────────────────────────────────────────────

/**
 * A `Connector` that decorates `inner` with `faults`.
 *
 * Same semantics as `tape_adk.chaos.ChaosConnector`:
 *
 *   * `lose_ack`  — dispatch's CONFIRMED becomes UNKNOWN. The inner call
 *                   already landed; the wrapper hides the ack.
 *   * `duplicate` — observe()'s result becomes DUPLICATE.
 *   * `delay`     — dispatch sleeps `ms` (± `jitter`) before the inner call.
 *
 * A seeded RNG is the only mutable thread of nondeterminism; a seeded
 * scenario is reproducible.
 */
export class ChaosConnector implements Connector {
  readonly inner: Connector;
  readonly faults: readonly Fault[];
  private readonly rng: () => number;

  constructor(inner: Connector, faults: readonly Fault[], rng?: () => number) {
    this.inner = inner;
    this.faults = faults;
    this.rng = rng ?? Math.random;
  }

  get name(): string {
    return this.inner.name ?? '';
  }

  private matching(kind: Fault['action'], effect?: EffectRecord): Fault | null {
    for (const f of this.faults) {
      if (f.action !== kind) continue;
      if (f.tool && effect) {
        if (effect.toolName !== f.tool) continue;
      }
      if (f.probability >= 1.0 || this.rng() < f.probability) {
        return f;
      }
    }
    return null;
  }

  async dispatch(effect: EffectRecord): Promise<DispatchResult> {
    // delay → before inner.
    const d = this.matching('delay', effect);
    if (d && d.ms > 0) {
      let jitterFactor = 1.0;
      if (d.jitter > 0) {
        jitterFactor = 1.0 + (this.rng() * 2 - 1) * d.jitter;
      }
      const ms = Math.max(0, d.ms * jitterFactor);
      await new Promise((res) => setTimeout(res, ms));
    }

    const result = await this.inner.dispatch(effect);

    // lose_ack → CONFIRMED → UNKNOWN (inner already wrote to the upstream).
    if (result.status === 'confirmed' && this.matching('lose_ack', effect)) {
      return {
        status: 'unknown',
        externalRef: result.externalRef,
        response: result.response,
        error: { reason: 'tape-ts.chaos: simulated lost ack' },
      };
    }
    return result;
  }

  async observe(effect: EffectRecord): Promise<ObservationResult> {
    const result = await this.inner.observe(effect);
    if (result.status === 'confirmed' && this.matching('duplicate', effect)) {
      return {
        status: 'duplicate',
        externalRef: result.externalRef,
        response: result.response,
        compensateKind: result.compensateKind ?? '',
      };
    }
    return result;
  }

  async compensate(obligation: ObligationRecord): Promise<CompensationResult> {
    // compensate() is the cleanup path; we don't decorate it.
    return await this.inner.compensate(obligation);
  }
}

// ── invariants: read the embedded tables directly ──────────────────────────

export interface InvariantResult {
  name: string;
  passed: boolean;
  detail: string;
}

function fmtResult(r: InvariantResult): string {
  const mark = r.passed ? 'OK ' : 'FAIL';
  return r.detail ? `[${mark}] ${r.name}: ${r.detail}` : `[${mark}] ${r.name}`;
}

/**
 * Predicate over the embedded journal. Subclasses set `name` and
 * implement `check`. Calling a parameter-free invariant returns `self`
 * — lets users write either `noStuckObligations` or `noStuckObligations()`
 * (same fix as the Python SDK's `Invariant.__call__`).
 */
export abstract class Invariant {
  abstract name: string;
  abstract check(svc: TapeSessionService): Promise<InvariantResult>;

  // Make instances callable: `noStuckObligations()` returns the same
  // singleton. Implemented by exporting a callable wrapper below.
}

class NoStuckObligationsInvariant extends Invariant {
  name = 'no_stuck_obligations';
  async check(svc: TapeSessionService): Promise<InvariantResult> {
    // Read the obligations table directly via the service's db.
    // (TapeSessionService has a public `db` field per service.ts.)
    // The four embedded tables are created lazily on the first mutating
    // call; for read-only invariants on a brand-new service, force the
    // DDL first.
    await ensureTables(svc);
    const rows = svc.db.prepare(`
      SELECT seq, kind FROM tape_obligations WHERE status = ?
    `).all(ObligationStatus.STUCK) as Array<{ seq: number; kind: string }>;
    if (rows.length === 0) {
      return { name: this.name, passed: true, detail: '0 stuck' };
    }
    const sample = rows.slice(0, 5).map((r) => `seq=${r.seq} kind=${r.kind}`).join(', ');
    return {
      name: this.name,
      passed: false,
      detail: `${rows.length} stuck: ${sample}`,
    };
  }
}

class NoBlindNonIdempotentRetryInvariant extends Invariant {
  name = 'no_blind_non_idempotent_retry';
  async check(svc: TapeSessionService): Promise<InvariantResult> {
    await ensureTables(svc);
    const rows = svc.db.prepare(`
      SELECT idempotency_key FROM tape_effects
       WHERE semantics = ? AND status = ? AND dispatch_attempts > 1
    `).all(EffectSemantics.NON_IDEMPOTENT, EffectStatus.PENDING) as Array<{ idempotency_key: string }>;
    if (rows.length === 0) {
      return { name: this.name, passed: true, detail: '0 violators' };
    }
    return {
      name: this.name,
      passed: false,
      detail: `${rows.length} NON_IDEMPOTENT effects retried while PENDING`,
    };
  }
}

class ExactlyOneInvariant extends Invariant {
  name: string;
  readonly connector: string;
  readonly tool: string;
  constructor(connector: string, tool: string) {
    super();
    this.connector = connector;
    this.tool = tool;
    this.name =
      'exactly_one' +
      (connector ? `(connector=${JSON.stringify(connector)})`
                 : tool ? `(tool=${JSON.stringify(tool)})` : '');
  }
  async check(svc: TapeSessionService): Promise<InvariantResult> {
    await ensureTables(svc);
    let sql = `SELECT COUNT(*) AS n FROM tape_effects WHERE status = ?`;
    const params: unknown[] = [EffectStatus.CONFIRMED];
    if (this.connector) {
      sql += ` AND connector = ?`;
      params.push(this.connector);
    }
    if (this.tool) {
      sql += ` AND tool_name = ?`;
      params.push(this.tool);
    }
    const row = svc.db.prepare(sql).get(...params) as { n: number };
    const n = Number(row?.n ?? 0);
    if (n === 1) return { name: this.name, passed: true, detail: '1 confirmed' };
    return {
      name: this.name,
      passed: false,
      detail: `${n} confirmed (expected 1)`,
    };
  }
}

// ── callable singletons + factories ────────────────────────────────────────

/**
 * Make a parameter-free invariant *callable* — `noStuckObligations()`
 * returns the same singleton instance, exactly like Python's
 * `Invariant.__call__`. Calling with args is a TypeError with a clear
 * message — same uniformity guarantee as the Python SDK.
 */
function makeCallable<T extends Invariant>(inv: T): T & ((...args: unknown[]) => T) {
  // Pre-build a placeholder; the proxy captures `proxy` lexically so a
  // no-arg call returns the proxy itself (Python's `Invariant.__call__`
  // returns `self`; `bare is called` works the same way here).
  // eslint-disable-next-line prefer-const
  let proxy: T & ((...args: unknown[]) => T);
  const fn = function (this: unknown, ...args: unknown[]): T {
    if (args.length > 0) {
      throw new TypeError(`${inv.constructor.name} takes no construction arguments`);
    }
    return proxy;
  } as unknown as T & ((...args: unknown[]) => T);
  // Re-route property lookups to the original instance so `fn.name` and
  // `fn.check(...)` both work — function objects have a read-only `name`,
  // so `Object.assign` can't copy `inv.name` onto `fn` directly.
  proxy = new Proxy(fn, {
    get(target, prop, receiver) {
      if (prop in (inv as object)) {
        const val = (inv as Record<string | symbol, unknown>)[prop as string | symbol];
        if (typeof val === 'function') {
          return (val as Function).bind(inv);
        }
        return val;
      }
      return Reflect.get(target, prop, receiver);
    },
  });
  return proxy;
}

export const noStuckObligations = makeCallable(new NoStuckObligationsInvariant());
export const noBlindNonIdempotentRetry = makeCallable(new NoBlindNonIdempotentRetryInvariant());

export function exactlyOne(opts: { connector?: string; tool?: string }): Invariant {
  if (opts.connector && opts.tool) {
    throw new Error('exactlyOne: pass connector or tool, not both');
  }
  if (!opts.connector && !opts.tool) {
    throw new Error('exactlyOne requires connector or tool');
  }
  return new ExactlyOneInvariant(opts.connector ?? '', opts.tool ?? '');
}

// ── ensure tables exist before read-only invariants query them ────────────

async function ensureTables(svc: TapeSessionService): Promise<void> {
  // Force a no-op create on the lazy DDL. Calling `getEffect` against
  // a (very likely absent) key triggers `ensureTables()` inside the
  // service. Same logical effect as Python's `_prepare_tables()`.
  await svc.getEffect({
    appName: '__chaos__', userId: '__chaos__', sessionId: '__chaos__',
    idempotencyKey: '__chaos__',
  });
}

// ── Scenario + Report + Session ────────────────────────────────────────────

export interface Scenario {
  readonly name: string;
  readonly faults: readonly Fault[];
  readonly invariants: readonly Invariant[];
  readonly seed: number;
  readonly strictFaults: boolean;
}

export function scenario(opts: {
  name: string;
  faults?: readonly Fault[];
  invariants?: readonly Invariant[];
  seed?: number;
  strictFaults?: boolean;
}): Scenario {
  return {
    name: opts.name,
    faults: Object.freeze([...(opts.faults ?? [])]),
    invariants: Object.freeze([...(opts.invariants ?? [])]),
    seed: opts.seed ?? 0,
    strictFaults: opts.strictFaults ?? true,
  };
}

export interface ChaosReport {
  scenarioName: string;
  seed: number;
  passed: boolean;
  invariantResults: InvariantResult[];
  notes: string[];
}

function reportToString(r: ChaosReport): string {
  const head = `ChaosReport(${JSON.stringify(r.scenarioName)}: ${r.passed ? 'pass' : 'FAIL'}, seed=${r.seed})`;
  const body = r.invariantResults.map((ir) => `  - ${fmtResult(ir)}`).join('\n');
  const notes = r.notes.map((n) => `  ! ${n}`).join('\n');
  return [head, body, notes].filter(Boolean).join('\n');
}

export interface Session {
  /** Connectors dict with chaos wrappers applied where targeted. */
  readonly connectors: Record<string, Connector>;
  /** Report — finalised on `close()`. */
  readonly report: ChaosReport;
}

// ── open / close session ───────────────────────────────────────────────────

function recordSkip(report: ChaosReport, scen: Scenario, message: string): void {
  // A declared fault couldn't be applied. Note always; under strict,
  // also fail the scenario via a synthetic `strict_faults` invariant
  // result. Same mechanism as `tape_adk/chaos.py`.
  report.notes.push(message);
  if (scen.strictFaults) {
    report.invariantResults.push({
      name: 'strict_faults', passed: false, detail: message,
    });
    report.passed = false;
  }
}

function openSession(
  scen: Scenario,
  connectors: Record<string, Connector>,
): Session {
  const rng = scen.seed ? makeRng(scen.seed) : Math.random;
  const report: ChaosReport = {
    scenarioName: scen.name, seed: scen.seed,
    passed: true, invariantResults: [], notes: [],
  };
  const wrapped: Record<string, Connector> = { ...connectors };

  const byConnector = new Map<string, Fault[]>();
  const toolScoped: Fault[] = [];
  for (const f of scen.faults) {
    if (f.layer !== LAYER_CONNECTOR) {
      recordSkip(report, scen,
        `fault layer ${JSON.stringify(f.layer)} not supported in embedded tier ` +
        `(server failpoints require the gRPC tier)`);
      continue;
    }
    if (f.target) {
      const arr = byConnector.get(f.target) ?? [];
      arr.push(f);
      byConnector.set(f.target, arr);
    } else if (f.tool) {
      toolScoped.push(f);
    } else {
      recordSkip(report, scen,
        'connector fault skipped: neither target nor tool set');
    }
  }

  for (const [name, faults] of byConnector) {
    if (!(name in connectors)) {
      recordSkip(report, scen,
        `connector fault for ${JSON.stringify(name)} skipped: ` +
        `connector not in \`connectors\` dict`);
      continue;
    }
    wrapped[name] = new ChaosConnector(
      connectors[name],
      [...faults, ...toolScoped],
      rng,
    );
  }
  if (toolScoped.length > 0) {
    const keys = Object.keys(connectors);
    if (keys.length === 0) {
      recordSkip(report, scen,
        'tool-scoped fault(s) skipped: empty `connectors` dict');
    }
    for (const name of keys) {
      if (byConnector.has(name)) continue;
      wrapped[name] = new ChaosConnector(
        connectors[name],
        [...toolScoped],
        rng,
      );
    }
  }

  return { connectors: wrapped, report };
}

async function closeSession(
  sess: Session,
  scen: Scenario,
  svc: TapeSessionService,
): Promise<void> {
  for (const inv of scen.invariants) {
    let ir: InvariantResult;
    try {
      ir = await inv.check(svc);
    } catch (ex) {
      ir = {
        name: inv.name ?? '<unnamed>',
        passed: false,
        detail: `raised: ${ex instanceof Error ? ex.message : String(ex)}`,
      };
    }
    sess.report.invariantResults.push(ir);
    if (!ir.passed) (sess.report as { passed: boolean }).passed = false;
  }
}

/**
 * One-shot convenience: open a session, call `body(connectors)`, then
 * run the invariants against `svc` and return the report.
 *
 * The TS analog of Python's `async with chaos.session(scen, svc=…) as
 * sess: await body(sess.connectors)` — TS doesn't have native async
 * context managers, so we use a body callback.
 */
export async function chaosRun(
  scen: Scenario,
  body: ((connectors: Record<string, Connector>) => Promise<void> | void) | null,
  opts: {
    svc: TapeSessionService;
    connectors: Record<string, Connector>;
  },
): Promise<ChaosReport> {
  const sess = openSession(scen, opts.connectors);
  try {
    if (body) await body(sess.connectors);
  } finally {
    await closeSession(sess, scen, opts.svc);
  }
  return sess.report;
}

/**
 * Explicit-lifecycle variant. Returns a `{session, finish()}` pair —
 * call `finish()` after running your body to evaluate invariants.
 * Mirrors the underlying primitive Python's `session()` context
 * manager exposes.
 */
export function openChaosSession(
  scen: Scenario,
  opts: { connectors: Record<string, Connector> },
): { session: Session; finish: (svc: TapeSessionService) => Promise<ChaosReport> } {
  const sess = openSession(scen, opts.connectors);
  return {
    session: sess,
    finish: async (svc: TapeSessionService) => {
      await closeSession(sess, scen, svc);
      return sess.report;
    },
  };
}

export { reportToString, fmtResult };
