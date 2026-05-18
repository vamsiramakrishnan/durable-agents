// Scenarios — declarative bundles of (faults, invariants, seed).
//
// Two layers of faults:
//   * server  — a named failpoint from the Rust catalogue
//                 (`design-principles/chaos.md §5`); rendered into the
//                 `FAILPOINTS` env-var spec by `failpointsEnv()`.
//   * connector — a wrap around a registered connector in `CONNECTORS`.
//
// The session() context applies connector wraps on enter() and restores
// them on exit(), running invariants against the live journal as it goes.

import { TapeClient } from '../client.ts';
import { CONNECTORS } from '../connectors/index.ts';
import type { Connector } from '../connectors/index.ts';
import { ChaosConnector } from './connectors.ts';

export type FaultLayer = 'server' | 'connector';

export interface Fault {
  readonly layer: FaultLayer;
  readonly target: string;        // failpoint name OR connector name
  readonly action: string;        // panic/sleep/return (server) OR lose_ack/duplicate/delay (connector)
  readonly probability: number;
  readonly afterN: number;        // server only: skip first N hits
  readonly ms: number;            // delay length
  readonly jitter: number;
  readonly when: string;          // free-form selector (Phase 2 CEL)
  readonly extra: Record<string, unknown>;
}

function fault(partial: Partial<Fault> & { layer: FaultLayer; target: string; action: string }): Fault {
  return {
    probability: 1.0, afterN: 0, ms: 0, jitter: 0,
    when: '', extra: {},
    ...partial,
  };
}

// ── server-layer faults ────────────────────────────────────────────────────

export function crash(failpoint: string,
                       opts: { probability?: number; afterN?: number; when?: string } = {}): Fault {
  return fault({ layer: 'server', target: failpoint, action: 'panic',
                  probability: opts.probability ?? 1.0,
                  afterN: opts.afterN ?? 0,
                  when: opts.when ?? '' });
}

export function delay(failpoint: string,
                       opts: { ms: number; probability?: number; jitter?: number; when?: string }): Fault {
  return fault({ layer: 'server', target: failpoint, action: 'sleep',
                  ms: opts.ms, jitter: opts.jitter ?? 0,
                  probability: opts.probability ?? 1.0,
                  when: opts.when ?? '' });
}

export function error(failpoint: string,
                       opts: { msg?: string; probability?: number; when?: string } = {}): Fault {
  return fault({ layer: 'server', target: failpoint, action: 'return',
                  probability: opts.probability ?? 1.0,
                  when: opts.when ?? '',
                  extra: { actionMsg: opts.msg ?? 'chaos' } });
}

// ── connector-layer faults ─────────────────────────────────────────────────

export function loseAck(opts: { connector?: string; tool?: string; probability?: number }): Fault {
  const target = opts.connector ?? opts.tool ?? '';
  if (!target) throw new Error('loseAck: requires connector or tool');
  return fault({ layer: 'connector', target, action: 'lose_ack',
                  probability: opts.probability ?? 0.3,
                  when: opts.tool ? `tool == ${JSON.stringify(opts.tool)}` : '' });
}

export function duplicate(opts: { connector?: string; tool?: string; probability?: number }): Fault {
  const target = opts.connector ?? opts.tool ?? '';
  if (!target) throw new Error('duplicate: requires connector or tool');
  return fault({ layer: 'connector', target, action: 'duplicate',
                  probability: opts.probability ?? 0.05,
                  when: opts.tool ? `tool == ${JSON.stringify(opts.tool)}` : '' });
}

export function delayConnector(opts: { connector: string; ms: number; jitter?: number }): Fault {
  return fault({ layer: 'connector', target: opts.connector, action: 'delay',
                  ms: opts.ms, jitter: opts.jitter ?? 0 });
}

// ── Scenario ───────────────────────────────────────────────────────────────

export interface Scenario {
  readonly name: string;
  readonly faults: readonly Fault[];
  readonly invariants: readonly Invariant[];
  readonly seed: number;
}

export interface Invariant {
  name: string;
  check(opts: { client: TapeClient; runId?: string }): Promise<InvariantResult>;
}

export interface InvariantResult {
  name: string;
  passed: boolean;
  detail: string;
}

export function scenario(opts: {
  name: string;
  faults?: readonly Fault[];
  invariants?: readonly Invariant[];
  seed?: number;
}): Scenario {
  return {
    name: opts.name,
    faults: Object.freeze([...(opts.faults ?? [])]),
    invariants: Object.freeze([...(opts.invariants ?? [])]),
    seed: opts.seed ?? 0,
  };
}

// ── FAILPOINTS env rendering ───────────────────────────────────────────────

function toFailSpec(f: Fault): string {
  let action: string = f.action;
  if (action === 'sleep') action = `sleep(${Math.floor(f.ms)})`;
  else if (action === 'return') action = `return(${String(f.extra.actionMsg ?? 'chaos')})`;
  else if (action === 'print') action = `print(${String(f.extra.actionMsg ?? 'chaos')})`;
  const parts: string[] = [];
  if (f.afterN > 0) parts.push(`${f.afterN}*off`);
  if (f.probability > 0 && f.probability < 1) parts.push(`${f.probability}*${action}`);
  else parts.push(action);
  return `${f.target}=${parts.join('->')}`;
}

/**
 * Render the server-layer faults of `scen` into the `FAILPOINTS` env-var
 * value the chaos-feature tape-server parses at startup. Connector-layer
 * faults are applied in-process via `session()`.
 */
export function failpointsEnv(scen: Scenario): string {
  return scen.faults
    .filter(f => f.layer === 'server')
    .map(toFailSpec)
    .join(';');
}

// ── ChaosReport + Session ──────────────────────────────────────────────────

export interface ChaosReport {
  scenarioName: string;
  seed: number;
  failpointsSpec: string;
  passed: boolean;
  invariantResults: InvariantResult[];
  notes: string[];
}

export interface SessionOpts {
  url?: string;
  runId?: string;
}

// Tiny seeded PRNG (mulberry32). Same seed → same stream — what the
// scenario's `seed` controls.
function makeRng(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6D2B79F5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export class Session {
  readonly scenario: Scenario;
  readonly url: string;
  runId?: string;
  readonly failpointsSpec: string;
  readonly report: ChaosReport;
  readonly rng: () => number;
  private restores: Array<() => void> = [];

  constructor(scen: Scenario, opts: SessionOpts = {}) {
    this.scenario = scen;
    this.url = opts.url ?? 'tape://localhost:7878';
    this.runId = opts.runId;
    this.failpointsSpec = failpointsEnv(scen);
    this.rng = scen.seed ? makeRng(scen.seed) : () => Math.random();
    this.report = {
      scenarioName: scen.name, seed: scen.seed,
      failpointsSpec: this.failpointsSpec,
      passed: true, invariantResults: [], notes: [],
    };
  }

  setRunId(rid: string): this { this.runId = rid; return this; }

  /** Apply connector wraps. Call before driving the agent. */
  async enter(): Promise<void> {
    const byTarget = new Map<string, Fault[]>();
    for (const f of this.scenario.faults) {
      if (f.layer !== 'connector') continue;
      const arr = byTarget.get(f.target) ?? [];
      arr.push(f);
      byTarget.set(f.target, arr);
    }
    for (const [name, faults] of byTarget) {
      if (!CONNECTORS.has(name)) {
        this.report.notes.push(`connector fault for ${JSON.stringify(name)} skipped: not registered`);
        continue;
      }
      const real = CONNECTORS.get(name);
      const wrapped = new ChaosConnector(real, faults, this.rng);
      CONNECTORS.replace(name, wrapped);
      this.restores.push(() => CONNECTORS.replace(name, real));
    }
  }

  /** Restore connectors and check invariants against the journal. */
  async exit(thrown?: unknown): Promise<void> {
    for (const fn of [...this.restores].reverse()) {
      try { fn(); } catch { /* swallow */ }
    }
    this.restores = [];
    if (thrown) {
      this.report.passed = false;
      this.report.notes.push(`body raised: ${thrown instanceof Error ? thrown.message : String(thrown)}`);
    }
    const client = new TapeClient(this.url);
    try {
      for (const inv of this.scenario.invariants) {
        let result: InvariantResult;
        try {
          result = await inv.check({ client, runId: this.runId });
        } catch (ex) {
          result = { name: inv.name, passed: false,
                     detail: `check threw: ${ex instanceof Error ? ex.message : String(ex)}` };
        }
        this.report.invariantResults.push(result);
        if (!result.passed) this.report.passed = false;
      }
    } finally {
      client.close();
    }
  }
}

export function session(scen: Scenario, opts: SessionOpts = {}): Session {
  return new Session(scen, opts);
}

/** Run `body(sess)` under `scen`, returning the report. */
export async function runScenario(
  scen: Scenario,
  body: (sess: Session) => Promise<void> | void,
  opts: SessionOpts = {},
): Promise<ChaosReport> {
  const sess = new Session(scen, opts);
  await sess.enter();
  let thrown: unknown;
  try { await body(sess); } catch (e) { thrown = e; }
  await sess.exit(thrown);
  return sess.report;
}
