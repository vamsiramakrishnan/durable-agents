// LDFI — derive the next chaos test from a successful run's lineage.
//
// Mirrors `tape.chaos.lineage`. The breaking-failpoint mapping is in
// lockstep with the Python module so Python-driven LDFI scenarios and
// TS-driven LDFI scenarios produce the same shape.

import type { TapeClient } from '../client.ts';
import type { Fault, Scenario, InvariantResult } from './scenarios.ts';
import { crash, scenario } from './scenarios.ts';

export interface LineageNode {
  seq: number;
  kind: string;          // 'run' | 'decision' | 'effect' | 'obligation' | 'gate' | 'value'
  payload: Record<string, unknown>;
  parentSeq: number;     // 0 = root
  breakingFailpoint: string;
}

export class LineageGraph {
  readonly runId: string;
  readonly nodes: readonly LineageNode[];
  constructor(runId: string, nodes: readonly LineageNode[]) {
    this.runId = runId;
    this.nodes = nodes;
  }

  ofKind(kind: string): LineageNode[] {
    return this.nodes.filter(n => n.kind === kind);
  }

  edges(): Array<[number, number]> {
    return this.nodes.filter(n => n.parentSeq > 0).map(n => [n.parentSeq, n.seq]);
  }

  /**
   * Minimal cuts. At maxSize=1 every node with a breaking_failpoint is
   * its own cut (the singleton case — Python's ships this for v1).
   * maxSize >= 2 enumerates pairs (combinatorial; opt-in).
   */
  minimalCuts(opts: { maxSize?: number } = {}): LineageNode[][] {
    const maxSize = opts.maxSize ?? 1;
    const candidates = this.nodes.filter(n => n.breakingFailpoint);
    const cuts: LineageNode[][] = candidates.map(n => [n]);
    if (maxSize >= 2) {
      for (let i = 0; i < candidates.length; i++) {
        for (let j = i + 1; j < candidates.length; j++) {
          if (candidates[i].breakingFailpoint === candidates[j].breakingFailpoint) continue;
          cuts.push([candidates[i], candidates[j]]);
        }
      }
    }
    return cuts;
  }

  static async fromRun(client: TapeClient, runId: string,
                       opts: { deadlineMs?: number } = {}): Promise<LineageGraph> {
    const deadlineMs = opts.deadlineMs ?? 5_000;
    const terminalStatuses = new Set(['terminal', 'failed', 'cancelled', 'stuck']);
    const nodes: LineageNode[] = [];
    const decisionSeqs = new Map<number, number>();
    const effectSeqs = new Map<string, number>();
    const gateSeqs = new Map<string, number>();
    const start = Date.now();

    const stream = client.subscribeRun({ runId, fromSeq: 0, timeoutMs: deadlineMs });
    for await (const entry of stream) {
      let payload: any;
      try { payload = JSON.parse((entry as any).payloadJson ?? '{}'); }
      catch { payload = { _raw: (entry as any).payloadJson }; }
      let parent = 0;
      let bp = '';
      const kind: string = (entry as any).kind;
      const seq: number = (entry as any).seq;

      if (kind === 'run') {
        bp = payload.status === 'running'
          ? 'tape::begin_run::post_db'
          : 'tape::end_run::post_db';
      } else if (kind === 'decision') {
        const idx = Number(payload.decision_index ?? -1);
        decisionSeqs.set(idx, seq);
        parent = decisionSeqs.get(idx - 1) ?? 0;
        bp = 'tape::record_decision::post_db';
      } else if (kind === 'effect') {
        const idx = Number(payload.decision_index ?? -1);
        parent = decisionSeqs.get(idx) ?? 0;
        const key = String(payload.idempotency_key ?? '');
        const status = String(payload.status ?? '').toLowerCase();
        if (key) {
          if (status === 'pending') {
            if (!effectSeqs.has(key)) effectSeqs.set(key, seq);
            bp = 'tape::begin_effect::post_db';
          } else if (status === 'confirmed') {
            bp = 'tape::complete_effect::post_db';
          } else if (status === 'failed' || status === 'unknown' || status === 'reconciled') {
            bp = 'tape::reconcile_effect::post_db';
          } else {
            bp = 'tape::begin_effect::post_db';
          }
        }
      } else if (kind === 'obligation') {
        parent = effectSeqs.get(String(payload.effect_key ?? '')) ?? 0;
        const status = String(payload.status ?? '').toLowerCase();
        bp = (status === 'compensated' || status === 'stuck')
          ? 'tape::resolve_obligation::post_db'
          : 'tape::register_compensation::post_db';
      } else if (kind === 'gate') {
        const gate = String(payload.gate ?? '');
        if (gate && !gateSeqs.has(gate)) gateSeqs.set(gate, seq);
        const status = String(payload.status ?? '').toLowerCase();
        bp = (status === 'delivered' || status === 'resolved')
          ? 'tape::send_signal::post_db'
          : 'tape::await_signal::post_db';
      } else if (kind === 'value') {
        bp = payload.deleted ? 'tape::delete_value::post_db' : 'tape::write_value::post_db';
      }

      nodes.push({ seq, kind, payload, parentSeq: parent, breakingFailpoint: bp });

      if (kind === 'run' && terminalStatuses.has(String(payload.status ?? '').toLowerCase())) break;
      if (Date.now() - start > deadlineMs) break;
    }
    return new LineageGraph(runId, nodes);
  }
}

import type { Invariant } from './scenarios.ts';

/** Translate every minimal cut of `graph` into a `Scenario`. */
export function deriveScenarios(graph: LineageGraph,
                                  opts: { invariants?: readonly Invariant[];
                                            maxCutSize?: number; baseName?: string } = {}): Scenario[] {
  const inv = opts.invariants ?? [];
  const baseName = opts.baseName ?? 'ldfi';
  const out: Scenario[] = [];
  for (const cut of graph.minimalCuts({ maxSize: opts.maxCutSize ?? 1 })) {
    const faults: Fault[] = [];
    const names: string[] = [];
    for (const node of cut) {
      faults.push(crash(node.breakingFailpoint, { afterN: 1, probability: 1.0 }));
      names.push(`${node.kind}@${node.seq}`);
    }
    out.push(scenario({
      name: `${baseName}::cut::${names.join('+')}`,
      faults, invariants: inv,
    }));
  }
  return out;
}

// ── LDFI loop ──────────────────────────────────────────────────────────────

export interface LDFIReport {
  baselineRunId: string;
  derivedCount: number;
  survivedCount: number;
  brokenScenarios: Array<{ name: string; failed: InvariantResult[] }>;
  survivalRate: number;
}

/**
 * Drive `runner(scen)` once per derived scenario and aggregate the
 * invariant outcomes. `runner` returns an object with `invariantResults`.
 */
export async function ldfiRunAll(
  derived: readonly Scenario[],
  runner: (scen: Scenario) => Promise<{ invariantResults: InvariantResult[] }>,
  opts: { baselineRunId?: string } = {},
): Promise<LDFIReport> {
  const rep: LDFIReport = {
    baselineRunId: opts.baselineRunId ?? '',
    derivedCount: derived.length,
    survivedCount: 0,
    brokenScenarios: [],
    survivalRate: 1.0,
  };
  for (const scen of derived) {
    const result = await runner(scen);
    const irs = result.invariantResults ?? [];
    if (irs.every(ir => ir.passed)) {
      rep.survivedCount++;
    } else {
      rep.brokenScenarios.push({
        name: scen.name,
        failed: irs.filter(ir => !ir.passed),
      });
    }
  }
  rep.survivalRate = rep.derivedCount === 0 ? 1.0 : rep.survivedCount / rep.derivedCount;
  return rep;
}
