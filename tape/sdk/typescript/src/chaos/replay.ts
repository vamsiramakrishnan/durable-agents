// Replay — bit-for-bit determinism check. Mirrors `tape.chaos.replay`.

import { TapeClient } from '../client.ts';
import type { Scenario, Session } from './scenarios.ts';
import { session as makeSession } from './scenarios.ts';
import { captureSnapshot } from './snapshot.ts';
import type { Snapshot } from './snapshot.ts';

export interface ReplayReport {
  scenarioName: string;
  seed: number;
  bitIdentical: boolean;
  snapA?: Snapshot;
  snapB?: Snapshot;
  diffSummary: string[];
  notes: string[];
}

function summarize(a: Snapshot, b: Snapshot): string[] {
  const out: string[] = [];
  for (const d of a.diff(b)) {
    if (d.op === '!=' && d.a && d.b) {
      out.push(`[${d.index}] ${d.a.kind} differs:\n    A: ${d.a.payload}\n    B: ${d.b.payload}`);
    } else if (d.op === '>' && d.a) {
      out.push(`[${d.index}] only in A: ${d.a.kind}`);
    } else if (d.op === '<' && d.b) {
      out.push(`[${d.index}] only in B: ${d.b.kind}`);
    }
  }
  return out;
}

/**
 * Run `body(client, session)` twice under `scen` with the same seed and
 * check journal bit-identity. `body` must produce a run — either by
 * returning its runId or calling `sess.setRunId(rid)`.
 *
 * Never throws on divergence; reports it.
 */
export async function replay(
  scen: Scenario,
  body: (client: TapeClient, sess: Session) => Promise<string | void> | string | void,
  opts: { url?: string; deadlineMs?: number } = {},
): Promise<ReplayReport> {
  const url = opts.url ?? 'tape://localhost:7878';
  const deadlineMs = opts.deadlineMs ?? 5_000;
  const report: ReplayReport = {
    scenarioName: scen.name, seed: scen.seed, bitIdentical: false,
    diffSummary: [], notes: [],
  };
  const snapshots: Snapshot[] = [];

  for (const passIdx of [1, 2]) {
    const sess = makeSession(scen, { url });
    await sess.enter();
    const client = new TapeClient(url);
    let rid: string | undefined;
    let thrown: unknown;
    try {
      const returned = await body(client, sess);
      rid = sess.runId ?? (typeof returned === 'string' ? returned : undefined);
    } catch (ex) {
      thrown = ex;
      report.notes.push(`pass ${passIdx} raised: ${ex instanceof Error ? ex.message : String(ex)}`);
    }
    if (!rid) {
      await sess.exit(thrown);
      client.close();
      if (!report.notes.length) {
        report.notes.push(`pass ${passIdx}: body did not produce a runId (set sess.runId or return it)`);
      }
      return report;
    }
    try {
      const snap = await captureSnapshot(client, rid, { deadlineMs });
      snapshots.push(snap);
    } finally {
      await sess.exit(thrown);
      client.close();
    }
  }

  const [a, b] = snapshots;
  report.snapA = a; report.snapB = b;
  report.bitIdentical = a.equals(b);
  if (!report.bitIdentical) report.diffSummary = summarize(a, b);
  return report;
}
