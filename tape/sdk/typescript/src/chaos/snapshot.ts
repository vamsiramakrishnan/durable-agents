// Snapshot — capture a run's journal + canonicalise for equality checks.
//
// Mirrors `tape.chaos.snapshot` from the Python SDK. The canonicalisation
// rules (strip timestamps, remap run-scoped identifiers) are kept in
// lockstep with the Python side so a Python-driven replay and a
// TS-driven replay are comparable.

import type { TapeClient } from '../client.ts';

const STRIP_KEYS: ReadonlySet<string> = new Set([
  'ts_ms', 'started_at_ms', 'ended_at_ms', 'last_update_time_ms',
  'lease_expires_at_ms', 'claim_expires_at_ms', 'dispatch_claim_expires_at_ms',
  'next_dispatch_at_ms', 'next_attempt_at_ms', 'fire_at_ms',
  'lease_owner', 'claimed_by', 'dispatch_claimed_by',
  'trace_id', 'span_id', 'parent_span_id',
  'seq', 'global_seq',
  // invocation_id varies per pass; remapping is sufficient for replay
  // semantics (matches the Python rule).
  'invocation_id',
]);

const TERMINAL_RUN_STATUSES: ReadonlySet<string> = new Set([
  'terminal', 'failed', 'cancelled', 'stuck',
]);

function canonical(value: unknown, runIdMap: Map<string, string>): unknown {
  if (value === null || value === undefined) return value;
  if (Array.isArray(value)) return value.map(v => canonical(v, runIdMap));
  if (typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      if (STRIP_KEYS.has(k)) continue;
      out[k] = canonical(v, runIdMap);
    }
    return out;
  }
  if (typeof value === 'string') {
    let s = value;
    for (const [raw, replacement] of runIdMap) {
      if (raw && s.includes(raw)) s = s.split(raw).join(replacement);
    }
    return s;
  }
  return value;
}

function stableStringify(value: unknown): string {
  if (value === null || value === undefined) return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  if (typeof value === 'object') {
    const keys = Object.keys(value as object).sort();
    return `{${keys.map(k => `${JSON.stringify(k)}:${stableStringify((value as any)[k])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

export interface JournalLine {
  kind: string;
  payload: string;             // stable, sort-key-encoded JSON of canonical payload
}

export interface Snapshot {
  runId: string;
  lines: readonly JournalLine[];
  diff(other: Snapshot): Array<{ index: number; op: '==' | '!=' | '<' | '>'; a?: JournalLine; b?: JournalLine }>;
  equals(other: Snapshot): boolean;
}

function makeSnapshot(runId: string, lines: JournalLine[]): Snapshot {
  return {
    runId, lines: Object.freeze(lines),
    equals(other) {
      if (other.lines.length !== this.lines.length) return false;
      for (let i = 0; i < this.lines.length; i++) {
        if (this.lines[i].kind !== other.lines[i].kind) return false;
        if (this.lines[i].payload !== other.lines[i].payload) return false;
      }
      return true;
    },
    diff(other) {
      const out: Array<{ index: number; op: '==' | '!=' | '<' | '>'; a?: JournalLine; b?: JournalLine }> = [];
      const n = Math.max(this.lines.length, other.lines.length);
      for (let i = 0; i < n; i++) {
        const a = this.lines[i]; const b = other.lines[i];
        if (!a) out.push({ index: i, op: '<', b });
        else if (!b) out.push({ index: i, op: '>', a });
        else if (a.kind !== b.kind || a.payload !== b.payload) out.push({ index: i, op: '!=', a, b });
      }
      return out;
    },
  };
}

/**
 * Stream the journal for `runId` via SubscribeRun, canonicalise, and
 * stop at the first terminal `run` entry (or `deadlineMs`).
 */
export async function captureSnapshot(client: TapeClient, runId: string,
                                       opts: { deadlineMs?: number; canonicalRunId?: string } = {}): Promise<Snapshot> {
  const deadlineMs = opts.deadlineMs ?? 5_000;
  const runIdMap = new Map<string, string>([[runId, opts.canonicalRunId ?? 'run-1']]);
  const lines: JournalLine[] = [];
  const start = Date.now();

  const stream = client.subscribeRun({ runId, fromSeq: 0, timeoutMs: deadlineMs });
  try {
    for await (const entry of stream) {
      let payload: unknown;
      try { payload = JSON.parse((entry as any).payloadJson ?? '{}'); }
      catch { payload = { _raw: (entry as any).payloadJson }; }
      const canon = canonical(payload, runIdMap);
      lines.push({ kind: (entry as any).kind, payload: stableStringify(canon) });
      if ((entry as any).kind === 'run') {
        const status = String((payload as any)?.status ?? '').toLowerCase();
        if (TERMINAL_RUN_STATUSES.has(status)) break;
      }
      if (Date.now() - start > deadlineMs) break;
    }
  } finally {
    // The async iterator's finally already cancels the gRPC stream.
  }
  return makeSnapshot(runId, lines);
}

// ── DeepSnapshot ───────────────────────────────────────────────────────────

export interface DeepSnapshot {
  runId: string;
  decisions: readonly string[];   // canonicalised
  effects: readonly string[];
  obligations: readonly string[];
  equals(other: DeepSnapshot): boolean;
}

function canonField(d: Record<string, unknown>, runIdMap: Map<string, string>): string {
  return stableStringify(canonical(d, runIdMap));
}

/**
 * Walks the full projection tables — decisions by index, effects by
 * journal+GetEffect, obligations via ListObligations. Catches body-level
 * drift that the summary Snapshot misses.
 */
export async function captureDeep(client: TapeClient, runId: string,
                                   opts: { canonicalRunId?: string; maxDecisions?: number } = {}): Promise<DeepSnapshot> {
  const runIdMap = new Map<string, string>([[runId, opts.canonicalRunId ?? 'run-1']]);
  const maxDecisions = opts.maxDecisions ?? 1_000;

  // Decisions
  const decisions: string[] = [];
  for (let i = 0; i < maxDecisions; i++) {
    try {
      const got: any = await client.getDecision({ runId, decisionIndex: i });
      if (!got.found) break;
      const d = got.decision;
      decisions.push(canonField({
        decision_index: d.decisionIndex, model: d.model,
        request_json: d.requestJson, response_json: d.responseJson,
        policy_version: d.policyVersion, rationale: d.rationale,
      }, runIdMap));
    } catch { break; }
  }

  // Effects — walk journal once to collect keys, then GetEffect each.
  const seen = new Set<string>();
  const stream = client.subscribeRun({ runId, fromSeq: 0, timeoutMs: 3_000 });
  for await (const entry of stream) {
    if ((entry as any).kind === 'effect') {
      let p: any; try { p = JSON.parse((entry as any).payloadJson ?? '{}'); } catch { continue; }
      const k = String(p.idempotency_key ?? '');
      if (k) seen.add(k);
    } else if ((entry as any).kind === 'run') {
      let p: any; try { p = JSON.parse((entry as any).payloadJson ?? '{}'); } catch { p = {}; }
      if (TERMINAL_RUN_STATUSES.has(String(p.status ?? '').toLowerCase())) break;
    }
  }
  const effects: string[] = [];
  for (const key of [...seen].sort()) {
    try {
      const got: any = await client.getEffect({ runId, idempotencyKey: key });
      if (!got.found) continue;
      const e = got.effect;
      effects.push(canonField({
        tool_name: e.toolName, idempotency_key: e.idempotencyKey,
        status: e.status, request_json: e.requestJson,
        response_json: e.responseJson, error_json: e.errorJson,
        semantics: e.semantics, dispatch_mode: e.dispatchMode,
        business_key: e.businessKey, connector: e.connector,
        external_ref: e.externalRef, decision_index: e.decisionIndex,
      }, runIdMap));
    } catch { /* skip */ }
  }

  // Obligations
  const obligations: string[] = [];
  try {
    const resp: any = await client.listObligations({ runId, onlyUnresolved: false });
    for (const o of resp.obligations ?? []) {
      obligations.push(canonField({
        kind: o.kind, effect_key: o.effectKey, status: o.status,
        payload_json: o.payloadJson, attempts: o.attempts,
        max_attempts: o.maxAttempts, last_error: o.lastError,
        result_json: o.resultJson, compensator_ref: o.compensatorRef,
      }, runIdMap));
    }
  } catch { /* skip */ }

  const dec = Object.freeze(decisions);
  const eff = Object.freeze(effects);
  const obl = Object.freeze(obligations);
  const eqStr = (a: readonly string[], b: readonly string[]) =>
    a.length === b.length && a.every((x, i) => x === b[i]);
  return {
    runId,
    decisions: dec,
    effects: eff,
    obligations: obl,
    equals(other: DeepSnapshot) {
      return eqStr(dec, other.decisions)
          && eqStr(eff, other.effects)
          && eqStr(obl, other.obligations);
    },
  };
}
