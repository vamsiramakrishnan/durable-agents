import { appendFile, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import type {
  Connector, EffectRecord, ObligationRecord,
  DispatchResult, ObservationResult, CompensationResult,
} from './index.ts';

/**
 * Append each dispatch/observe/compensate as a JSON line. Useful for
 * tests, demos, and the non-idempotent-bank example.
 */
export class LogConnector implements Connector {
  readonly name = 'log';
  private readonly path: string;
  private initialized = false;

  constructor(path: string = '/tmp/tape-outbox.jsonl') { this.path = path; }

  private async ensure(): Promise<void> {
    if (this.initialized) return;
    try { await mkdir(dirname(this.path), { recursive: true }); } catch { /* fine */ }
    this.initialized = true;
  }

  private async append(kind: string, body: unknown): Promise<void> {
    await this.ensure();
    const line = JSON.stringify({ kind, ts_ms: Date.now(), body }) + '\n';
    await appendFile(this.path, line);
  }

  async dispatch(effect: EffectRecord): Promise<DispatchResult> {
    await this.append('dispatch', effect);
    return { outcome: 'confirmed', response: { logged: true } };
  }
  async observe(effect: EffectRecord): Promise<ObservationResult> {
    await this.append('observe', effect);
    return { outcome: 'confirmed', count: 1 };
  }
  async compensate(obligation: ObligationRecord): Promise<CompensationResult> {
    await this.append('compensate', obligation);
    return { outcome: 'compensated' };
  }
}
