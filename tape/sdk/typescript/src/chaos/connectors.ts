// ChaosConnector — declarative fault wrap around any registered connector.
// Mirrors `tape.chaos.connectors.ChaosConnector` (Python).

import type {
  Connector, EffectRecord, ObligationRecord,
  DispatchResult, ObservationResult, CompensationResult,
} from '../connectors/index.ts';
import type { Fault } from './scenarios.ts';

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Wraps an inner `Connector` and applies declarative faults around its
 * dispatch / observe methods. Three fault kinds are honoured:
 *
 *   lose_ack   — dispatch returns `unknown` after the inner call lands.
 *                Models a lost ack so the reconciler resolves via observe().
 *   duplicate  — observe returns `duplicate` instead of `confirmed`. Models
 *                the upstream having landed two rows for the same key.
 *   delay      — sleeps `ms ± jitter` before dispatch. Models slow upstreams.
 *
 * `rng` is the seeded PRNG from `Session` — same seed across two replays =
 * same fault sequence. The wrapper exposes the same `name` as the inner
 * connector so the outbox reactor routes to it transparently.
 */
export class ChaosConnector implements Connector {
  readonly name: string;
  readonly inner: Connector;
  readonly faults: readonly Fault[];
  private readonly rng: () => number;

  constructor(inner: Connector, faults: readonly Fault[], rng: () => number) {
    this.inner = inner;
    this.faults = faults;
    this.rng = rng;
    this.name = inner.name;
  }

  private fire(kind: string): Fault | null {
    for (const f of this.faults) {
      if (f.action !== kind) continue;
      if (f.probability >= 1.0 || this.rng() < f.probability) return f;
    }
    return null;
  }

  async dispatch(effect: EffectRecord): Promise<DispatchResult> {
    const d = this.fire('delay');
    if (d && d.ms > 0) {
      let ms = d.ms;
      if (d.jitter > 0) ms = Math.max(0, Math.floor(ms * (1.0 + (this.rng() * 2 - 1) * d.jitter)));
      await sleep(ms);
    }
    const result = await this.inner.dispatch(effect);
    if (result.outcome === 'confirmed' && this.fire('lose_ack')) {
      return {
        outcome: 'unknown',
        response: result.response,
        error: 'tape.chaos: simulated lost ack',
        dispatchId: result.dispatchId,
      };
    }
    return result;
  }

  async observe(effect: EffectRecord): Promise<ObservationResult> {
    const result = await this.inner.observe(effect);
    if (result.outcome === 'confirmed' && this.fire('duplicate')) {
      return {
        outcome: 'duplicate',
        response: result.response,
        count: (result.count ?? 1) + 1,
      };
    }
    return result;
  }

  async compensate(obligation: ObligationRecord): Promise<CompensationResult> {
    // Compensation faults are not modelled in Phase 1 (matches Python).
    return this.inner.compensate(obligation);
  }
}

/** Sugar: build a wrapped connector outside of a session. */
export function wrapConnector(inner: Connector, faults: readonly Fault[],
                                rng: () => number = Math.random): ChaosConnector {
  return new ChaosConnector(inner, faults, rng);
}
