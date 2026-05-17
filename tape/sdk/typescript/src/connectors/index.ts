// Capability connectors — the things the outbox reactor calls. Mirrors
// `tape.connectors` in Python.

export type DispatchOutcome = 'confirmed' | 'pending' | 'unknown' | 'failed';
export type ObservationOutcome = 'confirmed' | 'absent' | 'duplicate' | 'stuck' | 'unknown';
export type CompensationOutcome = 'compensated' | 'pending' | 'stuck' | 'failed';

export interface EffectRecord {
  runId: string;
  idempotencyKey: string;
  toolName: string;
  connector: string;
  payload: unknown;
  businessKey?: string;
  attempt?: number;
  semantics?: string;
  tenantId?: string;
  appName?: string;
  metadata?: Record<string, unknown>;
}

export interface ObligationRecord {
  runId: string;
  effectKey: string;
  kind: string;
  payload: unknown;
  attempt?: number;
  compensatorRef?: string;
  tenantId?: string;
}

export interface DispatchResult {
  outcome: DispatchOutcome;
  response?: unknown;
  error?: string;
  dispatchId?: string;
  retryAfterMs?: number;
}

export interface ObservationResult {
  outcome: ObservationOutcome;
  response?: unknown;
  error?: string;
  count?: number;
}

export interface CompensationResult {
  outcome: CompensationOutcome;
  response?: unknown;
  error?: string;
}

/**
 * Every capability connector implements this interface. All three methods
 * MUST be idempotent on (runId, idempotencyKey).
 */
export interface Connector {
  readonly name: string;
  dispatch(effect: EffectRecord): Promise<DispatchResult>;
  observe(effect: EffectRecord): Promise<ObservationResult>;
  compensate(obligation: ObligationRecord): Promise<CompensationResult>;
}

export class ConnectorRegistry {
  private items = new Map<string, Connector>();

  register(name: string, c: Connector): void {
    if (this.items.has(name)) {
      throw new Error(`connectors: ${name} already registered`);
    }
    this.items.set(name, c);
  }

  replace(name: string, c: Connector): void { this.items.set(name, c); }

  get(name: string): Connector {
    const c = this.items.get(name);
    if (!c) throw new Error(`connectors: unknown connector ${name}; known: ${[...this.items.keys()].join(', ')}`);
    return c;
  }

  has(name: string): boolean { return this.items.has(name); }
  names(): string[] { return [...this.items.keys()]; }
  clear(): void { this.items.clear(); }
}

/** Process-global registry. Register at startup; consume from anywhere. */
export const CONNECTORS = new ConnectorRegistry();

export { LogConnector } from './log.ts';
export { HttpConnector, type HttpConnectorOpts } from './http.ts';
export { PubSubConnector, type PubSubConnectorOpts } from './pubsub.ts';
export { CloudTasksConnector, type CloudTasksConnectorOpts } from './cloud_tasks.ts';
