// `durableApp` — the wiring entrypoint for an ADK-style agent on Tape.
//
// Mirrors `tape.adk.durable_app(...)` from the Python SDK. The JS/TS port
// of ADK isn't shipped yet, so `durableApp({...})` returns the wiring
// **values** an adapter needs: the resolved TAPE_URL, the connected
// `TapeClient`, the lease-owner string, and the chosen `Budget`. When a
// TS ADK port lands, its `Runner` constructor will accept the bundle.

import { hostname } from 'node:os';
import { TapeClient, DEFAULT_URL, type ClientOptions } from './client.ts';

export interface Budget {
  usdCap?: number;
  tokenCap?: number;
}

export interface DurableAppConfig {
  /** ADK App name. Required. */
  name: string;
  /** Defaults to $TAPE_URL, then "tape://localhost:7878". */
  tapeUrl?: string;
  /** Admit/charge thresholds. Zero values mean "no cap". */
  budget?: Budget;
  /** Enable ADK ResumabilityConfig(is_resumable=True). */
  resumable?: boolean;
  /** Plugin polls RunStatus on every model boundary. */
  checkCancellation?: boolean;
  /** Overrides the default "<hostname>:<pid>" identity. */
  leaseOwner?: string;
  /** Overrides the default 120_000 ms lease. */
  leaseTtlMs?: number;
  /** Passed through to `new TapeClient(url, opts)`. */
  clientOptions?: ClientOptions;
}

export interface DurableApp {
  readonly name: string;
  readonly url: string;
  readonly client: TapeClient;
  readonly leaseOwner: string;
  readonly leaseTtlMs: number;
  readonly budget: Budget;
  readonly resumable: boolean;
  readonly checkCancellation: boolean;
  /** Close the underlying client. */
  close(): Promise<void>;
}

function defaultLeaseOwner(): string {
  return `${hostname() || 'host'}:${process.pid}`;
}

function defaultLeaseTtlMs(): number {
  const v = Number(process.env.TAPE_LEASE_MS);
  return Number.isFinite(v) && v > 0 ? v : 120_000;
}

/**
 * Wire a Tape-backed ADK app in one call.
 *
 * ```ts
 * const app = durableApp({ name: 'treasury', budget: { usdCap: 50 } });
 * try {
 *   const run = await app.client.beginRun({ ... });
 *   // ...
 * } finally {
 *   await app.close();
 * }
 * ```
 */
export function durableApp(cfg: DurableAppConfig): DurableApp {
  if (!cfg.name) throw new Error('durableApp: name is required');
  const url = cfg.tapeUrl || process.env.TAPE_URL || DEFAULT_URL;
  const client = new TapeClient(url, cfg.clientOptions);
  return {
    name: cfg.name,
    url,
    client,
    leaseOwner: cfg.leaseOwner || defaultLeaseOwner(),
    leaseTtlMs: cfg.leaseTtlMs ?? defaultLeaseTtlMs(),
    budget: cfg.budget ?? {},
    resumable: cfg.resumable ?? true,
    checkCancellation: cfg.checkCancellation ?? true,
    async close() {
      // TapeClient.close is synchronous in the gRPC stub, but we keep the
      // signature async so we can do future async cleanup safely.
      try { (client as any).close?.(); } catch { /* swallow */ }
    },
  };
}
