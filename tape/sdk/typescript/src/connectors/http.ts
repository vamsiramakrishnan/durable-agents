import type {
  Connector, EffectRecord, ObligationRecord,
  DispatchResult, ObservationResult, CompensationResult,
} from './index.ts';

export interface HttpConnectorOpts {
  name?: string;
  url: string;
  observeUrl?: string;
  compensateUrl?: string;
  timeoutMs?: number;
  headers?: Record<string, string>;
  /** Inject your own fetch (e.g. node's undici or a mocked one in tests). */
  fetchFn?: typeof fetch;
}

/**
 * POST the intent payload to an HTTPS endpoint. Headers attached:
 *   X-Tape-Idempotency-Key  the runner-derived dedup key
 *   X-Tape-Business-Key     when supplied by `outboxTool({ businessKey })`
 *   X-Tape-Run-Id           for traceability
 *   X-Tape-Attempt          dispatch attempt #
 *
 * Outcome: 2xx => CONFIRMED, 4xx => FAILED, 5xx/network => UNKNOWN.
 */
export class HttpConnector implements Connector {
  readonly name: string;
  private readonly opts: HttpConnectorOpts;
  private readonly fetcher: typeof fetch;

  constructor(opts: HttpConnectorOpts) {
    if (!opts.url) throw new Error('HttpConnector: opts.url is required');
    this.name = opts.name ?? 'http';
    this.opts = opts;
    this.fetcher = opts.fetchFn ?? (globalThis.fetch.bind(globalThis));
  }

  private async post(url: string, body: unknown, headers: Record<string, string>): Promise<{ status: number; body: unknown }> {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), this.opts.timeoutMs ?? 30_000);
    try {
      const resp = await this.fetcher(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...this.opts.headers, ...headers },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      let parsed: unknown;
      const text = await resp.text();
      try { parsed = JSON.parse(text); } catch { parsed = text; }
      return { status: resp.status, body: parsed };
    } finally {
      clearTimeout(t);
    }
  }

  private headersFor(e: { idempotencyKey: string; runId: string; businessKey?: string; attempt?: number }): Record<string, string> {
    const h: Record<string, string> = {
      'X-Tape-Idempotency-Key': e.idempotencyKey,
      'X-Tape-Run-Id': e.runId,
    };
    if (e.businessKey) h['X-Tape-Business-Key'] = e.businessKey;
    if (e.attempt) h['X-Tape-Attempt'] = String(e.attempt);
    return h;
  }

  async dispatch(effect: EffectRecord): Promise<DispatchResult> {
    try {
      const { status, body } = await this.post(this.opts.url, effect.payload, this.headersFor({
        idempotencyKey: effect.idempotencyKey, runId: effect.runId,
        businessKey: effect.businessKey, attempt: effect.attempt,
      }));
      if (status >= 200 && status < 300) return { outcome: 'confirmed', response: body };
      if (status >= 400 && status < 500) return { outcome: 'failed', response: body, error: `http ${status}` };
      return { outcome: 'unknown', response: body, error: `http ${status}` };
    } catch (ex) {
      return { outcome: 'unknown', error: (ex as Error).message };
    }
  }

  async observe(effect: EffectRecord): Promise<ObservationResult> {
    if (!this.opts.observeUrl) {
      return { outcome: 'unknown', error: 'no observeUrl configured' };
    }
    try {
      const { status, body } = await this.post(this.opts.observeUrl, {
        idempotency_key: effect.idempotencyKey,
        business_key: effect.businessKey,
        payload: effect.payload,
      }, this.headersFor({
        idempotencyKey: effect.idempotencyKey, runId: effect.runId,
        businessKey: effect.businessKey, attempt: effect.attempt,
      }));
      if (status !== 200) return { outcome: 'unknown', error: `http ${status}`, response: body };
      const obj = (body as any) ?? {};
      const count = Number(obj.count ?? 0);
      if (count === 0) return { outcome: 'absent', response: obj, count: 0 };
      if (count === 1) return { outcome: 'confirmed', response: obj, count: 1 };
      return { outcome: 'duplicate', response: obj, count };
    } catch (ex) {
      return { outcome: 'unknown', error: (ex as Error).message };
    }
  }

  async compensate(obligation: ObligationRecord): Promise<CompensationResult> {
    if (!this.opts.compensateUrl) {
      return { outcome: 'stuck', error: 'no compensateUrl configured' };
    }
    try {
      const { status, body } = await this.post(this.opts.compensateUrl, obligation.payload, {
        'X-Tape-Idempotency-Key': obligation.effectKey,
        'X-Tape-Run-Id': obligation.runId,
      });
      if (status >= 200 && status < 300) return { outcome: 'compensated', response: body };
      if (status >= 400 && status < 500) return { outcome: 'failed', response: body, error: `http ${status}` };
      return { outcome: 'pending', response: body, error: `http ${status}` };
    } catch (ex) {
      return { outcome: 'pending', error: (ex as Error).message };
    }
  }
}
