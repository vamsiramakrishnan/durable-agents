import type {
  Connector, EffectRecord, ObligationRecord,
  DispatchResult, ObservationResult, CompensationResult,
} from './index.ts';

export interface CloudTasksConnectorOpts {
  name?: string;
  project: string;
  location: string;
  queue: string;
  targetUrl: string;
  serviceAccountEmail?: string;
  observeUrl?: string;
  compensateUrl?: string;
}

const TASK_ID_SAFE = /[^a-zA-Z0-9\-_]/g;
function safeTaskId(key: string): string {
  return key.replace(TASK_ID_SAFE, '-').slice(0, 500);
}

/**
 * Enqueue an HTTP target on Cloud Tasks. Cloud Tasks owns retries,
 * backoff, and scheduling; the connector just creates the task.
 * `@google-cloud/tasks` is an optional dependency — lazy-imported.
 */
export class CloudTasksConnector implements Connector {
  readonly name: string;
  private readonly opts: CloudTasksConnectorOpts;
  private client: any = null;

  constructor(opts: CloudTasksConnectorOpts) {
    if (!opts.project || !opts.location || !opts.queue || !opts.targetUrl) {
      throw new Error('CloudTasksConnector: project / location / queue / targetUrl are required');
    }
    this.name = opts.name ?? `tasks:${opts.queue}`;
    this.opts = opts;
  }

  private queuePath(): string {
    return `projects/${this.opts.project}/locations/${this.opts.location}/queues/${this.opts.queue}`;
  }

  private async ensure(): Promise<any> {
    if (this.client) return this.client;
    try {
      const mod = await import('@google-cloud/tasks' as any);
      const CloudTasksClient = (mod as any).CloudTasksClient;
      this.client = new CloudTasksClient();
      return this.client;
    } catch (ex) {
      throw new Error(
        'CloudTasksConnector requires @google-cloud/tasks — `npm i @google-cloud/tasks`. ' +
        `Underlying: ${(ex as Error).message}`,
      );
    }
  }

  async dispatch(effect: EffectRecord): Promise<DispatchResult> {
    try {
      const c = await this.ensure();
      const task: Record<string, unknown> = {
        name: `${this.queuePath()}/tasks/${safeTaskId(effect.idempotencyKey)}`,
        httpRequest: {
          httpMethod: 'POST',
          url: this.opts.targetUrl,
          headers: {
            'Content-Type': 'application/json',
            'X-Tape-Idempotency-Key': effect.idempotencyKey,
            'X-Tape-Run-Id': effect.runId,
            'X-Tape-Business-Key': effect.businessKey ?? '',
          },
          body: Buffer.from(JSON.stringify(effect.payload)).toString('base64'),
          ...(this.opts.serviceAccountEmail ? {
            oidcToken: {
              serviceAccountEmail: this.opts.serviceAccountEmail,
              audience: this.opts.targetUrl,
            },
          } : {}),
        },
      };
      const [created] = await c.createTask({ parent: this.queuePath(), task });
      return { outcome: 'pending', response: { name: created.name }, dispatchId: created.name };
    } catch (ex) {
      const msg = (ex as Error).message;
      if (/already exists/i.test(msg) || /ALREADY_EXISTS/.test(msg)) {
        return { outcome: 'confirmed', response: { deduped: true } };
      }
      return { outcome: 'unknown', error: msg };
    }
  }

  async observe(effect: EffectRecord): Promise<ObservationResult> {
    if (!this.opts.observeUrl) {
      return { outcome: 'unknown', error: 'no observeUrl configured' };
    }
    const { HttpConnector } = await import('./http.ts');
    const h = new HttpConnector({ url: this.opts.observeUrl, observeUrl: this.opts.observeUrl });
    return h.observe(effect);
  }

  async compensate(obligation: ObligationRecord): Promise<CompensationResult> {
    if (!this.opts.compensateUrl) {
      return { outcome: 'stuck', error: 'no compensateUrl configured' };
    }
    const { HttpConnector } = await import('./http.ts');
    const h = new HttpConnector({ url: this.opts.compensateUrl, compensateUrl: this.opts.compensateUrl });
    return h.compensate(obligation);
  }
}
