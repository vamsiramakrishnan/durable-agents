import type {
  Connector, EffectRecord, ObligationRecord,
  DispatchResult, ObservationResult, CompensationResult,
} from './index.ts';

export interface PubSubConnectorOpts {
  name?: string;
  project: string;
  topic: string;
  compensateTopic?: string;
  /** Optional Tape URL used by Observe() to look up the subscriber's result record. */
  tapeUrl?: string;
}

/**
 * Publish the intent as a Pub/Sub message. The subscriber writes a result
 * record back to Tape (`tape.setValue("outbox/<connector>", key, {...})`)
 * which Observe() reads to resolve UNKNOWN. `@google-cloud/pubsub` is an
 * optional dependency — lazy-imported.
 */
export class PubSubConnector implements Connector {
  readonly name: string;
  private readonly opts: PubSubConnectorOpts;
  private client: any = null;

  constructor(opts: PubSubConnectorOpts) {
    if (!opts.project || !opts.topic) {
      throw new Error('PubSubConnector: project and topic are required');
    }
    this.name = opts.name ?? `pubsub:${opts.topic}`;
    this.opts = opts;
  }

  private async ensure(): Promise<any> {
    if (this.client) return this.client;
    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const mod = await import('@google-cloud/pubsub' as any);
      const PubSub = (mod as any).PubSub;
      this.client = new PubSub({ projectId: this.opts.project });
      return this.client;
    } catch (ex) {
      throw new Error(
        'PubSubConnector requires @google-cloud/pubsub — `npm i @google-cloud/pubsub`. ' +
        `Underlying: ${(ex as Error).message}`,
      );
    }
  }

  async dispatch(effect: EffectRecord): Promise<DispatchResult> {
    try {
      const c = await this.ensure();
      const topic = c.topic(this.opts.topic, { messageOrdering: true });
      const messageId = await topic.publishMessage({
        json: effect.payload,
        orderingKey: effect.runId,
        attributes: {
          tape_idempotency_key: effect.idempotencyKey,
          tape_run_id: effect.runId,
          tape_business_key: effect.businessKey ?? '',
          tape_tool: effect.toolName,
          tape_attempt: String(effect.attempt ?? 1),
        },
      });
      return { outcome: 'confirmed', response: { message_id: messageId }, dispatchId: messageId };
    } catch (ex) {
      return { outcome: 'unknown', error: (ex as Error).message };
    }
  }

  async observe(_effect: EffectRecord): Promise<ObservationResult> {
    // The Pub/Sub topology resolves UNKNOWN via the subscriber writing a
    // tape value the reconciler reads. To keep this file dep-light, we
    // surface a documented hook: chain an `HttpConnector.observeUrl`, or
    // override this method in a subclass.
    return { outcome: 'unknown', error: 'PubSubConnector.observe: chain a status URL via HttpConnector.observeUrl, or subclass.' };
  }

  async compensate(obligation: ObligationRecord): Promise<CompensationResult> {
    if (!this.opts.compensateTopic) {
      return { outcome: 'stuck', error: 'no compensateTopic configured' };
    }
    try {
      const c = await this.ensure();
      const topic = c.topic(this.opts.compensateTopic, { messageOrdering: true });
      const messageId = await topic.publishMessage({
        json: obligation.payload,
        orderingKey: obligation.runId,
        attributes: {
          tape_obligation_kind: obligation.kind,
          tape_effect_key: obligation.effectKey,
          tape_run_id: obligation.runId,
        },
      });
      return { outcome: 'compensated', response: { message_id: messageId } };
    } catch (ex) {
      return { outcome: 'pending', error: (ex as Error).message };
    }
  }
}
