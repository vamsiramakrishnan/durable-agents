// Sinks — the journal fan-out endpoints. Each sink is a tiny adapter that
// implements `publish(entry) -> Promise<void>` (and an optional `close()`),
// matching the callback shape `runEventFanout` already accepts:
//
//     await runEventFanout({ url, sink: (entry) => sink.publish(entry), ... })
//
// Built-ins:
//   * LogSink(path)          — append each entry as a JSON line; testable.
//   * WebhookSink(url, ...)  — POST each entry with `X-Tape-Event-Id` header.
//   * PubSubSink(project,topic) — publish to Cloud Pub/Sub (lazy-imports
//                                 @google-cloud/pubsub).
//
// At-least-once relay + consumer-side dedup on `(run_id, seq)` =
// exactly-once-effective.

import * as fs from 'node:fs';
import * as path from 'node:path';

export interface Sink {
  publish(entry: any): Promise<void> | void;
  close(): Promise<void> | void;
}

function entryToJson(entry: any): string {
  return JSON.stringify({
    run_id: entry.runId ?? '',
    seq: entry.seq ?? 0,
    kind: entry.kind ?? '',
    payload_json: entry.payloadJson ?? '',
    ts_ms: entry.tsMs ?? 0,
  });
}

// ── log sink ────────────────────────────────────────────────────────────────

export class LogSink implements Sink {
  private fd: number | null = null;
  private readonly target: string;
  constructor(target: string = ':stderr') {
    this.target = target;
    if (target !== ':stderr' && target !== ':stdout') {
      fs.mkdirSync(path.dirname(target) || '.', { recursive: true });
      this.fd = fs.openSync(target, 'a');
    }
  }
  publish(entry: any): void {
    const line = entryToJson(entry) + '\n';
    if (this.fd !== null) {
      fs.writeSync(this.fd, line);
    } else if (this.target === ':stdout') {
      process.stdout.write(line);
    } else {
      process.stderr.write(line);
    }
  }
  close(): void { if (this.fd !== null) { fs.closeSync(this.fd); this.fd = null; } }
}

// ── webhook sink ────────────────────────────────────────────────────────────

export interface WebhookSinkOpts {
  url: string;
  headers?: Record<string, string>;
  maxRetries?: number;
  initialBackoffMs?: number;
  timeoutMs?: number;
}

export class WebhookSink implements Sink {
  private readonly opts: Required<WebhookSinkOpts>;
  constructor(opts: WebhookSinkOpts) {
    if (!opts.url) throw new Error('WebhookSink: opts.url is required');
    this.opts = {
      headers: {},
      maxRetries: 3,
      initialBackoffMs: 500,
      timeoutMs: 10_000,
      ...opts,
    };
  }
  async publish(entry: any): Promise<void> {
    const body = entryToJson(entry);
    const eventId = `${entry.runId ?? ''}/${entry.seq ?? 0}`;
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'X-Tape-Event-Id': eventId,
      ...this.opts.headers,
    };

    let delay = this.opts.initialBackoffMs;
    let lastErr: unknown = null;
    for (let i = 0; i < this.opts.maxRetries; i++) {
      const ctl = new AbortController();
      const to = setTimeout(() => ctl.abort(), this.opts.timeoutMs);
      try {
        const resp = await fetch(this.opts.url, {
          method: 'POST', body, headers, signal: ctl.signal,
        });
        if (resp.status >= 200 && resp.status < 300) return;
        lastErr = new Error(`webhook ${this.opts.url} returned HTTP ${resp.status}`);
      } catch (ex) {
        lastErr = ex;
      } finally {
        clearTimeout(to);
      }
      await new Promise((r) => setTimeout(r, delay));
      delay *= 2;
    }
    throw lastErr ?? new Error(`webhook ${this.opts.url} exhausted retries`);
  }
  close(): void { /* nothing to close */ }
}

// ── Pub/Sub sink ────────────────────────────────────────────────────────────

export interface PubSubSinkOpts {
  project: string;
  topic: string;
}

/** Publish journal entries to Google Cloud Pub/Sub.
 *
 * Lazy-imports `@google-cloud/pubsub`. If the package isn't installed, the
 * first `publish()` call throws with an install hint.
 *
 * `orderingKey = run_id` so the subscriber preserves per-run order when it
 * enables ordered delivery. The `tape-event-id = run_id/seq` message attribute
 * is what consumers dedup on (Pub/Sub assigns its own `messageId`). */
export class PubSubSink implements Sink {
  private readonly opts: PubSubSinkOpts;
  private publisher: any = null;
  private topicName = '';
  constructor(opts: PubSubSinkOpts) {
    if (!opts.project || !opts.topic) {
      throw new Error('PubSubSink: project and topic are required');
    }
    this.opts = opts;
  }
  private async ensure(): Promise<void> {
    if (this.publisher) return;
    let mod: any;
    try {
      mod = await import('@google-cloud/pubsub');
    } catch (ex) {
      throw new Error(
        'PubSubSink requires @google-cloud/pubsub — `npm i @google-cloud/pubsub`',
      );
    }
    const client = new mod.PubSub({ projectId: this.opts.project });
    this.publisher = client.topic(this.opts.topic, {
      messageOrdering: true,
      enableMessageOrdering: true,
    });
    this.topicName = `projects/${this.opts.project}/topics/${this.opts.topic}`;
  }
  async publish(entry: any): Promise<void> {
    await this.ensure();
    const data = Buffer.from(entryToJson(entry), 'utf-8');
    const eventId = `${entry.runId ?? ''}/${entry.seq ?? 0}`;
    await this.publisher.publishMessage({
      data,
      orderingKey: String(entry.runId ?? ''),
      attributes: { 'tape-event-id': eventId, kind: String(entry.kind ?? '') },
    });
  }
  async close(): Promise<void> {
    if (this.publisher) {
      try { await this.publisher.flush(); } catch { /* ignore */ }
    }
  }
}

// ── callable adapter ────────────────────────────────────────────────────────

export class FnSink implements Sink {
  private readonly fn: (entry: any) => Promise<void> | void;
  constructor(fn: (entry: any) => Promise<void> | void) { this.fn = fn; }
  publish(entry: any) { return this.fn(entry); }
  close() { /* nothing */ }
}
