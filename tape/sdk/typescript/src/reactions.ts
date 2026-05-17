// reactions.ts — the event-bus user surface for the TypeScript SDK.
//
// Mirrors the Python `tape.reactions` module: a process-global registry
// populated by `on*` calls, `registerAll(url)` to push them to the server,
// and `runDispatcher(url)` / `runPubSubBridge(...)` to run the in-proc
// dispatcher loop / Pub/Sub bridge. See ../../../design-principles/tape-event-bus.md
// and ../../python/tape/reactions.py for the reference impl.
//
// JS has no Python-style decorators outside class members, so the surface is
// registration-style: `on(pattern, handler, opts?)` registers a reaction
// definition; nothing is sent to the server until `registerAll` or
// `runDispatcher` runs. This module is optional — nothing else in the SDK
// depends on it.

import * as os from 'node:os';
import { TapeClient, DEFAULT_URL, HandlerKind } from './client.ts';

// ── public types ────────────────────────────────────────────────────────────

export interface ReactionOptions {
  predicate?: string;            // CEL expression, server-side
  agent?: string;                // app name — kind=AGENT
  publish?: string;              // broker url — kind=PUBLISH
  maxConcurrency?: number;
  rateLimitPerS?: number;
  debounceMs?: number;
  retryMax?: number;
  retryBackoffMs?: number;
  dlqAfterN?: number;
  numShards?: number;
  bootstrapFromHead?: boolean;
  name?: string;
  reactionId?: string;
}

/** The Task type is intentionally a structural type — the gRPC layer hands us
 *  a plain object whose keys mirror the proto field names (camelCased by
 *  @grpc/proto-loader with `keepCase: false`). */
export interface Task {
  taskId: string;
  reactionId: string;
  shard: number;
  sourceRunId: string;
  sourceGlobalSeq: number;
  subject: string;
  payloadJson: string;
  status: number;
  attempts: number;
  nextAttemptAtMs: number;
  leaseOwner: string;
  leaseExpiresAtMs: number;
  lastError: string;
  createdAtMs: number;
  traceId: string;
  parentSpanId: string;
}

export interface Reaction {
  reactionId: string;
  name: string;
  subjectPattern: string;
  predicateCel: string;
  handlerKind: number;
  agentApp: string;
  publishTarget: string;
  maxConcurrency: number;
  rateLimitPerS: number;
  debounceMs: number;
  retryMax: number;
  retryBackoffMs: number;
  dlqAfterN: number;
  numShards: number;
  createdAtMs: number;
  deleted: boolean;
  bootstrapFromHead: boolean;
}

export interface ReactionEnvelope {
  task: Task;
  payload: any;
}

export type ReactionHandler = (envelope: ReactionEnvelope) => Promise<void> | void;

/** One `on(...)` declaration, before it's been pushed to the server. */
export interface ReactionDef {
  subjectPattern: string;
  handler: ReactionHandler | null;
  handlerKind: number;
  predicateCel: string;
  agentApp: string;
  publishTarget: string;
  maxConcurrency: number;
  rateLimitPerS: number;
  debounceMs: number;
  retryMax: number;
  retryBackoffMs: number;
  dlqAfterN: number;
  numShards: number;
  bootstrapFromHead: boolean;
  name: string;
  reactionId: string;
  /** Populated after a successful `registerAll`. */
  serverReactionId: string;
}

// ── registry ────────────────────────────────────────────────────────────────

const REGISTRY: ReactionDef[] = [];

/** Drop every registered reaction. Test-only helper. */
export function _clearRegistry(): void { REGISTRY.length = 0; }
export function getRegistry(): ReactionDef[] { return REGISTRY.slice(); }

// ── decorators / registration helpers ───────────────────────────────────────

function resolveHandlerKind(
  agent: string | undefined,
  publish: string | undefined,
): { kind: number; agentApp: string; publishTarget: string } {
  if (agent && publish) {
    throw new Error('on(): pass either agent= OR publish=, not both');
  }
  if (agent) return { kind: HandlerKind.AGENT, agentApp: agent, publishTarget: '' };
  if (publish) return { kind: HandlerKind.PUBLISH, agentApp: '', publishTarget: publish };
  return { kind: HandlerKind.TASK, agentApp: '', publishTarget: '' };
}

/** Register a handler for `subjectPattern`. Stores the definition in a
 *  process-global registry; call `registerAll(url)` or `runDispatcher(url)`
 *  to push them to the server.
 *
 *  Pattern grammar: `/tape/<kind>/<verb>/<dim1>/<dim2>...` with `*` for one
 *  segment and `**` for the trailing rest. See the design doc. */
export function on(
  subjectPattern: string,
  handler: ReactionHandler,
  opts: ReactionOptions = {},
): void {
  const { kind, agentApp, publishTarget } = resolveHandlerKind(opts.agent, opts.publish);
  const rd: ReactionDef = {
    subjectPattern,
    handler,
    handlerKind: kind,
    predicateCel: opts.predicate ?? '',
    agentApp,
    publishTarget,
    maxConcurrency: Math.max(1, opts.maxConcurrency ?? 1),
    rateLimitPerS: opts.rateLimitPerS ?? 0,
    debounceMs: opts.debounceMs ?? 0,
    retryMax: opts.retryMax ?? 5,
    retryBackoffMs: opts.retryBackoffMs ?? 1000,
    dlqAfterN: opts.dlqAfterN ?? 5,
    numShards: Math.max(1, opts.numShards ?? 1),
    bootstrapFromHead: opts.bootstrapFromHead ?? false,
    name: opts.name ?? (handler.name || 'reaction'),
    reactionId: opts.reactionId ?? '',
    serverReactionId: '',
  };
  REGISTRY.push(rd);
}

// ── subject helpers ─────────────────────────────────────────────────────────

/** URL-encode a single subject segment. Pass `*` / `**` through unchanged. */
function seg(s: string): string {
  if (s === '*' || s === '**') return s;
  return encodeURIComponent(s);
}

function withName(opts: ReactionOptions, fallback: string): ReactionOptions {
  return { ...opts, name: opts.name ?? fallback };
}

/** Sloppy two-form parser used by the value/effect/gate convenience wrappers:
 *  the second positional may be a string (key/tool/verb) OR an options bag
 *  (the user skipped the second arg). */
function pickKey(keyOrOpts: string | ReactionOptions | undefined, fallback: string):
  { k: string; opts: ReactionOptions } {
  if (typeof keyOrOpts === 'string') return { k: keyOrOpts, opts: {} };
  if (keyOrOpts == null) return { k: fallback, opts: {} };
  return { k: fallback, opts: keyOrOpts };
}

/** Fire when a value in `(namespace, key)` is written. `key="*"` matches one
 *  segment, `key="**"` matches any trailing segments. */
export function onValueChange(
  namespace: string,
  keyOrOpts: string | ReactionOptions | undefined,
  handler: ReactionHandler,
  opts: ReactionOptions = {},
): void {
  const picked = pickKey(keyOrOpts, '*');
  const merged = { ...picked.opts, ...opts };
  const pattern = `/tape/value/changed/${seg(namespace)}/${seg(picked.k)}`;
  on(pattern, handler, withName(merged, `on_value_change/${namespace}/${picked.k}`));
}

export function onValueDeleted(
  namespace: string,
  keyOrOpts: string | ReactionOptions | undefined,
  handler: ReactionHandler,
  opts: ReactionOptions = {},
): void {
  const picked = pickKey(keyOrOpts, '*');
  const merged = { ...picked.opts, ...opts };
  const pattern = `/tape/value/deleted/${seg(namespace)}/${seg(picked.k)}`;
  on(pattern, handler, withName(merged, `on_value_deleted/${namespace}/${picked.k}`));
}

export function onEffectConfirmed(
  toolOrOpts: string | ReactionOptions | undefined,
  handler: ReactionHandler,
  opts: ReactionOptions = {},
): void {
  const picked = pickKey(toolOrOpts, '*');
  const merged = { ...picked.opts, ...opts };
  on(`/tape/effect/confirmed/${seg(picked.k)}/**`, handler,
    withName(merged, `on_effect_confirmed/${picked.k}`));
}

export function onEffectFailed(
  toolOrOpts: string | ReactionOptions | undefined,
  handler: ReactionHandler,
  opts: ReactionOptions = {},
): void {
  const picked = pickKey(toolOrOpts, '*');
  const merged = { ...picked.opts, ...opts };
  on(`/tape/effect/failed/${seg(picked.k)}/**`, handler,
    withName(merged, `on_effect_failed/${picked.k}`));
}

export function onEffectUnknown(
  toolOrOpts: string | ReactionOptions | undefined,
  handler: ReactionHandler,
  opts: ReactionOptions = {},
): void {
  const picked = pickKey(toolOrOpts, '*');
  const merged = { ...picked.opts, ...opts };
  on(`/tape/effect/unknown/${seg(picked.k)}/**`, handler,
    withName(merged, `on_effect_unknown/${picked.k}`));
}

export function onDecisionRecorded(handler: ReactionHandler, opts: ReactionOptions = {}): void {
  on('/tape/decision/recorded/**', handler, withName(opts, 'on_decision_recorded'));
}

/** Fire on a gate lifecycle event. Default `verb='released'`. */
export function onGate(
  gate: string,
  verb: 'released' | 'waiting' | undefined,
  handler: ReactionHandler,
  opts: ReactionOptions = {},
): void {
  const v = verb ?? 'released';
  on(`/tape/gate/${seg(v)}/${seg(gate)}/**`, handler, withName(opts, `on_gate/${gate}/${v}`));
}

/** Fire on run-lifecycle events. `status='terminal'` (default), `'failed'`, etc. */
export function onRun(
  status: string,
  handler: ReactionHandler,
  opts: ReactionOptions = {},
): void {
  on(`/tape/run/${seg(status)}/**`, handler, withName(opts, `on_run/${status}`));
}

// ── registration ────────────────────────────────────────────────────────────

/** Call `RegisterReaction` for every registered reaction. Idempotent on
 *  `reactionId` — the server upserts. `prefix` is prepended to each
 *  reaction's `name` so parallel test runs don't collide. */
export async function registerAll(opts: { url?: string; prefix?: string } = {}): Promise<Reaction[]> {
  const c = new TapeClient(opts.url ?? DEFAULT_URL);
  const out: Reaction[] = [];
  try {
    for (const rd of REGISTRY) {
      const r: Reaction = await c.registerReaction({
        reactionId: rd.reactionId,
        name: opts.prefix ? opts.prefix + rd.name : rd.name,
        subjectPattern: rd.subjectPattern,
        predicateCel: rd.predicateCel,
        handlerKind: rd.handlerKind,
        agentApp: rd.agentApp,
        publishTarget: rd.publishTarget,
        maxConcurrency: rd.maxConcurrency,
        rateLimitPerS: rd.rateLimitPerS,
        debounceMs: rd.debounceMs,
        retryMax: rd.retryMax,
        retryBackoffMs: rd.retryBackoffMs,
        dlqAfterN: rd.dlqAfterN,
        numShards: rd.numShards,
        bootstrapFromHead: rd.bootstrapFromHead,
      }) as Reaction;
      rd.serverReactionId = r.reactionId;
      out.push(r);
    }
    return out;
  } finally { c.close(); }
}

// ── backpressure primitives ─────────────────────────────────────────────────

/** Counting-promise semaphore: up to `capacity` `acquire()` calls resolve
 *  immediately; further ones queue until a `release()` lands. */
class Semaphore {
  private permits: number;
  private waiters: Array<() => void> = [];
  constructor(capacity: number) { this.permits = Math.max(1, capacity | 0); }
  async acquire(): Promise<void> {
    if (this.permits > 0) { this.permits--; return; }
    await new Promise<void>((r) => this.waiters.push(r));
  }
  release(): void {
    const w = this.waiters.shift();
    if (w) w();
    else this.permits++;
  }
  /** Resolve once every outstanding permit is released. */
  async drain(capacity: number): Promise<void> {
    while (this.permits < capacity) {
      await new Promise<void>((r) => setTimeout(r, 5));
    }
  }
}

/** Tiny token bucket — capacity == rate, refills 1 token / (1/rate)s.
 *  `acquire()` resolves once a token is available; `rate <= 0` disables. */
class TokenBucket {
  readonly rate: number;
  private tokens: number;
  private last: number;
  constructor(ratePerS: number) {
    this.rate = Math.max(0, ratePerS | 0);
    this.tokens = this.rate;
    this.last = Date.now();
  }
  async acquire(): Promise<void> {
    if (this.rate <= 0) return;
    /* eslint-disable no-constant-condition */
    while (true) {
      const now = Date.now();
      const elapsed = (now - this.last) / 1000;
      this.last = now;
      this.tokens = Math.min(this.rate, this.tokens + elapsed * this.rate);
      if (this.tokens >= 1) { this.tokens -= 1; return; }
      const need = (1 - this.tokens) / this.rate;
      await new Promise((r) => setTimeout(r, Math.min(250, Math.max(1, need * 1000))));
    }
  }
}

/** Coalesces `(reaction, subject)` within a window: returns true the first
 *  time a subject is seen, false until the window elapses. Matches the
 *  Python `_Debouncer` semantics — the dispatcher completes a debounced
 *  task as a no-op so it doesn't pile up in the DLQ. */
class Debouncer {
  readonly windowMs: number;
  private last = new Map<string, number>();
  constructor(windowMs: number) { this.windowMs = Math.max(0, windowMs | 0); }
  allow(subject: string): boolean {
    if (this.windowMs <= 0) return true;
    const now = Date.now();
    const prev = this.last.get(subject) ?? -1;
    if (prev < 0 || now - prev >= this.windowMs) {
      this.last.set(subject, now);
      return true;
    }
    return false;
  }
}

// ── OTel propagation (lazy) ─────────────────────────────────────────────────

let otelChecked = false;
let otelApi: any = null;

async function tryLoadOtel(): Promise<any> {
  if (otelChecked) return otelApi;
  otelChecked = true;
  try {
    // Indirected through a variable so TS doesn't try to resolve the module
    // at compile time — `@opentelemetry/api` is an optional runtime dep.
    const mod = '@opentelemetry/api';
    otelApi = await import(/* webpackIgnore: true */ mod);
  } catch { otelApi = null; }
  return otelApi;
}

async function withOtelSpan<T>(traceId: string, parentSpanId: string,
                               fn: () => Promise<T>): Promise<T> {
  if (!traceId || !parentSpanId) return fn();
  const api = await tryLoadOtel();
  if (!api?.trace?.getTracer) return fn();
  try {
    const tracer = api.trace.getTracer('tape.reactions');
    const parentCtx = api.trace.setSpanContext(api.context.active(), {
      traceId, spanId: parentSpanId, traceFlags: 0x01, isRemote: true,
    });
    return await api.context.with(parentCtx, () =>
      tracer.startActiveSpan('tape.task', async (span: any) => {
        try { return await fn(); }
        finally { span.end?.(); }
      }),
    );
  } catch { return fn(); }
}

// ── dispatcher ──────────────────────────────────────────────────────────────

function defaultOwner(): string {
  const env = process.env.TAPE_DISPATCHER_OWNER;
  if (env) return env;
  const hex = Math.random().toString(16).slice(2, 8);
  return `${os.hostname()}:${process.pid}:${hex}`;
}

function envelopeOf(task: Task): ReactionEnvelope {
  let payload: any = {};
  if (task.payloadJson) {
    try { payload = JSON.parse(task.payloadJson); }
    catch { payload = { raw: task.payloadJson }; }
  }
  return { task, payload };
}

interface ReactionState {
  rd: ReactionDef;
  sem: Semaphore;
  bucket: TokenBucket;
  debouncer: Debouncer;
}

async function dispatchOne(
  st: ReactionState, client: TapeClient, owner: string, task: Task,
  inFlight: Set<Promise<void>>,
): Promise<void> {
  const rd = st.rd;
  // Debounced: complete as a no-op (matches Python).
  if (!st.debouncer.allow(task.subject)) {
    try { await client.completeTask({ taskId: task.taskId, owner }); }
    catch { /* swallow — server may have re-leased; nothing to do */ }
    return;
  }

  await st.sem.acquire();
  const p = (async () => {
    try {
      await st.bucket.acquire();
      const env = envelopeOf(task);
      await withOtelSpan(task.traceId, task.parentSpanId, async () => {
        await rd.handler!(env);
      });
      try { await client.completeTask({ taskId: task.taskId, owner }); }
      catch { /* ignore — lease may have expired */ }
    } catch (ex) {
      const permanent = task.attempts >= rd.dlqAfterN;
      const errStr = (ex as Error)?.stack || String(ex);
      try {
        await client.nackTask({ taskId: task.taskId, owner, error: errStr, permanent });
      } catch { /* swallow */ }
    } finally {
      st.sem.release();
    }
  })();
  inFlight.add(p);
  p.finally(() => inFlight.delete(p));
}

/** Run the in-proc dispatcher loop: for each registered TASK reaction, claim
 *  a bounded batch from the server, fire handlers honouring max_concurrency
 *  / rate_limit_per_s / debounce_ms, and complete/nack each task. The server
 *  enforces retry / DLQ — this dispatcher just hands `permanent=true` to
 *  `NackTask` once `attempts >= dlq_after_n`. */
export async function runDispatcher(opts: {
  url?: string;
  owner?: string;
  pollIntervalMs?: number;
  once?: boolean;
  register?: boolean;
  prefix?: string;
  claimMax?: number;
  leaseMs?: number;
} = {}): Promise<void> {
  const url = opts.url ?? DEFAULT_URL;
  const owner = opts.owner ?? defaultOwner();
  const pollIntervalMs = opts.pollIntervalMs ?? 500;
  const claimMax = opts.claimMax ?? 16;
  const leaseMs = opts.leaseMs ?? 60_000;
  const register = opts.register ?? true;

  if (register) await registerAll({ url, prefix: opts.prefix });

  const state = new Map<string, ReactionState>();
  for (const rd of REGISTRY) {
    if (rd.handlerKind !== HandlerKind.TASK) continue;
    if (!rd.serverReactionId) continue;
    state.set(rd.serverReactionId, {
      rd,
      sem: new Semaphore(rd.maxConcurrency),
      bucket: new TokenBucket(rd.rateLimitPerS),
      debouncer: new Debouncer(rd.debounceMs),
    });
  }

  const client = new TapeClient(url);
  const inFlight = new Set<Promise<void>>();
  try {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      let didAny = false;
      for (const [rid, st] of state) {
        let tasks: Task[] = [];
        try {
          tasks = (await client.claimTasks({
            reactionId: rid, shard: -1, owner, leaseMs, max: claimMax,
          })) as Task[];
        } catch { /* swallow — retry next pass */ continue; }
        for (const t of tasks) {
          didAny = true;
          await dispatchOne(st, client, owner, t, inFlight);
        }
      }
      if (opts.once) {
        // wait for in-flight handlers to land so callers can observe their effects.
        await Promise.allSettled(Array.from(inFlight));
        return;
      }
      if (!didAny) await new Promise((r) => setTimeout(r, pollIntervalMs));
    }
  } finally {
    await Promise.allSettled(Array.from(inFlight));
    client.close();
  }
}

// ── Pub/Sub bridge ──────────────────────────────────────────────────────────

/** Pull PUBLISH-kind tasks and forward them to a Cloud Pub/Sub topic.
 *  Lazy-imports `@google-cloud/pubsub`; throws if it isn't installed. */
export async function runPubSubBridge(opts: {
  url?: string; project: string; topic: string; reactionId?: string;
  owner?: string; once?: boolean; pollIntervalMs?: number;
  claimMax?: number; leaseMs?: number;
}): Promise<void> {
  let pubsub: any;
  try {
    const mod = '@google-cloud/pubsub';
    pubsub = await import(/* webpackIgnore: true */ mod);
  } catch (ex) {
    throw new Error(
      'runPubSubBridge requires @google-cloud/pubsub; npm install @google-cloud/pubsub',
    );
  }
  const url = opts.url ?? DEFAULT_URL;
  const owner = opts.owner ?? defaultOwner();
  const pollIntervalMs = opts.pollIntervalMs ?? 500;
  const claimMax = opts.claimMax ?? 32;
  const leaseMs = opts.leaseMs ?? 60_000;

  const PubSub = pubsub.PubSub;
  const client = new PubSub({ projectId: opts.project });
  const topic = client.topic(opts.topic, { messageOrdering: true });

  // Resolve reaction ids.
  let rids: string[] = [];
  if (opts.reactionId) {
    rids = [opts.reactionId];
  } else {
    for (const rd of REGISTRY) {
      if (rd.handlerKind !== HandlerKind.PUBLISH) continue;
      if (!rd.serverReactionId) await registerAll({ url });
      if (rd.serverReactionId) rids.push(rd.serverReactionId);
    }
  }

  const tape = new TapeClient(url);
  try {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      let didAny = false;
      for (const rid of rids) {
        let tasks: Task[] = [];
        try {
          tasks = (await tape.claimTasks({
            reactionId: rid, shard: -1, owner, leaseMs, max: claimMax,
          })) as Task[];
        } catch { continue; }
        for (const t of tasks) {
          didAny = true;
          const attrs: Record<string, string> = {
            'tape-task-id': t.taskId,
            'tape-reaction-id': t.reactionId,
            'tape-subject': t.subject,
            'tape-global-seq': String(t.sourceGlobalSeq),
            'tape-trace-id': t.traceId || '',
          };
          try {
            await topic.publishMessage({
              data: Buffer.from(t.payloadJson || '', 'utf8'),
              attributes: attrs,
              orderingKey: t.sourceRunId || '',
            });
            await tape.completeTask({ taskId: t.taskId, owner });
          } catch (ex) {
            try {
              await tape.nackTask({
                taskId: t.taskId, owner,
                error: `pubsub-publish: ${ex}`, permanent: false,
              });
            } catch { /* swallow */ }
          }
        }
      }
      if (opts.once) return;
      if (!didAny) await new Promise((r) => setTimeout(r, pollIntervalMs));
    }
  } finally {
    try { await client.close(); } catch { /* ignore */ }
    try { tape.close(); } catch { /* ignore */ }
  }
}
