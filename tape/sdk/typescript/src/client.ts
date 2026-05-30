// TapeClient — the TypeScript/Node client over the `tape.v1` gRPC service.
//
// URL schemes:
//   tape://host:port   plaintext gRPC (self-hosted, k8s, local)
//   tapes://host       TLS on :443 (Cloud Run / any HTTPS endpoint); when the
//                      endpoint is IAM-protected, an OIDC ID token is attached
//                      automatically via google-auth-library (an optional dep)
//
// The proto is parsed at load time by @grpc/proto-loader, so there's no codegen
// step: pass `protoPath` if the file lives somewhere other than ../proto.

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DEFAULT_PROTO_PATH = join(__dirname, '..', 'proto', 'tape.proto');

export const DEFAULT_URL = process.env.TAPE_URL ?? 'tape://localhost:7878';

// ── status enums (mirror the proto) ─────────────────────────────────────────
export const RunStatus = Object.freeze({
  UNSPECIFIED: 0, RUNNABLE: 1, RUNNING: 2, WAITING: 3,
  TERMINAL: 4, FAILED: 5, COMPENSATING: 6, STUCK: 7, CANCELLED: 8,
});
export const EffectStatus = Object.freeze({
  UNSPECIFIED: 0, PENDING: 1, CONFIRMED: 2, FAILED: 3, UNKNOWN: 4,
});
// Outbox / non-idempotent contract (see proto: EffectSemantics,
// EffectDispatchMode, EffectResolution). Defaults preserve v1 behaviour
// (idempotent + inline); opt into the outbox path by passing non-defaults
// to beginEffect.
export const EffectSemantics = Object.freeze({
  UNSPECIFIED: 0, IDEMPOTENT: 1, NON_IDEMPOTENT: 2, OBSERVE_ONLY: 3,
});
export const EffectDispatchMode = Object.freeze({
  UNSPECIFIED: 0, INLINE: 1, OUTBOX: 2,
});
export const EffectResolution = Object.freeze({
  UNSPECIFIED: 0, CONFIRMED: 1, FAILED: 2, ABSENT: 3, DUPLICATE: 4, STUCK: 5,
});
export const ObligationStatus = Object.freeze({
  UNSPECIFIED: 0, PENDING: 1, COMMITTED: 2, COMPENSATED: 3, STUCK: 4,
});
// New event-bus enums — mirror tape.v1.HandlerKind / tape.v1.TaskStatus.
export const HandlerKind = Object.freeze({
  UNSPECIFIED: 0, AGENT: 1, TASK: 2, PUBLISH: 3,
});
export const TaskStatus = Object.freeze({
  UNSPECIFIED: 0, PENDING: 1, CLAIMED: 2, DONE: 3, FAILED: 4, DLQ: 5,
});

function targetOf(url: string): { target: string; secure: boolean } {
  if (url.startsWith('tapes://')) {
    const h = url.slice('tapes://'.length);
    return { target: h.includes(':') ? h : `${h}:443`, secure: true };
  }
  if (url.startsWith('grpcs://')) return targetOf('tapes://' + url.slice('grpcs://'.length));
  if (url.startsWith('tape://')) return { target: url.slice('tape://'.length), secure: false };
  if (url.startsWith('grpc://')) return { target: url.slice('grpc://'.length), secure: false };
  return { target: url, secure: false };
}

function audienceFor(url: string): string {
  const { target } = targetOf(url);
  return `https://${target.split(':')[0]}`;
}

// A refreshing call-credentials plugin: attach a Google ID token for the Cloud
// Run audience. Best-effort — if google-auth-library isn't installed or the
// fetch fails, the call goes through without auth (fine if the endpoint isn't
// IAM-protected). Lazy-imported so non-GCP users have no extra dep.
function makeIdTokenCallCreds(audience: string): grpc.CallCredentials {
  let token = '';
  let expSec = 0;
  return grpc.credentials.createFromMetadataGenerator(async (_params, callback) => {
    const md = new grpc.Metadata();
    const now = Date.now() / 1000;
    if (!token || now > expSec - 60) {
      try {
        const { GoogleAuth } = await import('google-auth-library');
        const auth = new GoogleAuth();
        const client = await auth.getIdTokenClient(audience);
        // google-auth-library 10.x returns a Fetch-style `Headers` instance;
        // 9.x returned a plain `Record<string, string>`. Tape's package.json
        // pins ^10.6.2 but a downstream may override back to 9 (or the
        // optional dep may resolve from an existing lockfile). Read both
        // shapes so authentication doesn't silently fall through on v9.
        const headers: any = await client.getRequestHeaders(audience);
        const bearer = typeof headers?.get === 'function'
          ? (headers.get('Authorization') ?? headers.get('authorization'))
          : (headers['Authorization'] ?? headers['authorization']);
        if (typeof bearer === 'string' && bearer.startsWith('Bearer ')) {
          token = bearer.slice('Bearer '.length);
          try {
            const payload = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString());
            expSec = Number(payload.exp ?? now + 1800);
          } catch { expSec = now + 1800; }
        }
      } catch (e) {
        // proceed without auth; the request might still be served (TLS-without-IAM)
      }
    }
    if (token) md.set('authorization', `Bearer ${token}`);
    callback(null, md);
  });
}

export interface ClientOptions {
  protoPath?: string;
  auth?: boolean;          // default true on tapes://
  audience?: string;       // override the derived audience
  idToken?: string;        // static token (overrides auth)
}

type Stub = grpc.Client & {
  [k: string]: (req: any, cb: (err: grpc.ServiceError | null, res: any) => void) => grpc.ClientUnaryCall | grpc.ClientReadableStream<any>;
};

export class TapeClient {
  readonly url: string;
  private readonly stub: Stub;
  private readonly channel: grpc.Channel;

  constructor(url: string = DEFAULT_URL, opts: ClientOptions = {}) {
    this.url = url;
    const def = protoLoader.loadSync(opts.protoPath ?? DEFAULT_PROTO_PATH, {
      keepCase: false, longs: Number, enums: Number, defaults: true, oneofs: true,
    });
    const grpcObj = grpc.loadPackageDefinition(def) as any;
    const TapeSvc = grpcObj.tape.v1.Tape;

    const { target, secure } = targetOf(url);
    let creds: grpc.ChannelCredentials;
    if (secure) {
      const channelCreds = grpc.credentials.createSsl();
      let callCreds: grpc.CallCredentials | undefined;
      if (opts.idToken) {
        callCreds = grpc.credentials.createFromMetadataGenerator((_p, cb) => {
          const md = new grpc.Metadata();
          md.set('authorization', `Bearer ${opts.idToken}`);
          cb(null, md);
        });
      } else if (opts.auth !== false) {
        callCreds = makeIdTokenCallCreds(opts.audience ?? process.env.TAPE_AUDIENCE ?? audienceFor(url));
      }
      creds = callCreds ? grpc.credentials.combineChannelCredentials(channelCreds, callCreds) : channelCreds;
    } else {
      creds = grpc.credentials.createInsecure();
    }
    this.stub = new TapeSvc(target, creds) as Stub;
    this.channel = this.stub.getChannel() as unknown as grpc.Channel;
  }

  close(): void { this.stub.close(); }

  // promisify a unary RPC call
  private call<Req, Res>(method: string, req: Req): Promise<Res> {
    return new Promise((resolve, reject) => {
      this.stub[method](req, (err, res) => err ? reject(err) : resolve(res as Res));
    });
  }

  // streaming RPC -> AsyncIterable<Res>. Pass `timeoutMs` to bound the call
  // with a gRPC deadline — the iterator will surface DEADLINE_EXCEEDED (which
  // we swallow as "end of stream") once the deadline passes. Useful for
  // tick-style consumers (outbox relay) and tests.
  private async *stream<Req, Res>(
    method: string, req: Req, timeoutMs?: number,
  ): AsyncGenerator<Res> {
    const callOpts: grpc.CallOptions = {};
    if (typeof timeoutMs === 'number' && timeoutMs > 0) {
      callOpts.deadline = Date.now() + timeoutMs;
    }
    const s = (this.stub[method] as any)(req, callOpts) as grpc.ClientReadableStream<Res>;
    const queue: Res[] = [];
    let done = false;
    let err: Error | null = null;
    const wakers: Array<() => void> = [];
    s.on('data', (m: Res) => { queue.push(m); const w = wakers.shift(); if (w) w(); });
    s.on('end', () => { done = true; wakers.forEach(w => w()); });
    s.on('error', (e: any) => {
      // Treat DEADLINE_EXCEEDED / CANCELLED as a clean stream end — the
      // call was bounded by the caller's `timeoutMs`, not a real failure.
      const code = e?.code;
      if (code === grpc.status.DEADLINE_EXCEEDED || code === grpc.status.CANCELLED) {
        done = true; wakers.forEach((w) => w()); return;
      }
      err = e; done = true; wakers.forEach((w) => w());
    });
    try {
      while (true) {
        if (err) throw err;
        if (queue.length) { yield queue.shift()!; continue; }
        if (done) return;
        await new Promise<void>((r) => wakers.push(r));
      }
    } finally {
      try { s.cancel(); } catch { /* already done */ }
    }
  }

  // ── run lifecycle ─────────────────────────────────────────────────────────
  // Identity fields (tenantId / actor / subject / agentId / aiplexInstanceId /
  // gatewayRoute / scopes / labels) are populated by AIPlex-managed
  // deployments from the AIPLEX_* env vars; non-AIPlex callers may omit them.
  // See tape/proto/tape.proto §BeginRunRequest.
  beginRun(r: { appName: string; userId: string; sessionId: string; invocationId: string;
                leaseOwner?: string; leaseTtlMs?: number;
                tenantId?: string; actor?: string; subject?: string; agentId?: string;
                aiplexInstanceId?: string; gatewayRoute?: string;
                scopes?: string[]; labels?: Record<string, string> }) {
    return this.call('BeginRun', {
      ...r,
      leaseTtlMs: r.leaseTtlMs ?? 120_000,
      tenantId: r.tenantId ?? '',
      actor: r.actor ?? '',
      subject: r.subject ?? '',
      agentId: r.agentId ?? '',
      aiplexInstanceId: r.aiplexInstanceId ?? '',
      gatewayRoute: r.gatewayRoute ?? '',
      scopes: r.scopes ?? [],
      labels: r.labels ?? {},
    });
  }
  resumeRun(r: { runId: string; leaseOwner?: string; leaseTtlMs?: number }) {
    return this.call('ResumeRun', { ...r, leaseTtlMs: r.leaseTtlMs ?? 120_000 });
  }
  endRun(r: { runId: string; status?: number; detailJson?: string }) {
    return this.call('EndRun', { runId: r.runId, status: r.status ?? RunStatus.TERMINAL, detailJson: r.detailJson ?? '' });
  }
  getRun(runId: string) { return this.call('GetRun', { runId }); }
  listRunsToRecover(r: { limit?: number; nowMs?: number } = {}) {
    return this.call('ListRunsToRecover', { limit: r.limit ?? 100, nowMs: r.nowMs ?? 0 });
  }
  subscribeRun(r: { runId: string; fromSeq?: number; timeoutMs?: number }) {
    return this.stream('SubscribeRun',
      { runId: r.runId, fromSeq: r.fromSeq ?? 0 }, r.timeoutMs);
  }

  // ── decisions ─────────────────────────────────────────────────────────────
  recordDecision(r: { runId: string; decisionIndex: number; model?: string; requestJson?: string; responseJson?: string; rationale?: string; policyVersion?: string }) {
    return this.call('RecordDecision', { model: '', requestJson: '', responseJson: '', rationale: '', policyVersion: '', ...r });
  }
  getDecision(r: { runId: string; decisionIndex: number }) { return this.call('GetDecision', r); }

  // ── effects ───────────────────────────────────────────────────────────────
  //
  // `semantics`, `dispatchMode`, `businessKey`, `connector` opt into the outbox
  // contract (non-idempotent upstreams). Defaults are IDEMPOTENT + INLINE,
  // which preserves the v1 behaviour. The server refuses NON_IDEMPOTENT +
  // INLINE — that error surfaces as a gRPC InvalidArgument / Internal here.
  // Authorization scope (AIPlex integration PR 2) — when non-empty, the
  // server verifies it appears in the run's granted scopes before any
  // effect row is written; on mismatch the call rejects with
  // codes.PermissionDenied.
  beginEffect(r: {
    runId: string; decisionIndex: number; toolName: string;
    callIndex?: number; requestJson?: string; customKey?: string;
    semantics?: number; dispatchMode?: number;
    businessKey?: string; connector?: string;
    scope?: string;
  }) {
    return this.call('BeginEffect', {
      callIndex: 0, requestJson: '', customKey: '',
      semantics: EffectSemantics.UNSPECIFIED, dispatchMode: EffectDispatchMode.UNSPECIFIED,
      businessKey: '', connector: '', scope: '', ...r,
    });
  }
  completeEffect(r: { runId: string; idempotencyKey: string; status: number; responseJson?: string; errorJson?: string }) {
    return this.call('CompleteEffect', { responseJson: '', errorJson: '', ...r });
  }
  getEffect(r: { runId: string; idempotencyKey: string }) { return this.call('GetEffect', r); }
  reconcileEffect(r: { runId: string; idempotencyKey: string; resolvedStatus: number; responseJson?: string; errorJson?: string }) {
    return this.call('ReconcileEffect', { responseJson: '', errorJson: '', ...r });
  }

  // ── outbox dispatch (for non-idempotent upstreams) ────────────────────────

  // PENDING+OUTBOX effects whose next_dispatch_at_ms <= now and whose lease is
  // empty/expired. `connector` scopes the result.
  listEffectsToDispatch(r: { connector?: string; limit?: number; nowMs?: number } = {}) {
    return this.call('ListEffectsToDispatch', { connector: '', limit: 200, nowMs: 0, ...r });
  }
  // Atomic CAS lease. `acquired=false` => another dispatcher won; the loser
  // must not call the upstream.
  claimEffectDispatch(r: { runId: string; idempotencyKey: string; claimer: string; leaseTtlMs?: number }) {
    return this.call('ClaimEffectDispatch', { leaseTtlMs: 60_000, ...r });
  }
  // Failed dispatch. `nextDispatchAtMs <= 0` drives the effect to UNKNOWN
  // (safety exit — no blind retry); a positive value schedules a retry.
  recordDispatchAttempt(r: { runId: string; idempotencyKey: string; error: string; nextDispatchAtMs: number }) {
    return this.call('RecordDispatchAttempt', r);
  }
  // The reconciler's write path. `resolution` is one of EffectResolution.*.
  // DUPLICATE + `compensateOnDuplicateKind` registers a compensation
  // obligation atomically with the observation.
  recordExternalObservation(r: {
    runId: string; idempotencyKey: string; resolution: number;
    externalRef?: string; responseJson?: string; errorJson?: string;
    compensateOnDuplicateKind?: string;
  }) {
    return this.call('RecordExternalObservation', {
      externalRef: '', responseJson: '', errorJson: '',
      compensateOnDuplicateKind: '', ...r,
    });
  }

  // ── obligations ───────────────────────────────────────────────────────────
  //
  // The state machine:
  //   register_compensation  →  PENDING (queued; eligible immediately)
  //   claim_obligation       →  COMMITTED with lease (CAS — one drainer wins)
  //   resolve_obligation     →  COMPENSATED | STUCK (terminal)
  //   record_obligation_attempt → PENDING with backoff (or STUCK if exhausted)
  //
  // `compensatorRef` ("module:attr") lets a generic drainer process resolve
  // the inverse without importing the agent's module. `maxAttempts: 0` uses
  // the server default (5).
  registerCompensation(r: {
    runId: string; effectKey: string; kind: string;
    payloadJson?: string; compensatorRef?: string; maxAttempts?: number;
  }) {
    return this.call('RegisterCompensation', {
      payloadJson: '', compensatorRef: '', maxAttempts: 0, ...r,
    });
  }
  listObligations(r: { runId: string; onlyUnresolved?: boolean; statusFilter?: number }) {
    return this.call('ListObligations', { onlyUnresolved: true, statusFilter: 0, ...r });
  }
  resolveObligation(r: { runId: string; obligationSeq: number; status: number; resultJson?: string }) {
    return this.call('ResolveObligation', { resultJson: '', ...r });
  }
  // Cross-run drainer feed. Defaults (include_pending=true,
  // include_committed_expired=true) match the obligations reactor's hot set.
  listUnresolvedObligations(r: {
    limit?: number; nowMs?: number;
    includePending?: boolean; includeStuck?: boolean; includeCommittedExpired?: boolean;
  } = {}) {
    return this.call('ListUnresolvedObligations', {
      limit: 500, nowMs: 0,
      includePending: true, includeStuck: false, includeCommittedExpired: true, ...r,
    });
  }
  // Atomic lease CAS: returns { acquired, obligation }. `acquired=false` means
  // either someone else holds it or it's not eligible (backoff not elapsed).
  claimObligation(r: { runId: string; obligationSeq: number; claimer: string; leaseTtlMs?: number }) {
    return this.call('ClaimObligation', { leaseTtlMs: 60_000, ...r });
  }
  // Report a failed attempt; the server reschedules (PENDING + backoff) or
  // marks STUCK (when retries are exhausted, or `nextAttemptAtMs <= 0`).
  recordObligationAttempt(r: { runId: string; obligationSeq: number; error: string; nextAttemptAtMs: number }) {
    return this.call('RecordObligationAttempt', r);
  }

  // ── budget ────────────────────────────────────────────────────────────────
  setBudget(r: { runId: string; usdCap?: number; tokenCap?: number }) { return this.call('SetBudget', { usdCap: 0, tokenCap: 0, ...r }); }
  admitBudget(r: { runId: string; usdEstimate?: number; tokenEstimate?: number }) { return this.call('AdmitBudget', { usdEstimate: 0, tokenEstimate: 0, ...r }); }
  chargeBudget(r: { runId: string; usd?: number; tokens?: number }) { return this.call('ChargeBudget', { usd: 0, tokens: 0, ...r }); }

  // ── gates / signals ───────────────────────────────────────────────────────
  awaitSignal(r: { runId: string; gateName: string; payloadJson?: string }) { return this.call('AwaitSignal', { payloadJson: '', ...r }); }
  sendSignal(r: { runId?: string; appName?: string; userId?: string; sessionId?: string; gateName: string; resolutionJson?: string }) {
    return this.call('SendSignal', { runId: '', appName: '', userId: '', sessionId: '', resolutionJson: '', ...r });
  }

  // ── reconciliation ────────────────────────────────────────────────────────
  listPendingEffects(r: { olderThanMs?: number; includePending?: boolean; includeUnknown?: boolean; limit?: number } = {}) {
    return this.call('ListPendingEffects', { olderThanMs: 0, includePending: true, includeUnknown: true, limit: 200, ...r });
  }

  // ── timers ────────────────────────────────────────────────────────────────
  setTimer(r: { runId: string; fireAtMs: number; kind: string; timerId?: string; payloadJson?: string }) {
    return this.call('SetTimer', { timerId: '', payloadJson: '', ...r });
  }
  cancelTimer(r: { runId: string; timerId: string }) { return this.call('CancelTimer', r); }
  listDueTimers(r: { nowMs?: number; limit?: number; claim?: boolean } = {}) {
    return this.call('ListDueTimers', { nowMs: 0, limit: 200, claim: false, ...r });
  }

  // ── WAL tail ──────────────────────────────────────────────────────────────
  subscribeEvents(r: { fromTsMs?: number; runId?: string; kind?: string; fromGlobalSeq?: number; subjectPattern?: string; timeoutMs?: number } = {}) {
    const { timeoutMs, ...req } = r;
    return this.stream('SubscribeEvents', {
      fromTsMs: 0, runId: '', kind: '', fromGlobalSeq: 0, subjectPattern: '', ...req,
    }, timeoutMs);
  }

  // Subject-routed, global-seq-cursored WAL stream. The preferred path for
  // new code; honours `*` (one segment) and `**` (rest) wildcards and an
  // optional CEL predicate evaluated server-side. Pass `timeoutMs` to bound
  // the call (server emits DEADLINE_EXCEEDED, which we surface as a clean
  // end-of-stream — handy for tick-style consumers).
  subscribeBySubject(r: { subjectPattern: string; predicateCel?: string; fromGlobalSeq?: number; timeoutMs?: number }) {
    const { timeoutMs, ...req } = r;
    return this.stream('SubscribeBySubject', {
      predicateCel: '', fromGlobalSeq: 0, ...req,
    }, timeoutMs);
  }

  // ── reactions ─────────────────────────────────────────────────────────────
  registerReaction(r: {
    reactionId?: string; name?: string; subjectPattern: string;
    predicateCel?: string; handlerKind: number; agentApp?: string;
    publishTarget?: string; maxConcurrency?: number; rateLimitPerS?: number;
    debounceMs?: number; retryMax?: number; retryBackoffMs?: number;
    dlqAfterN?: number; numShards?: number; bootstrapFromHead?: boolean;
  }): Promise<any> {
    return this.call('RegisterReaction', {
      reactionId: '', name: '', predicateCel: '', agentApp: '',
      publishTarget: '', maxConcurrency: 1, rateLimitPerS: 0, debounceMs: 0,
      retryMax: 5, retryBackoffMs: 1000, dlqAfterN: 5, numShards: 1,
      bootstrapFromHead: false, ...r,
    });
  }
  async deregisterReaction(reactionId: string): Promise<boolean> {
    const resp: any = await this.call('DeregisterReaction', { reactionId });
    return Boolean(resp?.deregistered);
  }
  async listReactions(subjectPattern: string = ''): Promise<any[]> {
    const resp: any = await this.call('ListReactions', { subjectPattern });
    return (resp?.reactions ?? []) as any[];
  }

  // ── tasks ─────────────────────────────────────────────────────────────────
  async claimTasks(r: {
    reactionId: string; shard?: number; owner: string;
    leaseMs?: number; max?: number; nowMs?: number;
  }): Promise<any[]> {
    const resp: any = await this.call('ClaimTasks', {
      shard: -1, leaseMs: 60_000, max: 16, nowMs: 0, ...r,
    });
    return (resp?.tasks ?? []) as any[];
  }
  async completeTask(r: { taskId: string; owner: string }): Promise<any> {
    const resp: any = await this.call('CompleteTask', r);
    return resp?.task;
  }
  async nackTask(r: { taskId: string; owner: string; error?: string; permanent?: boolean }): Promise<any> {
    const resp: any = await this.call('NackTask', { error: '', permanent: false, ...r });
    return resp?.task;
  }
  async listTasks(r: { reactionId: string; status?: number; limit?: number }): Promise<any[]> {
    const resp: any = await this.call('ListTasks', { status: 0, limit: 200, ...r });
    return (resp?.tasks ?? []) as any[];
  }

  // ── reactive key-value store ──────────────────────────────────────────────
  writeValue(r: { namespace: string; key: string; valueJson: string; ifVersion?: number; writer?: string }) {
    return this.call('WriteValue', { ifVersion: -1, writer: '', ...r });
  }
  getValue(r: { namespace: string; key: string }) {
    return this.call('GetValue', r);
  }
  watchValue(r: { namespace: string; key: string; fromVersion?: number; timeoutMs?: number }) {
    const { timeoutMs, ...req } = r;
    return this.stream('WatchValue', { fromVersion: 0, ...req }, timeoutMs);
  }
  deleteValue(r: { namespace: string; key: string }) {
    return this.call('DeleteValue', r);
  }

  // ── ADK SessionService shim ───────────────────────────────────────────────
  createSession(r: { appName: string; userId: string; sessionId?: string; stateJson?: string }) {
    return this.call('CreateSession', { sessionId: '', stateJson: '{}', ...r });
  }
  getSession(r: { appName: string; userId: string; sessionId: string; maxEvents?: number }) {
    return this.call('GetSession', { maxEvents: 0, ...r });
  }
  listSessions(r: { appName: string; userId: string }) { return this.call('ListSessions', r); }
  deleteSession(r: { appName: string; userId: string; sessionId: string }) { return this.call('DeleteSession', r); }
  appendEvent(r: { appName: string; userId: string; sessionId: string; event: any; stateDeltaJson?: string }) {
    return this.call('AppendEvent', { stateDeltaJson: '{}', ...r });
  }
}
