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
export const ObligationStatus = Object.freeze({
  UNSPECIFIED: 0, PENDING: 1, COMMITTED: 2, COMPENSATED: 3, STUCK: 4,
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
        const headers = await client.getRequestHeaders(audience);
        const bearer = headers['Authorization'] ?? headers['authorization'];
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
    this.channel = this.stub.getChannel();
  }

  close(): void { this.stub.close(); }

  // promisify a unary RPC call
  private call<Req, Res>(method: string, req: Req): Promise<Res> {
    return new Promise((resolve, reject) => {
      this.stub[method](req, (err, res) => err ? reject(err) : resolve(res as Res));
    });
  }

  // streaming RPC -> AsyncIterable<Res>
  private async *stream<Req, Res>(method: string, req: Req): AsyncGenerator<Res> {
    const s = this.stub[method](req) as unknown as grpc.ClientReadableStream<Res>;
    const queue: Res[] = [];
    let done = false;
    let err: Error | null = null;
    const wakers: Array<() => void> = [];
    s.on('data', (m: Res) => { queue.push(m); const w = wakers.shift(); if (w) w(); });
    s.on('end', () => { done = true; wakers.forEach(w => w()); });
    s.on('error', (e: Error) => { err = e; done = true; wakers.forEach(w => w()); });
    while (true) {
      if (err) throw err;
      if (queue.length) { yield queue.shift()!; continue; }
      if (done) return;
      await new Promise<void>((r) => wakers.push(r));
    }
  }

  // ── run lifecycle ─────────────────────────────────────────────────────────
  beginRun(r: { appName: string; userId: string; sessionId: string; invocationId: string; leaseOwner?: string; leaseTtlMs?: number }) {
    return this.call('BeginRun', { ...r, leaseTtlMs: r.leaseTtlMs ?? 120_000 });
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
  subscribeRun(r: { runId: string; fromSeq?: number }) {
    return this.stream('SubscribeRun', { runId: r.runId, fromSeq: r.fromSeq ?? 0 });
  }

  // ── decisions ─────────────────────────────────────────────────────────────
  recordDecision(r: { runId: string; decisionIndex: number; model?: string; requestJson?: string; responseJson?: string; rationale?: string; policyVersion?: string }) {
    return this.call('RecordDecision', { model: '', requestJson: '', responseJson: '', rationale: '', policyVersion: '', ...r });
  }
  getDecision(r: { runId: string; decisionIndex: number }) { return this.call('GetDecision', r); }

  // ── effects ───────────────────────────────────────────────────────────────
  beginEffect(r: { runId: string; decisionIndex: number; toolName: string; callIndex?: number; requestJson?: string; customKey?: string }) {
    return this.call('BeginEffect', { callIndex: 0, requestJson: '', customKey: '', ...r });
  }
  completeEffect(r: { runId: string; idempotencyKey: string; status: number; responseJson?: string; errorJson?: string }) {
    return this.call('CompleteEffect', { responseJson: '', errorJson: '', ...r });
  }
  getEffect(r: { runId: string; idempotencyKey: string }) { return this.call('GetEffect', r); }
  reconcileEffect(r: { runId: string; idempotencyKey: string; resolvedStatus: number; responseJson?: string; errorJson?: string }) {
    return this.call('ReconcileEffect', { responseJson: '', errorJson: '', ...r });
  }

  // ── obligations ───────────────────────────────────────────────────────────
  registerCompensation(r: { runId: string; effectKey: string; kind: string; payloadJson?: string }) {
    return this.call('RegisterCompensation', { payloadJson: '', ...r });
  }
  listObligations(r: { runId: string; onlyUnresolved?: boolean }) {
    return this.call('ListObligations', { onlyUnresolved: true, ...r });
  }
  resolveObligation(r: { runId: string; obligationSeq: number; status: number; resultJson?: string }) {
    return this.call('ResolveObligation', { resultJson: '', ...r });
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
  subscribeEvents(r: { fromTsMs?: number; runId?: string; kind?: string } = {}) {
    return this.stream('SubscribeEvents', { fromTsMs: 0, runId: '', kind: '', ...r });
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
