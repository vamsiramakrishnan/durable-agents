// Agent-layer chaos proxies — Toxiproxy-for-LLM and -MCP.
//
// Mirrors `tape.chaos.proxies` from the Python SDK. Stdlib-only (`node:http`,
// `node:https`) — no extra dependency. The proxy is plaintext-to-agent;
// it upgrades to TLS on the way out if the upstream is `https://`.
//
//   import * as chaos from 'tape-ts/chaos';
//
//   const p = chaos.modelProxy('https://api.anthropic.com', [
//     chaos.proxy.delay({ pathPrefix: '/v1/messages', ms: 2_000, probability: 0.1 }),
//     chaos.proxy.injectStatus({ pathPrefix: '/v1/messages', status: 429, probability: 0.05 }),
//   ]);
//   await p.start();   // listens on 127.0.0.1:auto-port
//   // point your LLM client's baseUrl at p.url
//   await p.stop();

import * as http from 'node:http';
import * as https from 'node:https';
import { URL } from 'node:url';
import type { AddressInfo } from 'node:net';

export type FaultKind =
  | 'delay'
  | 'inject_status'
  | 'truncate_stream'
  | 'mangle_json'
  | 'inject_prompt'
  | 'tool_shadow'
  | 'schema_drift'
  | 'drop_connection';

export interface ProxyFault {
  readonly kind: FaultKind;
  readonly pathPrefix: string;
  readonly probability: number;
  readonly ms: number;
  readonly jitter: number;
  readonly status: number;
  readonly body: string;
  readonly atEvent: number;
  readonly jsonPath: string;
  readonly replacement: unknown;
  readonly suffix: string;
  readonly extraTool: Record<string, unknown> | null;
  readonly driftFn: ((payload: unknown) => unknown) | null;
}

function fault(partial: Partial<ProxyFault> & { kind: FaultKind }): ProxyFault {
  return {
    pathPrefix: '', probability: 1.0, ms: 0, jitter: 0,
    status: 0, body: '', atEvent: 0, jsonPath: '',
    replacement: null, suffix: '', extraTool: null, driftFn: null,
    ...partial,
  };
}

// ── fault constructors ─────────────────────────────────────────────────────

export function delay(opts: { pathPrefix?: string; ms: number; jitter?: number; probability?: number }): ProxyFault {
  return fault({ kind: 'delay', pathPrefix: opts.pathPrefix ?? '', ms: opts.ms,
                  jitter: opts.jitter ?? 0, probability: opts.probability ?? 1.0 });
}

export function injectStatus(opts: { pathPrefix?: string; status: number; body?: string; probability?: number }): ProxyFault {
  return fault({ kind: 'inject_status', pathPrefix: opts.pathPrefix ?? '',
                  status: opts.status, body: opts.body ?? '',
                  probability: opts.probability ?? 1.0 });
}

export function truncateStream(opts: { pathPrefix?: string; atEvent: number; probability?: number }): ProxyFault {
  return fault({ kind: 'truncate_stream', pathPrefix: opts.pathPrefix ?? '',
                  atEvent: opts.atEvent, probability: opts.probability ?? 1.0 });
}

export function mangleJson(opts: { pathPrefix?: string; jsonPath?: string; replacement?: unknown; probability?: number }): ProxyFault {
  return fault({ kind: 'mangle_json', pathPrefix: opts.pathPrefix ?? '',
                  jsonPath: opts.jsonPath ?? '', replacement: opts.replacement ?? null,
                  probability: opts.probability ?? 1.0 });
}

export function injectPrompt(opts: { pathPrefix?: string; suffix: string; probability?: number }): ProxyFault {
  return fault({ kind: 'inject_prompt', pathPrefix: opts.pathPrefix ?? '',
                  suffix: opts.suffix, probability: opts.probability ?? 1.0 });
}

export function toolShadow(opts: { pathPrefix?: string; extraTool: Record<string, unknown>; probability?: number }): ProxyFault {
  return fault({ kind: 'tool_shadow', pathPrefix: opts.pathPrefix ?? '',
                  extraTool: opts.extraTool, probability: opts.probability ?? 1.0 });
}

export function schemaDrift(opts: { pathPrefix?: string; driftFn: (payload: unknown) => unknown; probability?: number }): ProxyFault {
  return fault({ kind: 'schema_drift', pathPrefix: opts.pathPrefix ?? '',
                  driftFn: opts.driftFn, probability: opts.probability ?? 1.0 });
}

export function dropConnection(opts: { pathPrefix?: string; probability?: number } = {}): ProxyFault {
  return fault({ kind: 'drop_connection', pathPrefix: opts.pathPrefix ?? '',
                  probability: opts.probability ?? 1.0 });
}

// ── helpers ────────────────────────────────────────────────────────────────

function setJsonAt(obj: any, path: string, value: unknown): any {
  if (!path) return value;
  const parts = path.split('.');
  let cur: any = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    const p = parts[i];
    try {
      if (Array.isArray(cur) && /^\d+$/.test(p)) cur = cur[Number(p)];
      else if (cur && typeof cur === 'object') cur = cur[p];
      else return obj;
    } catch { return obj; }
  }
  const last = parts[parts.length - 1];
  try {
    if (Array.isArray(cur) && /^\d+$/.test(last)) cur[Number(last)] = value;
    else if (cur && typeof cur === 'object') cur[last] = value;
  } catch { /* ignore */ }
  return obj;
}

function injectPromptInto(obj: any, suffix: string): any {
  if (Array.isArray(obj)) for (const v of obj) injectPromptInto(v, suffix);
  else if (obj && typeof obj === 'object') {
    for (const k of ['text', 'content', 'output_text']) {
      if (typeof obj[k] === 'string') obj[k] = obj[k] + suffix;
    }
    for (const v of Object.values(obj)) injectPromptInto(v, suffix);
  }
  return obj;
}

function shadowTools(obj: any, extra: Record<string, unknown>): any {
  if (Array.isArray(obj)) for (const v of obj) shadowTools(v, extra);
  else if (obj && typeof obj === 'object') {
    for (const [k, v] of Object.entries(obj)) {
      if (k === 'tools' && Array.isArray(v)) (v as unknown[]).push({ ...extra });
      else shadowTools(v, extra);
    }
  }
  return obj;
}

const DROP_REQUEST_HEADERS = new Set([
  'host', 'content-length', 'connection', 'keep-alive',
  'proxy-authenticate', 'proxy-authorization', 'te', 'trailers',
  'transfer-encoding', 'upgrade',
]);

// ── ChaosProxy ─────────────────────────────────────────────────────────────

export class ChaosProxy {
  readonly upstream: string;
  readonly faults: readonly ProxyFault[];
  private readonly rng: () => number;
  private readonly timeoutMs: number;
  private server: http.Server | null = null;
  private _host = '';
  private _port = 0;
  readonly faultHits = new Map<string, number>();

  constructor(upstream: string, faults: readonly ProxyFault[] = [],
              opts: { rng?: () => number; timeoutMs?: number } = {}) {
    this.upstream = upstream.replace(/\/$/, '');
    this.faults = faults;
    this.rng = opts.rng ?? Math.random;
    this.timeoutMs = opts.timeoutMs ?? 60_000;
  }

  get url(): string {
    return `http://${this._host}:${this._port}`;
  }

  async start(opts: { host?: string; port?: number } = {}): Promise<void> {
    const server = http.createServer((req, res) => this.handle(req, res));
    await new Promise<void>((resolve) => {
      server.listen(opts.port ?? 0, opts.host ?? '127.0.0.1', () => resolve());
    });
    const addr = server.address() as AddressInfo;
    this._host = addr.address; this._port = addr.port;
    this.server = server;
  }

  async stop(): Promise<void> {
    if (!this.server) return;
    await new Promise<void>((resolve, reject) => {
      this.server!.close((err) => err ? reject(err) : resolve());
    });
    this.server = null;
  }

  private matching(path: string, kind: FaultKind): ProxyFault[] {
    const out: ProxyFault[] = [];
    for (const f of this.faults) {
      if (f.kind !== kind) continue;
      if (f.pathPrefix && !path.startsWith(f.pathPrefix)) continue;
      if (f.probability >= 1.0 || this.rng() < f.probability) {
        out.push(f);
        const key = `${kind}:${f.pathPrefix}`;
        this.faultHits.set(key, (this.faultHits.get(key) ?? 0) + 1);
      }
    }
    return out;
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const path = req.url ?? '/';

    // PRE-FORWARD faults: delay, inject_status.
    for (const f of this.matching(path, 'delay')) {
      let ms = f.ms;
      if (f.jitter > 0) ms = Math.max(0, Math.floor(ms * (1.0 + (this.rng() * 2 - 1) * f.jitter)));
      await new Promise(r => setTimeout(r, ms));
    }
    const injected = this.matching(path, 'inject_status');
    if (injected.length) {
      const f = injected[0];
      const body = Buffer.from(f.body, 'utf-8');
      res.writeHead(f.status, {
        'Content-Type': 'text/plain; charset=utf-8',
        'Content-Length': body.length,
        'X-Tape-Chaos': 'inject_status',
      });
      res.end(body);
      return;
    }

    // Read the request body fully (proxies are lower-throughput; OK).
    const chunks: Buffer[] = [];
    for await (const ch of req) chunks.push(ch as Buffer);
    const body = Buffer.concat(chunks);

    // Forward.
    const target = new URL(this.upstream + path);
    const lib = target.protocol === 'https:' ? https : http;
    const upstreamHeaders: Record<string, string> = {};
    for (const [k, v] of Object.entries(req.headers)) {
      if (DROP_REQUEST_HEADERS.has(k.toLowerCase())) continue;
      if (v === undefined) continue;
      upstreamHeaders[k] = Array.isArray(v) ? v.join(',') : String(v);
    }
    if (body.length) upstreamHeaders['content-length'] = String(body.length);

    let upstreamRes: http.IncomingMessage;
    try {
      upstreamRes = await new Promise<http.IncomingMessage>((resolve, reject) => {
        const r = lib.request({
          method: req.method, hostname: target.hostname,
          port: target.port || (target.protocol === 'https:' ? 443 : 80),
          path: target.pathname + target.search,
          headers: upstreamHeaders, timeout: this.timeoutMs,
        }, resolve);
        r.on('error', reject);
        r.on('timeout', () => { r.destroy(new Error('upstream timeout')); });
        if (body.length) r.write(body);
        r.end();
      });
    } catch (ex) {
      res.writeHead(502, { 'X-Tape-Chaos': 'upstream-unreachable' });
      res.end(`upstream unreachable: ${ex instanceof Error ? ex.message : String(ex)}`);
      return;
    }

    const ctype = (upstreamRes.headers['content-type'] ?? '').toString();
    if (ctype.startsWith('text/event-stream')) {
      await this.replyStream(path, upstreamRes, res);
    } else if (ctype.startsWith('application/json')) {
      await this.replyJson(path, upstreamRes, res);
    } else {
      await this.replyPassthrough(path, upstreamRes, res);
    }
  }

  private sendHeaders(upstreamRes: http.IncomingMessage, res: http.ServerResponse,
                       opts: { overrideLength?: number; extra?: Record<string, string> } = {}): void {
    const headers: Record<string, string | number> = {};
    for (const [k, v] of Object.entries(upstreamRes.headers)) {
      const lk = k.toLowerCase();
      if (lk === 'transfer-encoding' || lk === 'content-encoding'
          || lk === 'connection' || lk === 'keep-alive') continue;
      if (opts.overrideLength !== undefined && lk === 'content-length') continue;
      if (v !== undefined) headers[k] = Array.isArray(v) ? v.join(',') : String(v);
    }
    if (opts.overrideLength !== undefined) headers['Content-Length'] = opts.overrideLength;
    if (opts.extra) Object.assign(headers, opts.extra);
    res.writeHead(upstreamRes.statusCode ?? 200, headers);
  }

  private async replyPassthrough(path: string, upstreamRes: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const chunks: Buffer[] = [];
    for await (const ch of upstreamRes) chunks.push(ch as Buffer);
    const data = Buffer.concat(chunks);
    const drops = this.matching(path, 'drop_connection');
    this.sendHeaders(upstreamRes, res, { overrideLength: data.length, extra: { 'X-Tape-Chaos': 'passthrough' } });
    if (drops.length) { res.end(); return; }
    res.end(data);
  }

  private async replyJson(path: string, upstreamRes: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const chunks: Buffer[] = [];
    for await (const ch of upstreamRes) chunks.push(ch as Buffer);
    const data = Buffer.concat(chunks);
    let payload: any;
    try { payload = JSON.parse(data.toString('utf-8')); }
    catch {
      this.sendHeaders(upstreamRes, res, { overrideLength: data.length });
      res.end(data); return;
    }
    const applied: string[] = [];
    for (const f of this.matching(path, 'mangle_json')) {
      payload = setJsonAt(payload, f.jsonPath, f.replacement);
      applied.push('mangle_json');
    }
    for (const f of this.matching(path, 'inject_prompt')) {
      payload = injectPromptInto(payload, f.suffix); applied.push('inject_prompt');
    }
    for (const f of this.matching(path, 'tool_shadow')) {
      if (f.extraTool) { payload = shadowTools(payload, { ...f.extraTool }); applied.push('tool_shadow'); }
    }
    for (const f of this.matching(path, 'schema_drift')) {
      if (f.driftFn) {
        try { payload = f.driftFn(payload); }
        catch (ex) { console.warn('proxy: schemaDrift threw:', ex); }
        applied.push('schema_drift');
      }
    }
    const drops = this.matching(path, 'drop_connection');
    const newBody = Buffer.from(JSON.stringify(payload), 'utf-8');
    this.sendHeaders(upstreamRes, res, {
      overrideLength: newBody.length,
      extra: { 'X-Tape-Chaos': applied.join(',') || 'json' },
    });
    if (drops.length) { res.end(); return; }
    res.end(newBody);
  }

  private async replyStream(path: string, upstreamRes: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const truncates = this.matching(path, 'truncate_stream');
    const cutAt = Math.min(...(truncates.length ? truncates.map(t => t.atEvent) : [0]));
    const drops = this.matching(path, 'drop_connection');
    this.sendHeaders(upstreamRes, res, {
      extra: { 'X-Tape-Chaos': truncates.length ? 'truncate_stream'
                                : drops.length ? 'drop_connection' : 'sse' },
    });
    let eventCount = 0;
    let buf = Buffer.alloc(0);
    for await (const chunk of upstreamRes) {
      buf = Buffer.concat([buf, chunk as Buffer]);
      while (true) {
        const idx = buf.indexOf('\n\n');
        if (idx < 0) break;
        const evt = buf.slice(0, idx + 2);
        buf = buf.slice(idx + 2);
        eventCount++;
        if (!res.write(evt)) await new Promise<void>(r => res.once('drain', r));
        if (cutAt && eventCount >= cutAt) { res.end(); return; }
      }
      if (drops.length && eventCount >= 1) { res.end(); return; }
    }
    res.end();
  }
}

// ── convenience constructors ───────────────────────────────────────────────

/** A `ChaosProxy` tuned for an LLM provider's `baseUrl` (Anthropic/OpenAI/Vertex). */
export function modelProxy(upstream: string, faults: readonly ProxyFault[] = [],
                            opts: { rng?: () => number; timeoutMs?: number } = {}): ChaosProxy {
  return new ChaosProxy(upstream, faults, opts);
}

/** A `ChaosProxy` tuned for an MCP server's HTTP/SSE endpoint. */
export function mcpProxy(upstream: string, faults: readonly ProxyFault[] = [],
                          opts: { rng?: () => number; timeoutMs?: number } = {}): ChaosProxy {
  return new ChaosProxy(upstream, faults, opts);
}
