// End-to-end tests for the event-bus surface (registerReaction, ClaimTasks,
// CompleteTask, NackTask, ListTasks, SubscribeBySubject) and the in-proc
// dispatcher (`runDispatcher` / `on*` helpers). Mirrors the high-level
// coverage of `tape/tests/test_event_bus.py`.
//
// Each test spawns a fresh tape-server (matching the Python suite's
// function-scoped `tape_server` fixture) — the server's matcher tails the
// journal across all reactions, and a per-test fresh server gives the
// deterministic isolation reactions need.
//
// Skips if the Rust tape-server binary isn't built (same pattern as
// client.test.ts).

import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import * as net from 'node:net';
import {
  TapeClient, HandlerKind, TaskStatus,
  on, registerAll, runDispatcher, _clearRegistry,
} from '../src/index.ts';

const __dirname = dirname(fileURLToPath(import.meta.url));
const SERVER_BIN = join(__dirname, '..', '..', '..', 'server', 'target', 'debug', 'tape-server');
const SERVER_AVAILABLE = existsSync(SERVER_BIN);

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.listen(0, '127.0.0.1', () => {
      const p = (s.address() as net.AddressInfo).port;
      s.close(() => resolve(p));
    });
    s.on('error', reject);
  });
}

async function waitFor(host: string, port: number, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ok = await new Promise<boolean>((r) => {
      const s = net.connect(port, host);
      s.on('connect', () => { s.end(); r(true); });
      s.on('error', () => { r(false); });
    });
    if (ok) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`server never came up at ${host}:${port}`);
}

async function waitUntil<T>(
  fn: () => Promise<T> | T,
  timeoutMs = 5000,
  intervalMs = 100,
): Promise<T | null> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = await fn();
    if (v) return v;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  return null;
}

interface Server { proc: ChildProcess; url: string; }

async function spawnServer(): Promise<Server> {
  const port = await freePort();
  const proc = spawn(SERVER_BIN, ['--listen', `127.0.0.1:${port}`, '--store', 'memory'], {
    stdio: 'ignore',
    env: { ...process.env, RUST_LOG: 'tape_server=warn' },
  });
  await waitFor('127.0.0.1', port);
  return { proc, url: `tape://127.0.0.1:${port}` };
}

function killServer(s: Server): void {
  try { s.proc.kill('SIGTERM'); } catch { /* already dead */ }
}

// Wrap a test body that needs a fresh server. Skips cleanly if the binary
// isn't built.
function withServer(name: string, body: (s: Server) => Promise<void>): void {
  test(name, async (t) => {
    if (!SERVER_AVAILABLE) { t.skip(`tape-server not built at ${SERVER_BIN}`); return; }
    _clearRegistry();
    const s = await spawnServer();
    try { await body(s); }
    finally { _clearRegistry(); killServer(s); }
  });
}


// ── 1. Journal entries carry global_seq, subject, schema_version ───────────

withServer('journal entries have non-zero global_seq and non-empty subject',
  async ({ url }) => {
    const c = new TapeClient(url);
    try {
      const begun: any = await c.beginRun({
        appName: 'a', userId: 'u', sessionId: 's-gs',
        invocationId: `inv-${Date.now().toString(36)}`,
        leaseOwner: 'test', leaseTtlMs: 60_000,
      });
      await c.recordDecision({ runId: begun.runId, decisionIndex: 0,
        responseJson: '{"plan":1}' });
      await c.writeValue({ namespace: 'treasury', key: 'fx_rate',
        valueJson: '"1.10"', writer: 'oracle' });

      // give the server time to drain its in-process notify
      await new Promise((r) => setTimeout(r, 200));

      // Drain /tape/** with a short deadline. The server's stream surfaces
      // DEADLINE_EXCEEDED once the deadline passes, which the client treats
      // as a clean end-of-stream.
      const entries: any[] = [];
      for await (const e of c.subscribeBySubject({
        subjectPattern: '/tape/**', fromGlobalSeq: 0, timeoutMs: 800,
      }) as any) {
        entries.push(e);
      }

      assert.ok(entries.length >= 1, `expected at least one /tape/ entry, got ${entries.length}`);
      for (const e of entries) {
        assert.ok(Number(e.globalSeq) > 0, `global_seq must be > 0; got ${e.globalSeq}`);
        assert.ok(String(e.subject).startsWith('/tape/'),
          `subject must start with /tape/; got ${e.subject}`);
        assert.equal(Number(e.schemaVersion), 1,
          `schema_version must default to 1; got ${e.schemaVersion}`);
      }
      const seqs = entries.map((e) => Number(e.globalSeq));
      const sortedSeqs = [...seqs].sort((a, b) => a - b);
      assert.deepEqual(seqs, sortedSeqs, 'global_seq must be monotonic');
    } finally { try { c.close(); } catch { /* */ } }
  });


// ── 2. subscribeBySubject only returns matching entries ────────────────────

withServer('subscribeBySubject only returns matching entries', async ({ url }) => {
  const c = new TapeClient(url);
  try {
    await c.writeValue({ namespace: 'treasury', key: 'k1',
      valueJson: '1', writer: 'tt' });
    await c.writeValue({ namespace: 'other', key: 'k1',
      valueJson: '2', writer: 'tt' });
    await new Promise((r) => setTimeout(r, 200));

    const entries: any[] = [];
    for await (const e of c.subscribeBySubject({
      subjectPattern: '/tape/value/changed/treasury/**', fromGlobalSeq: 0, timeoutMs: 800,
    }) as any) {
      entries.push(e);
    }

    assert.ok(entries.length >= 1, 'expected ≥1 treasury entry');
    for (const e of entries) {
      assert.ok(e.subject.startsWith('/tape/value/changed/treasury/'),
        `got ${e.subject} — subject pattern should filter`);
    }
  } finally { try { c.close(); } catch { /* */ } }
});


// ── 3. Registered task reaction → dispatcher runs handler → complete_task ──

withServer('registered task reaction runs the handler via the dispatcher',
  async ({ url }) => {
    const called: any[] = [];
    on('/tape/value/changed/disp-test/**', (env) => { called.push(env); },
      { maxConcurrency: 2 });

    const c = new TapeClient(url);
    try {
      const rs = await registerAll({ url, prefix: `t${Date.now().toString(36)}-` });
      assert.equal(rs.length, 1, 'one registered reaction');
      const rid = rs[0].reactionId;
      assert.ok(rid, 'server-assigned reaction_id');

      await c.writeValue({ namespace: 'disp-test', key: 'k1',
        valueJson: '{"v":1}', writer: 't' });

      // wait for the matcher to enqueue a task
      const tasks: any = await waitUntil(async () => {
        const ts = await c.listTasks({ reactionId: rid, limit: 10 });
        return ts.length > 0 ? ts : null;
      }, 5000);
      assert.ok(tasks && tasks.length >= 1, `expected ≥1 pending task; got ${tasks?.length}`);

      // run dispatcher once — should claim, run, complete
      await runDispatcher({ url, once: true, register: false, pollIntervalMs: 50 });

      assert.ok(called.length > 0, 'handler should be invoked');
      assert.ok(called[0]?.task?.subject?.includes('disp-test'),
        `envelope.task.subject should contain disp-test; got ${called[0]?.task?.subject}`);
      // Payload is the journal entry's payload_json; for a value-write, the
      // server packages `{namespace, key, value: {namespace, key, value_json, version}, ...}`.
      assert.equal(called[0]?.payload?.namespace, 'disp-test');
      assert.equal(called[0]?.payload?.value?.value_json, '{"v":1}');

      // task should be DONE
      const done = await waitUntil(async () => {
        const ts = await c.listTasks({ reactionId: rid, status: TaskStatus.DONE, limit: 10 });
        return ts.length > 0 ? ts : null;
      }, 4000);
      assert.ok(done && done.length > 0, 'task should be DONE');
    } finally { c.close(); }
  });


// ── 4. bootstrap_from_head skips backlog ───────────────────────────────────

withServer('bootstrap_from_head skips the existing backlog', async ({ url }) => {
  const c = new TapeClient(url);
  try {
    // Write several pre-existing entries to a namespace.
    const ns = 'bootstrap-test';
    for (let i = 0; i < 3; i++) {
      await c.writeValue({ namespace: ns, key: `k${i}`,
        valueJson: `${i}`, writer: 't' });
    }
    await new Promise((r) => setTimeout(r, 200));

    // Register a reaction AFTER the backlog with bootstrap_from_head=true.
    // The flag must be propagated through the gRPC layer (TS SDK
    // responsibility) and honoured by the server's cursor-seed logic — the
    // reaction must not see any of the 3 pre-existing matching entries.
    const r: any = await c.registerReaction({
      name: 'after-only',
      subjectPattern: `/tape/value/changed/${ns}/**`,
      handlerKind: HandlerKind.TASK,
      bootstrapFromHead: true,
    });
    assert.ok(r.reactionId, 'server-assigned reaction_id');

    // Give the matcher a beat to (not) process the backlog.
    await new Promise((rr) => setTimeout(rr, 1000));

    const backlogTasks = await c.listTasks({ reactionId: r.reactionId, limit: 50 });
    assert.equal(backlogTasks.length, 0,
      `bootstrap_from_head should skip backlog; got ${backlogTasks.length} tasks`);
  } finally { c.close(); }
});


// ── 5. listReactions returns what we registered ────────────────────────────

withServer('listReactions returns registered reactions; deregisterReaction works',
  async ({ url }) => {
    const c = new TapeClient(url);
    try {
      const pattern = `/tape/effect/confirmed/list-test/**`;
      const r: any = await c.registerReaction({
        name: 'list-me',
        subjectPattern: pattern,
        handlerKind: HandlerKind.TASK,
      });
      const reactions = await c.listReactions(pattern);
      assert.ok(reactions.some((x: any) => x.reactionId === r.reactionId),
        'just-registered reaction should appear in listReactions');

      const ok = await c.deregisterReaction(r.reactionId);
      assert.equal(ok, true, 'deregisterReaction returns true on success');

      const after = await c.listReactions(pattern);
      assert.equal(after.length, 0,
        'deregistered reaction should not appear in listReactions');
    } finally { c.close(); }
  });
