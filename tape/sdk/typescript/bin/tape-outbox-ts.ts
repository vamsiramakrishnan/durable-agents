#!/usr/bin/env node
// tape-outbox-ts — TypeScript counterpart of `tape-outbox` (Go) and
// `python -m tape.reactors.outbox` (Python).
//
// Drives one (or every) pass of the outbox dispatcher: list PENDING+OUTBOX
// effects, claim each, dispatch via the registered connector, record the
// outcome. The cross-SDK parity test in tape/tests/parity exercises this
// shim against the same Tape server as the Python/Go/Java counterparts;
// keep its flag surface aligned with the Go CLI so a `make sdk-parity`
// pass means "all four spoke the same protocol."
//
// Usage:
//   node --experimental-strip-types --no-warnings bin/tape-outbox-ts.ts \
//        --url tape://localhost:7878 [--once] [--connector NAME] \
//        [--interval-ms 1000] [--max-attempts 5] [--claimer ID] \
//        [--register-log-connector --log-connector-path /path/out.jsonl]
//
// `--register-log-connector` plants the built-in LogConnector under the
// name `log`, which is what the parity harness's `make_pending_outbox_effect`
// expects. Real deployments register their own connectors before invoking
// this CLI (or call `runOutboxDispatcher` directly from an agent process).

import {
  CONNECTORS, LogConnector,
} from '../src/connectors/index.ts';
import {
  outboxDispatchOnce, runOutboxDispatcher, type OutboxOutcome,
} from '../src/outbox_reactor.ts';

interface Args {
  url: string;
  connector?: string;
  intervalMs: number;
  maxAttempts: number;
  claimer?: string;
  once: boolean;
  registerLog: boolean;
  logPath: string;
}

// Tiny flag parser — no external dep so the shim is `node --strip-types`-runnable
// without a build. Two forms accepted per option: `--foo bar` and `--foo=bar`.
function parseArgs(argv: string[]): Args {
  const out: Args = {
    url: 'tape://localhost:7878',
    intervalMs: 1000,
    maxAttempts: 5,
    once: false,
    registerLog: false,
    logPath: '/tmp/tape-outbox.jsonl',
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const take = (): string => {
      const eq = a.indexOf('=');
      if (eq >= 0) return a.slice(eq + 1);
      const v = argv[++i];
      if (v === undefined) throw new Error(`flag ${a} expects a value`);
      return v;
    };
    if (a === '--url' || a.startsWith('--url=')) out.url = take();
    else if (a === '--connector' || a.startsWith('--connector=')) out.connector = take();
    else if (a === '--interval-ms' || a.startsWith('--interval-ms=')) out.intervalMs = parseInt(take(), 10);
    else if (a === '--max-attempts' || a.startsWith('--max-attempts=')) out.maxAttempts = parseInt(take(), 10);
    else if (a === '--claimer' || a.startsWith('--claimer=')) out.claimer = take();
    else if (a === '--once') out.once = true;
    else if (a === '--register-log-connector') out.registerLog = true;
    else if (a === '--log-connector-path' || a.startsWith('--log-connector-path=')) out.logPath = take();
    else if (a === '--help' || a === '-h') { printHelp(); process.exit(0); }
    else { process.stderr.write(`tape-outbox-ts: unknown flag ${a}\n`); process.exit(2); }
  }
  return out;
}

function printHelp(): void {
  process.stdout.write(
    'tape-outbox-ts — outbox dispatcher (TypeScript)\n\n' +
    '  --url URL                    Tape server URL (default tape://localhost:7878)\n' +
    '  --connector NAME             restrict to one connector\n' +
    '  --once                       run one pass and exit\n' +
    '  --interval-ms MS             poll interval in daemon mode (default 1000)\n' +
    '  --max-attempts N             give up after N (default 5)\n' +
    '  --claimer ID                 identity for dispatch_claimed_by\n' +
    '  --register-log-connector     register the built-in LogConnector\n' +
    '  --log-connector-path PATH    where LogConnector writes (default /tmp/tape-outbox.jsonl)\n');
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));

  if (args.registerLog) {
    CONNECTORS.register('log', new LogConnector(args.logPath));
  }

  const onTick = (outs: OutboxOutcome[]): void => {
    if (outs.length === 0) return;
    process.stdout.write(JSON.stringify({ outbox: outs }) + '\n');
  };

  if (args.once) {
    const outs = await outboxDispatchOnce({
      url: args.url,
      connector: args.connector,
      claimer: args.claimer,
      dispatchMaxAttempts: args.maxAttempts,
    });
    onTick(outs);
    return;
  }

  // Daemon mode — loops forever inside `runOutboxDispatcher`. Process
  // signals (SIGINT/SIGTERM) currently terminate the process directly;
  // a cleaner shutdown would require the reactor to accept an
  // AbortSignal (parity gap with the Go CLI's context.NotifyContext).
  await runOutboxDispatcher({
    url: args.url,
    connector: args.connector,
    claimer: args.claimer,
    dispatchMaxAttempts: args.maxAttempts,
    intervalMs: args.intervalMs,
    onTick,
  });
}

main().catch((err) => {
  process.stderr.write(`tape-outbox-ts: ${err?.stack ?? err}\n`);
  process.exit(1);
});
