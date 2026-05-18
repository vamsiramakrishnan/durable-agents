// TapeChaos — fault injection + chaos engineering for durable ADK agents.
//
// Mirrors `tape.chaos` from the Python SDK. The shape: scenarios are
// declarative bundles of (faults, invariants, seed); faults target either
// the server's failpoint catalogue (Phase 0) or a registered connector;
// invariants are predicates over Tape's journal projections — the journal
// is the oracle.
//
// See `design-principles/chaos.md` for the full design.
//
// Headline pattern:
//
//   import * as chaos from 'tape-ts/chaos';
//
//   const scen = chaos.scenario({
//     name: 'bank-wire-survives-crash',
//     seed: 42,
//     faults: [
//       chaos.crash('tape::begin_effect::post_db', { afterN: 1 }),
//       chaos.loseAck({ connector: 'bank.wire', probability: 0.3 }),
//     ],
//     invariants: [chaos.invariants.noStuckObligations],
//   });
//   const sess = chaos.session(scen, { url: 'tape://127.0.0.1:7878' });
//   await sess.enter();
//   try { await runMyAgent(); } finally { await sess.exit(); }
//   console.log(sess.report);

export {
  type Fault,
  type Scenario,
  type ChaosReport,
  type SessionOpts,
  Session,
  crash,
  delay,
  error,
  loseAck,
  duplicate,
  delayConnector,
  scenario,
  session,
  runScenario,
  failpointsEnv,
} from './scenarios.ts';

export * as invariants from './invariants.ts';

export {
  ChaosConnector,
  wrapConnector,
} from './connectors.ts';

export {
  type Snapshot,
  type JournalLine,
  captureSnapshot,
  type DeepSnapshot,
  captureDeep,
} from './snapshot.ts';

export {
  type ReplayReport,
  replay,
} from './replay.ts';

export {
  type LineageNode,
  LineageGraph,
  deriveScenarios,
  type LDFIReport,
  ldfiRunAll,
} from './lineage.ts';

export {
  type ReliabilitySurface,
  Recorder,
  score,
} from './reliability.ts';

export * as proxy from './proxies.ts';
export {
  ChaosProxy,
  modelProxy,
  mcpProxy,
  type ProxyFault,
} from './proxies.ts';
