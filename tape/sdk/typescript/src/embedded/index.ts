// Public re-exports for the embedded SQL path. Mirrors the surface of
// `tape_adk` in the Python package: schema + service + connectors + reactors
// + decorators. No ADK integration (ADK-TS does not exist).
//
// Usage:
//   import Database from 'better-sqlite3';
//   import { adaptBetterSqlite3, TapeSessionService } from 'tape-ts/embedded';
//   const svc = new TapeSessionService(adaptBetterSqlite3(new Database(':memory:')));
//   await svc.beginEffect({ … });

export {
  createAllTables, adaptBetterSqlite3,
  type EmbeddedDb, type EmbeddedStatement,
  type EffectRow, type ObligationRow, type TimerRow, type ValueRow,
} from './schema.ts';

export {
  TapeSessionService,
  EffectStatus, EffectSemantics, EffectDispatchMode, EffectResolution,
  ObligationStatus,
  type EffectStatusT, type EffectSemanticsT, type EffectDispatchModeT,
  type EffectResolutionT, type ObligationStatusT,
  type EffectRecord, type ObligationRecord, type TimerRecord, type ValueRecord,
} from './service.ts';

export {
  LogConnector,
  type Connector,
  type DispatchResult, type ObservationResult, type CompensationResult,
} from './connectors.ts';

export {
  dispatchOutboxOnce, reconcileOnce, drainObligationsOnce, fireDueTimersOnce,
  type TickAuditEntry,
} from './reactors.ts';

export {
  effect, outboxTool, metaOf,
  type EffectMeta,
} from './decorators.ts';
