export {
  TapeClient, DEFAULT_URL, RunStatus, EffectStatus, ObligationStatus,
  // Outbox / non-idempotent contract — opt in via beginEffect's new fields.
  EffectSemantics, EffectDispatchMode, EffectResolution,
  // Event-bus surface.
  HandlerKind, TaskStatus,
  type ClientOptions,
} from './client.ts';
export {
  effect, effectMetaOf, idempotencyKey, runIdOf, registerCompensator, getCompensator,
  registerStatusCheck, getStatusCheck, AckLost,
  type RetryPolicy, type EffectMeta,
} from './effect.ts';
export {
  recoverOnce, reconcileOnce, fireDueTimersOnce, runReactors, runEventFanout,
  type RedriveFn, type RunReactorsOptions,
} from './reactors.ts';
export {
  outboxDispatchOnce, runOutboxDispatcher, dispatchOne,
  type OutboxReactorOptions, type RunOutboxOptions, type OutboxOutcome,
} from './outbox_reactor.ts';
export {
  LogSink, WebhookSink, PubSubSink, FnSink,
  type Sink, type WebhookSinkOpts, type PubSubSinkOpts,
} from './sinks.ts';
export {
  on, onValueChange, onValueDeleted, onEffectConfirmed, onEffectFailed,
  onEffectUnknown, onDecisionRecorded, onGate, onRun,
  registerAll, runDispatcher, runPubSubBridge,
  getRegistry, _clearRegistry,
  type ReactionOptions, type ReactionHandler, type ReactionDef,
  type ReactionEnvelope, type Reaction, type Task,
} from './reactions.ts';

// ── Standalone DX (parity with Python's tape.adk.durable_app / @tape.outbox_tool /
//    tape.connectors / tape.obs / tape.tenancy) ───────────────────────────────
export {
  durableApp,
  type Budget, type DurableApp, type DurableAppConfig,
} from './durable.ts';
export {
  outboxTool, outboxMetaOf, isOutboxEnvelope, OutboxConfigError,
  type OutboxSemantics, type OutboxToolOpts, type OutboxEnvelope,
} from './outbox.ts';
export {
  CONNECTORS, ConnectorRegistry,
  LogConnector, HttpConnector, PubSubConnector, CloudTasksConnector,
  type Connector,
  type DispatchOutcome, type ObservationOutcome, type CompensationOutcome,
  type EffectRecord, type ObligationRecord,
  type DispatchResult, type ObservationResult, type CompensationResult,
  type HttpConnectorOpts, type PubSubConnectorOpts, type CloudTasksConnectorOpts,
} from './connectors/index.ts';
export {
  logJson, span, setSpanHook, ALL_SPANS, STRUCTURED_FIELDS,
  SPAN_BEGIN_RUN, SPAN_RESUME_RUN, SPAN_RECORD_DECISION,
  SPAN_BEGIN_EFFECT, SPAN_COMPLETE_EFFECT,
  SPAN_RECONCILE_EFFECT, SPAN_DISPATCH_EFFECT,
  SPAN_COMPENSATE, SPAN_REDRIVE,
  SPAN_AWAIT_SIGNAL, SPAN_SEND_SIGNAL,
  type SpanEnd, type SpanHook,
} from './obs.ts';
export {
  tenancyDefaults, tenancyFromEnv, tenancyFromObject, isHard, warnIfHardButUnenforced,
  type TenancyMode, type TenancyConfig,
} from './tenancy.ts';

// TapeChaos — fault injection + chaos engineering surface. Optional;
// importing tape-ts does not pull `tape-ts/chaos` in. Use it via:
//
//   import * as chaos from 'tape-ts/chaos';
//
// See `design-principles/chaos.md` for the design.
export * as chaos from './chaos/index.ts';
