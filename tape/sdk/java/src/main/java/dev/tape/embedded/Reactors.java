package dev.tape.embedded;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static dev.tape.embedded.Schema.EffectRecord;
import static dev.tape.embedded.Schema.ObligationRecord;
import static dev.tape.embedded.Schema.TimerRecord;

/**
 * Four reactor functions over {@link TapeSessionService} — Java port of
 * {@code tape_adk.reactors}.
 *
 * <p>Where the Rust {@code tape-server} runs reactors as separate
 * processes, the embedded form ships them as library functions the
 * operator runs from any container — a thread in the JVM, a Cloud Run
 * Job, a Kubernetes CronJob, whatever. Each {@code *_once} does at most
 * {@code limit} items per tick so a busy loop self-rate-limits naturally.
 *
 * <p>Crash-safety is built in: claims have TTLs, so a process that dies
 * mid-tick releases its work to the next runner.
 */
public final class Reactors {

    private Reactors() {}

    /** A small audit record per item touched by a reactor tick. */
    public record ReactorAction(
            String kind,             // "dispatch" | "reconcile" | "drain" | "timer"
            String idempotencyKey,   // for effect-touched actions
            Long seq,                // for obligation-touched actions
            String timerId,          // for timer-touched actions
            String outcome,          // human-readable outcome tag
            String note,             // optional skip reason / backoff detail
            Long backoffMs) {

        public static ReactorAction effect(String kind, String key, String outcome) {
            return new ReactorAction(kind, key, null, null, outcome, null, null);
        }
        public static ReactorAction effectSkip(String kind, String key, String reason) {
            return new ReactorAction(kind, key, null, null, "skip", reason, null);
        }
        public static ReactorAction effectBackoff(String kind, String key, String outcome, long backoffMs) {
            return new ReactorAction(kind, key, null, null, outcome, null, backoffMs);
        }
        public static ReactorAction obligation(String kind, long seq, String outcome) {
            return new ReactorAction(kind, null, seq, null, outcome, null, null);
        }
        public static ReactorAction obligationSkip(long seq, String reason) {
            return new ReactorAction("drain", null, seq, null, "skip", reason, null);
        }
        public static ReactorAction obligationBackoff(long seq, String outcome, long backoffMs) {
            return new ReactorAction("drain", null, seq, null, outcome, null, backoffMs);
        }
        public static ReactorAction timer(String timerId, String outcome) {
            return new ReactorAction("timer", null, null, timerId, outcome, null, null);
        }
    }

    public static final long DEFAULT_LEASE_TTL_MS = 60_000L;
    public static final long DEFAULT_BACKOFF_MS = 5_000L;
    public static final long MAX_BACKOFF_MS = 300_000L;

    private static long nowMs() { return System.currentTimeMillis(); }

    // ── outbox dispatcher ──────────────────────────────────────────────────

    /** One tick of the outbox loop. */
    public static List<ReactorAction> dispatchOutboxOnce(
            TapeSessionService svc, Map<String, Connector> connectors,
            String claimer) throws SQLException {
        return dispatchOutboxOnce(svc, connectors, claimer, 50,
            DEFAULT_LEASE_TTL_MS, DEFAULT_BACKOFF_MS, MAX_BACKOFF_MS);
    }

    public static List<ReactorAction> dispatchOutboxOnce(
            TapeSessionService svc, Map<String, Connector> connectors,
            String claimer, int limit, long leaseTtlMs,
            long defaultBackoffMs, long maxBackoffMs) throws SQLException {
        List<ReactorAction> results = new ArrayList<>();
        long now = nowMs();
        List<EffectRecord> effects = svc.listEffectsToDispatch(now, "", limit);
        for (EffectRecord eff : effects) {
            Connector connector = eff.connector() == null ? null : connectors.get(eff.connector());
            if (connector == null) {
                results.add(ReactorAction.effectSkip("dispatch",
                    eff.idempotencyKey(), "no connector for " + eff.connector()));
                continue;
            }
            TapeSessionService.ClaimEffectResult claim = svc.claimEffectDispatch(
                eff.appName(), eff.userId(), eff.sessionId(), eff.idempotencyKey(),
                claimer, leaseTtlMs, now);
            if (!claim.acquired()) {
                results.add(ReactorAction.effectSkip("dispatch",
                    eff.idempotencyKey(), "lost the claim"));
                continue;
            }
            // Re-read after the claim.
            Optional<EffectRecord> maybeFresh = svc.getEffect(
                eff.appName(), eff.userId(), eff.sessionId(), eff.idempotencyKey());
            if (maybeFresh.isEmpty() || !EffectRecord.PENDING.equals(maybeFresh.get().status())) {
                results.add(ReactorAction.effectSkip("dispatch",
                    eff.idempotencyKey(), "not PENDING after claim"));
                continue;
            }
            EffectRecord fresh = maybeFresh.get();

            DispatchResult outcome;
            try {
                outcome = connector.dispatch(fresh);
            } catch (Exception ex) {
                int attempts = fresh.dispatchAttempts() + 1;
                long backoff = Math.min(
                    defaultBackoffMs * (1L << Math.max(0, attempts - 1)),
                    maxBackoffMs);
                svc.recordDispatchAttempt(
                    fresh.appName(), fresh.userId(), fresh.sessionId(),
                    fresh.idempotencyKey(),
                    ex.getClass().getSimpleName() + ": " + ex.getMessage(),
                    now + backoff);
                results.add(ReactorAction.effectBackoff("dispatch",
                    fresh.idempotencyKey(), "exception", backoff));
                continue;
            }

            switch (outcome.status()) {
                case DispatchResult.CONFIRMED -> {
                    svc.completeEffect(
                        fresh.appName(), fresh.userId(), fresh.sessionId(),
                        fresh.idempotencyKey(), EffectRecord.CONFIRMED,
                        outcome.response() == null ? null : outcome.response().toString(),
                        null);
                    if (outcome.externalRef() != null && !outcome.externalRef().isEmpty()) {
                        // Attach external_ref via a direct UPDATE — the effect
                        // is already terminal, so completeEffect won't pick it up.
                        attachExternalRef(svc, fresh, outcome.externalRef());
                    }
                    results.add(ReactorAction.effect("dispatch",
                        fresh.idempotencyKey(), "confirmed"));
                }
                case DispatchResult.UNKNOWN -> {
                    svc.recordDispatchAttempt(
                        fresh.appName(), fresh.userId(), fresh.sessionId(),
                        fresh.idempotencyKey(),
                        String.valueOf(outcome.error() == null ? "ack lost" : outcome.error()),
                        0L);
                    results.add(ReactorAction.effect("dispatch",
                        fresh.idempotencyKey(), "unknown"));
                }
                case DispatchResult.FAILED -> {
                    if (outcome.retryAfterMs() < 0) {
                        svc.completeEffect(
                            fresh.appName(), fresh.userId(), fresh.sessionId(),
                            fresh.idempotencyKey(), EffectRecord.FAILED,
                            null,
                            outcome.error() == null ? null : outcome.error().toString());
                        results.add(ReactorAction.effect("dispatch",
                            fresh.idempotencyKey(), "failed-terminal"));
                    } else {
                        int attempts = fresh.dispatchAttempts() + 1;
                        long backoff = outcome.retryAfterMs() > 0 ? outcome.retryAfterMs()
                            : Math.min(defaultBackoffMs * (1L << Math.max(0, attempts - 1)),
                                       maxBackoffMs);
                        svc.recordDispatchAttempt(
                            fresh.appName(), fresh.userId(), fresh.sessionId(),
                            fresh.idempotencyKey(),
                            String.valueOf(outcome.error() == null ? "dispatch failed" : outcome.error()),
                            now + backoff);
                        results.add(ReactorAction.effectBackoff("dispatch",
                            fresh.idempotencyKey(), "failed-retry", backoff));
                    }
                }
                default -> results.add(ReactorAction.effectSkip("dispatch",
                    fresh.idempotencyKey(), "unknown connector status " + outcome.status()));
            }
        }
        return results;
    }

    private static void attachExternalRef(
            TapeSessionService svc, EffectRecord fresh, String externalRef) throws SQLException {
        // The simplest portable way: re-issue an external observation with
        // CONFIRMED resolution (which sets external_ref and leaves status
        // CONFIRMED — `recordExternalObservation` is idempotent on a row
        // that's already CONFIRMED in the sense that it just refreshes the
        // tracked external_ref).
        svc.recordExternalObservation(
            fresh.appName(), fresh.userId(), fresh.sessionId(), fresh.idempotencyKey(),
            Schema.EffectResolution.CONFIRMED, externalRef, null, null, "");
    }

    // ── reconciler ─────────────────────────────────────────────────────────

    public static List<ReactorAction> reconcileOnce(
            TapeSessionService svc, Map<String, Connector> connectors) throws SQLException {
        return reconcileOnce(svc, connectors, 0L, 50);
    }

    public static List<ReactorAction> reconcileOnce(
            TapeSessionService svc, Map<String, Connector> connectors,
            long stalePendingMs, int limit) throws SQLException {
        List<ReactorAction> results = new ArrayList<>();
        long cutoff = stalePendingMs > 0 ? nowMs() - stalePendingMs : 0L;
        List<EffectRecord> effects = svc.listPendingEffects(
            cutoff, stalePendingMs > 0, true, limit);
        for (EffectRecord eff : effects) {
            String connName = eff.connector() == null ? "" : eff.connector();
            Connector connector = connectors.get(connName);
            if (connector == null) {
                results.add(ReactorAction.effectSkip("reconcile",
                    eff.idempotencyKey(), "no connector for " + connName));
                continue;
            }
            ObservationResult obs;
            try {
                obs = connector.observe(eff);
            } catch (Exception ex) {
                results.add(ReactorAction.effectSkip("reconcile",
                    eff.idempotencyKey(), "observe raised: " + ex.getMessage()));
                continue;
            }
            svc.recordExternalObservation(
                eff.appName(), eff.userId(), eff.sessionId(), eff.idempotencyKey(),
                obs.status(), obs.externalRef(),
                obs.response() == null ? null : obs.response().toString(),
                obs.error() == null ? null : obs.error().toString(),
                obs.compensateKind());
            results.add(ReactorAction.effect("reconcile",
                eff.idempotencyKey(), obs.status()));
        }
        return results;
    }

    // ── compensation drainer ──────────────────────────────────────────────

    public static List<ReactorAction> drainObligationsOnce(
            TapeSessionService svc, Map<String, Connector> connectors,
            String claimer) throws SQLException {
        return drainObligationsOnce(svc, connectors, claimer, 50,
            DEFAULT_LEASE_TTL_MS, DEFAULT_BACKOFF_MS, MAX_BACKOFF_MS);
    }

    public static List<ReactorAction> drainObligationsOnce(
            TapeSessionService svc, Map<String, Connector> connectors,
            String claimer, int limit, long leaseTtlMs,
            long defaultBackoffMs, long maxBackoffMs) throws SQLException {
        List<ReactorAction> results = new ArrayList<>();
        long now = nowMs();
        List<ObligationRecord> obligations = svc.listUnresolvedObligations(
            now, limit, true, false, true);
        for (ObligationRecord ob : obligations) {
            EffectRecord eff = null;
            if (ob.effectKey() != null && !ob.effectKey().isEmpty()) {
                eff = svc.getEffect(
                    ob.appName(), ob.userId(), ob.sessionId(), ob.effectKey())
                    .orElse(null);
            }
            String connName = (eff != null && eff.connector() != null && !eff.connector().isEmpty())
                ? eff.connector() : ob.kind();
            Connector connector = connectors.get(connName);
            if (connector == null) {
                results.add(ReactorAction.obligationSkip(ob.seq(),
                    "no connector for " + connName));
                continue;
            }
            TapeSessionService.ClaimObligationResult claim = svc.claimObligation(
                ob.seq(), claimer, leaseTtlMs, now);
            if (!claim.acquired()) {
                results.add(ReactorAction.obligationSkip(ob.seq(), "lost the claim"));
                continue;
            }
            CompensationResult outcome;
            try {
                outcome = connector.compensate(ob);
            } catch (Exception ex) {
                int attempts = ob.attempts() + 1;
                long backoff = Math.min(
                    defaultBackoffMs * (1L << Math.max(0, attempts - 1)),
                    maxBackoffMs);
                svc.recordObligationAttempt(ob.seq(),
                    ex.getClass().getSimpleName() + ": " + ex.getMessage(),
                    now + backoff);
                results.add(ReactorAction.obligationBackoff(ob.seq(), "exception", backoff));
                continue;
            }
            switch (outcome.status()) {
                case CompensationResult.COMPENSATED -> {
                    svc.resolveObligation(ob.seq(),
                        ObligationRecord.COMPENSATED,
                        outcome.response() == null ? null : outcome.response().toString());
                    results.add(ReactorAction.obligation("drain", ob.seq(), "compensated"));
                }
                case CompensationResult.FAILED -> {
                    long backoff = outcome.retryAfterMs() > 0 ? outcome.retryAfterMs()
                        : Math.min(defaultBackoffMs * (1L << Math.max(0, ob.attempts())),
                                   maxBackoffMs);
                    svc.recordObligationAttempt(ob.seq(),
                        String.valueOf(outcome.error() == null ? "compensate failed" : outcome.error()),
                        now + backoff);
                    results.add(ReactorAction.obligationBackoff(ob.seq(), "failed-retry", backoff));
                }
                default -> results.add(ReactorAction.obligationSkip(ob.seq(),
                    "unknown compensate status " + outcome.status()));
            }
        }
        return results;
    }

    // ── timer firer ───────────────────────────────────────────────────────

    /** Claim all due timers and (optionally) hand each to a dispatcher
     *  callback. With dispatcher=null, the timers are just marked fired —
     *  useful when a downstream watcher does its own polling on
     *  {@code tape_timers.fired}. */
    public static List<ReactorAction> fireDueTimersOnce(
            TapeSessionService svc, Consumer<TimerRecord> dispatcher) throws SQLException {
        return fireDueTimersOnce(svc, dispatcher, 100);
    }

    public static List<ReactorAction> fireDueTimersOnce(
            TapeSessionService svc, Consumer<TimerRecord> dispatcher, int limit) throws SQLException {
        List<TimerRecord> timers = svc.listDueTimers(nowMs(), limit, true);
        List<ReactorAction> out = new ArrayList<>();
        for (TimerRecord t : timers) {
            if (dispatcher == null) {
                out.add(ReactorAction.timer(t.timerId(), "marked-fired"));
                continue;
            }
            try {
                dispatcher.accept(t);
                out.add(ReactorAction.timer(t.timerId(), "fired"));
            } catch (Exception ex) {
                out.add(ReactorAction.timer(t.timerId(),
                    "dispatcher raised: " + ex.getMessage()));
            }
        }
        return out;
    }
}
