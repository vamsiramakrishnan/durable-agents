package dev.tape.embedded.chaos;

import dev.tape.embedded.TapeSessionService;

/**
 * Predicate over the embedded journal — Java port of
 * {@code tape_adk.chaos.Invariant}.
 *
 * <p>An invariant reads the embedded SQL tables directly (no gRPC client)
 * and returns an {@link InvariantResult}. The {@link Invariants} class
 * provides built-in invariants: {@code noStuckObligations},
 * {@code noBlindNonIdempotentRetry}, {@code exactlyOne(connector=...)}.
 */
@FunctionalInterface
public interface Invariant {

    /** Stable name used for the report row. */
    default String name() { return "<unnamed>"; }

    /** Run the predicate against the live store. */
    InvariantResult check(TapeSessionService svc) throws Exception;
}
