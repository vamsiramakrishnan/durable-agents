package dev.tape.chaos;

import dev.tape.TapeClient;

/**
 * Predicate over Tape's journal projections. The journal is the oracle.
 * See {@link Invariants} for the built-in catalogue.
 */
public interface Invariant {
    String name();
    InvariantResult check(TapeClient client, String runId);
}
