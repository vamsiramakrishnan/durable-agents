package dev.tape.sinks;

import dev.tape.proto.EventEntry;

/**
 * A {@code Sink} is what the WAL fan-out adapters call for every journal
 * entry. Implementations must be safe to call concurrently from a single
 * tail stream; the relay invokes them in order.
 *
 * <p>Pair an at-least-once relay with a sink that dedupes on
 * {@code (run_id, seq)} (or one whose backend dedupes per-message — Pub/Sub
 * does, within its dedup window) for exactly-once-effective delivery.
 */
public interface Sink {

    /** Publish one journal entry. Must throw on failure (so the relay can
     *  pause its cursor). */
    void publish(EventEntry entry) throws Exception;

    /** Release any held resources. May be a no-op. */
    default void close() throws Exception {}
}
