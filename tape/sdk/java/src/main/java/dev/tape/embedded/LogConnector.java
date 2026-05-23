package dev.tape.embedded;

import java.util.ArrayList;
import java.util.List;

/**
 * No-op connector that records every call. Useful for tests and demos
 * where you don't want to wire up a real upstream.
 *
 * <p>Mirrors {@code tape_adk.connectors.LogConnector}.
 *
 * <p>Distinct from {@code dev.tape.connectors.LogConnector} (the gRPC-path
 * sink that writes JSONL) — this one is in-memory and exposes its call
 * log for assertions.
 */
public final class LogConnector implements Connector {

    private final String name;
    public final List<Schema.EffectRecord> dispatches = new ArrayList<>();
    public final List<Schema.EffectRecord> observations = new ArrayList<>();
    public final List<Schema.ObligationRecord> compensations = new ArrayList<>();

    public LogConnector() { this("log"); }
    public LogConnector(String name) { this.name = name; }

    @Override public String name() { return name; }

    @Override public DispatchResult dispatch(Schema.EffectRecord effect) {
        dispatches.add(effect);
        String key = effect.idempotencyKey();
        String shortKey = key.length() > 8 ? key.substring(0, 8) : key;
        return DispatchResult.confirmed("log-" + shortKey, null);
    }

    @Override public ObservationResult observe(Schema.EffectRecord effect) {
        observations.add(effect);
        return ObservationResult.absent();
    }

    @Override public CompensationResult compensate(Schema.ObligationRecord obligation) {
        compensations.add(obligation);
        return CompensationResult.compensated(null);
    }
}
