package dev.tape.chaos;

import dev.tape.connectors.Connector;

import java.util.List;
import java.util.Random;

/**
 * Wraps an inner {@link Connector} with declarative {@link Fault} rules.
 * Mirrors {@code tape.chaos.ChaosConnector} (Python/TS/Go). Honoured
 * fault kinds: {@code lose_ack}, {@code duplicate}, {@code delay}.
 */
public final class ChaosConnector implements Connector {

    private final Connector inner;
    private final List<Fault> faults;
    private final Random rng;

    public ChaosConnector(Connector inner, List<Fault> faults, Random rng) {
        this.inner = inner;
        this.faults = List.copyOf(faults);
        this.rng = (rng != null) ? rng : new Random();
    }

    @Override public String name() { return inner.name(); }

    private Fault fire(String kind) {
        for (Fault f : faults) {
            if (!kind.equals(f.action())) continue;
            if (f.probability() >= 1.0 || rng.nextDouble() < f.probability()) return f;
        }
        return null;
    }

    @Override
    public Result dispatch(Effect effect) throws Exception {
        Fault d = fire("delay");
        if (d != null && d.ms() > 0) {
            double ms = d.ms();
            if (d.jitter() > 0) {
                ms *= (1.0 + (rng.nextDouble() * 2 - 1) * d.jitter());
                if (ms < 0) ms = 0;
            }
            Thread.sleep((long) ms);
        }
        Result result = inner.dispatch(effect);
        if (result.outcome == DispatchOutcome.CONFIRMED && fire("lose_ack") != null) {
            Result mutated = new Result(DispatchOutcome.UNKNOWN);
            mutated.response = result.response;
            mutated.error = "tape.chaos: simulated lost ack";
            mutated.dispatchId = result.dispatchId;
            return mutated;
        }
        return result;
    }

    @Override
    public Observation observe(Effect effect) throws Exception {
        Observation result = inner.observe(effect);
        if (result.outcome == ObservationOutcome.CONFIRMED && fire("duplicate") != null) {
            Observation mutated = new Observation(ObservationOutcome.DUPLICATE);
            mutated.response = result.response;
            mutated.count = result.count + 1;
            return mutated;
        }
        return result;
    }

    @Override
    public Compensation compensate(Obligation obligation) throws Exception {
        // Compensation faults aren't modelled in Phase 1 (matches Python/TS/Go).
        return inner.compensate(obligation);
    }
}
