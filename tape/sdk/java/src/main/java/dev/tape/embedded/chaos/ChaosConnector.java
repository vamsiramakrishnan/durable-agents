package dev.tape.embedded.chaos;

import dev.tape.embedded.CompensationResult;
import dev.tape.embedded.Connector;
import dev.tape.embedded.DispatchResult;
import dev.tape.embedded.ObservationResult;
import dev.tape.embedded.Schema.EffectRecord;
import dev.tape.embedded.Schema.ObligationRecord;

import java.util.List;
import java.util.Random;

/**
 * A {@link Connector} that decorates {@code inner} with a list of
 * {@link Fault}s — Java port of {@code tape_adk.chaos.ChaosConnector}.
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>{@code lose_ack}  — dispatch's CONFIRMED becomes UNKNOWN. The
 *       inner call already landed; the wrapper hides the ack.</li>
 *   <li>{@code duplicate} — observe()'s CONFIRMED becomes DUPLICATE.</li>
 *   <li>{@code delay}     — dispatch sleeps {@code ms} (± {@code jitter})
 *       before the inner call.</li>
 * </ul>
 *
 * <p>A seeded {@link Random} is the only mutable thread of nondeterminism;
 * a seeded scenario is reproducible.
 *
 * <p>{@code compensate()} is the cleanup path; we don't decorate it
 * (same choice as the Python reference).
 */
public final class ChaosConnector implements Connector {

    private final Connector inner;
    private final List<Fault> faults;
    private final Random rng;

    public ChaosConnector(Connector inner, List<Fault> faults, Random rng) {
        this.inner = inner;
        this.faults = faults == null ? List.of() : List.copyOf(faults);
        this.rng = rng == null ? new Random() : rng;
    }

    @Override public String name() { return inner.name(); }

    /** Find a matching fault by action + tool scope + probability. */
    private Fault matching(String kind, EffectRecord effect) {
        for (Fault f : faults) {
            if (!kind.equals(f.action())) continue;
            if (f.tool() != null && !f.tool().isEmpty() && effect != null) {
                String tn = effect.toolName() == null ? "" : effect.toolName();
                if (!tn.equals(f.tool())) continue;
            }
            if (f.probability() >= 1.0 || rng.nextDouble() < f.probability()) {
                return f;
            }
        }
        return null;
    }

    @Override
    public DispatchResult dispatch(EffectRecord effect) throws Exception {
        Fault d = matching("delay", effect);
        if (d != null && d.ms() > 0) {
            double jitterFactor = 1.0;
            if (d.jitter() > 0) {
                double offset = (rng.nextDouble() * 2.0 - 1.0) * d.jitter();
                jitterFactor = 1.0 + offset;
            }
            long sleepMs = (long) Math.max(0.0, d.ms() * jitterFactor);
            if (sleepMs > 0) Thread.sleep(sleepMs);
        }

        DispatchResult result = inner.dispatch(effect);

        if (result != null
                && DispatchResult.CONFIRMED.equals(result.status())
                && matching("lose_ack", effect) != null) {
            return new DispatchResult(
                DispatchResult.UNKNOWN,
                result.externalRef(),
                result.response(),
                java.util.Map.of("reason", "tape_adk.chaos: simulated lost ack"),
                0L);
        }
        return result;
    }

    @Override
    public ObservationResult observe(EffectRecord effect) throws Exception {
        ObservationResult result = inner.observe(effect);
        if (result != null
                && ObservationResult.CONFIRMED.equals(result.status())
                && matching("duplicate", effect) != null) {
            String ck = result.compensateKind() == null ? "" : result.compensateKind();
            return new ObservationResult(
                ObservationResult.DUPLICATE,
                result.externalRef(),
                result.response(),
                null,
                ck);
        }
        return result;
    }

    @Override
    public CompensationResult compensate(ObligationRecord obligation) throws Exception {
        return inner.compensate(obligation);
    }
}
