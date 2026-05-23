package dev.tape.embedded.chaos;

import java.util.List;

/**
 * A named bundle of faults + invariants + seed — Java port of
 * {@code tape_adk.chaos.Scenario}.
 *
 * <p>{@code strictFaults=true} (default): a connector-targeted fault whose
 * target isn't in the {@code connectors} dict FAILS the scenario via a
 * synthetic {@code strict_faults} invariant result — same mechanism as
 * the gRPC SDK. The silent-skip false positive is the bug both versions
 * share until this guard fires.
 */
public record Scenario(
        String name,
        List<Fault> faults,
        List<Invariant> invariants,
        long seed,
        boolean strictFaults) {

    public Scenario {
        if (name == null) name = "";
        faults = faults == null ? List.of() : List.copyOf(faults);
        invariants = invariants == null ? List.of() : List.copyOf(invariants);
    }

    /** Default {@code seed=0, strictFaults=true}. */
    public static Scenario of(String name, List<Fault> faults,
                              List<Invariant> invariants) {
        return new Scenario(name, faults, invariants, 0L, true);
    }
}
