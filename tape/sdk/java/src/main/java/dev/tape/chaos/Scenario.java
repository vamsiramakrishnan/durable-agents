package dev.tape.chaos;

import java.util.List;

/**
 * A named bundle of (faults, invariants, seed). Mirrors
 * {@code tape.chaos.Scenario} (Python/TS/Go).
 *
 * <p>Build with {@link #of} or the {@link Builder}.
 */
public record Scenario(
        String name,
        List<Fault> faults,
        List<Invariant> invariants,
        long seed
) {
    public Scenario {
        if (name == null || name.isEmpty()) throw new IllegalArgumentException("name required");
        faults = (faults == null) ? List.of() : List.copyOf(faults);
        invariants = (invariants == null) ? List.of() : List.copyOf(invariants);
    }

    /** Build with the default seed (0) and no invariants. */
    public static Scenario of(String name, List<Fault> faults) {
        return new Scenario(name, faults, List.of(), 0L);
    }

    public static Builder builder(String name) { return new Builder(name); }

    public static final class Builder {
        private final String name;
        private List<Fault> faults = List.of();
        private List<Invariant> invariants = List.of();
        private long seed = 0L;
        private Builder(String name) { this.name = name; }
        public Builder faults(List<Fault> f) { this.faults = f; return this; }
        public Builder invariants(List<Invariant> i) { this.invariants = i; return this; }
        public Builder seed(long s) { this.seed = s; return this; }
        public Scenario build() { return new Scenario(name, faults, invariants, seed); }
    }
}
