package dev.tape.chaos;

import java.util.ArrayList;
import java.util.List;

/**
 * The outcome of one scenario run. Mutable: {@link ChaosSession} appends
 * to it as the scenario plays out.
 */
public final class ChaosReport {
    public final String scenarioName;
    public final long seed;
    public final String failpointsSpec;
    public boolean passed = true;
    public final List<InvariantResult> invariantResults = new ArrayList<>();
    public final List<String> notes = new ArrayList<>();

    public ChaosReport(String scenarioName, long seed, String failpointsSpec) {
        this.scenarioName = scenarioName;
        this.seed = seed;
        this.failpointsSpec = failpointsSpec;
    }

    @Override public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("ChaosReport(\"").append(scenarioName)
         .append("\": ").append(passed ? "PASS" : "FAIL")
         .append(", seed=").append(seed).append(")");
        for (InvariantResult ir : invariantResults) b.append("\n  - ").append(ir);
        for (String n : notes) b.append("\n  ! ").append(n);
        return b.toString();
    }
}
