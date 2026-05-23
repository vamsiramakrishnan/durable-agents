package dev.tape.embedded.chaos;

import java.util.ArrayList;
import java.util.List;

/**
 * Outcome of one chaos scenario — Java port of
 * {@code tape_adk.chaos.ChaosReport}.
 *
 * <p>Mutable on purpose: invariants append rows as they run during
 * session-close.
 */
public final class ChaosReport {

    private final String scenarioName;
    private final long seed;
    private boolean passed = true;
    private final List<InvariantResult> invariantResults = new ArrayList<>();
    private final List<String> notes = new ArrayList<>();

    public ChaosReport(String scenarioName, long seed) {
        this.scenarioName = scenarioName == null ? "" : scenarioName;
        this.seed = seed;
    }

    public String scenarioName() { return scenarioName; }
    public long seed() { return seed; }
    public boolean passed() { return passed; }
    public List<InvariantResult> invariantResults() { return List.copyOf(invariantResults); }
    public List<String> notes() { return List.copyOf(notes); }

    void addResult(InvariantResult r) {
        invariantResults.add(r);
        if (!r.passed()) passed = false;
    }

    void addNote(String n) { notes.add(n); }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ChaosReport('").append(scenarioName)
          .append("': ").append(passed ? "pass" : "FAIL")
          .append(", seed=").append(seed).append(")");
        for (InvariantResult ir : invariantResults) {
            sb.append("\n  - ").append(ir);
        }
        for (String n : notes) {
            sb.append("\n  ! ").append(n);
        }
        return sb.toString();
    }
}
