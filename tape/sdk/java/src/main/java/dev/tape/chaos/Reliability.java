package dev.tape.chaos;

import java.util.ArrayList;
import java.util.List;

/**
 * Reliability Surface — R(k, ε, λ). Mirrors {@code tape.chaos.reliability}.
 *
 * <ul>
 *   <li>k: scenarios driven</li>
 *   <li>ε: invariant-violation rate (0.0 best, 1.0 worst)</li>
 *   <li>λ: recovery rate (1.0 best, 0.0 worst)</li>
 * </ul>
 */
public final class Reliability {
    private Reliability() {}

    public record Surface(int k, double epsilon, double lambda) {
        @Override public String toString() {
            return String.format("R(k=%d, ε=%.2f, λ=%.2f)", k, epsilon, lambda);
        }
    }

    private record Row(String name, boolean passed, List<String> failed, boolean terminal, List<String> notes) {}

    /** Accumulator for chaos campaign results. */
    public static final class Recorder {
        private final List<Row> rows = new ArrayList<>();

        public void add(ChaosReport report, boolean terminal) {
            List<String> failed = new ArrayList<>();
            for (InvariantResult ir : report.invariantResults) {
                if (!ir.passed()) failed.add(ir.name());
            }
            rows.add(new Row(report.scenarioName, report.passed, failed, terminal,
                              new ArrayList<>(report.notes)));
        }

        public Surface surface() {
            int k = rows.size();
            if (k == 0) return new Surface(0, 0.0, 1.0);
            int violations = 0, terminal = 0;
            for (Row r : rows) {
                if (!r.passed) violations++;
                if (r.terminal) terminal++;
            }
            return new Surface(k, (double) violations / k, (double) terminal / k);
        }

        /** Stable-shape Markdown table — paste-into-PR friendly. */
        public String toMarkdown(String title) {
            if (title == null || title.isEmpty()) title = "TapeChaos campaign";
            Surface s = surface();
            StringBuilder b = new StringBuilder();
            b.append("# ").append(title).append("\n\n");
            b.append(String.format("**Reliability Surface**: `R(k=%d, ε=%.2f, λ=%.2f)`%n%n",
                    s.k, s.epsilon, s.lambda));
            b.append("- ").append(s.k).append(" scenarios\n");
            b.append("- ").append((int) Math.round(s.epsilon * s.k)).append(" invariant violations\n");
            b.append("- ").append((int) Math.round(s.lambda * s.k)).append(" runs reached terminal\n\n");
            b.append("| Scenario | Passed | Terminal | Failed invariants |\n");
            b.append("|---|---|---|---|\n");
            for (Row r : rows) {
                String failed = r.failed.isEmpty() ? "—" : String.join(", ", r.failed);
                b.append("| `").append(r.name).append("` | ")
                 .append(r.passed ? "OK" : "FAIL").append(" | ")
                 .append(r.terminal ? "yes" : "no").append(" | ")
                 .append(failed).append(" |\n");
            }
            return b.toString();
        }
    }

    /** Quick score for a list of reports. */
    public static Surface score(List<ChaosReport> reports) {
        Recorder rec = new Recorder();
        for (ChaosReport r : reports) rec.add(r, true);
        return rec.surface();
    }
}
