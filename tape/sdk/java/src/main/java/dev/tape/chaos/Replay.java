package dev.tape.chaos;

import dev.tape.TapeClient;

import java.util.ArrayList;
import java.util.List;

/**
 * Bit-for-bit determinism check. Mirrors {@code tape.chaos.replay}.
 *
 * <p>Run a scenario body twice with the same seed against the same server;
 * capture the journal both times; assert they're identical after
 * canonicalisation. Never throws on divergence; returns a report.
 */
public final class Replay {
    private Replay() {}

    public record Report(
            String scenarioName, long seed, boolean bitIdentical,
            Snapshot snapA, Snapshot snapB,
            List<String> diffSummary, List<String> notes
    ) {
        @Override public String toString() {
            StringBuilder b = new StringBuilder();
            b.append("ReplayReport(\"").append(scenarioName).append("\": ")
             .append(bitIdentical ? "DETERMINISTIC" : "DRIFTED")
             .append(", seed=").append(seed).append(")");
            if (!bitIdentical) {
                int a = snapA == null ? 0 : snapA.lines.size();
                int bb = snapB == null ? 0 : snapB.lines.size();
                b.append("\n  journal lengths: ").append(a).append(" vs ").append(bb);
                for (int i = 0; i < diffSummary.size() && i < 10; i++) {
                    b.append("\n  - ").append(diffSummary.get(i));
                }
                if (diffSummary.size() > 10)
                    b.append("\n  ... and ").append(diffSummary.size() - 10).append(" more");
            }
            for (String n : notes) b.append("\n  ! ").append(n);
            return b.toString();
        }
    }

    /** Body of the scenario — must produce a run, either by returning its
     *  run_id or by calling {@code sess.setRunId(rid)}. */
    @FunctionalInterface
    public interface Body {
        String run(TapeClient client, ChaosSession session) throws Exception;
    }

    public static Report replay(Scenario scen, Body body, String url) {
        if (url == null || url.isEmpty()) url = "tape://localhost:7878";
        List<Snapshot> snapshots = new ArrayList<>(2);
        List<String> notes = new ArrayList<>();

        for (int pass = 1; pass <= 2; pass++) {
            ChaosSession.Opts opts = new ChaosSession.Opts();
            opts.url = url;
            try (ChaosSession sess = new ChaosSession(scen, opts)) {
                sess.enter();
                String runId = null;
                try (TapeClient client = new TapeClient(url)) {
                    try {
                        runId = body.run(client, sess);
                    } catch (Exception ex) {
                        sess.recordThrown(ex);
                        notes.add("pass " + pass + " raised: " + ex.getMessage());
                    }
                    if (runId == null || runId.isEmpty()) {
                        // late-bound by setRunId? we can't read it back here;
                        // we accept the empty result.
                    }
                    if (runId == null || runId.isEmpty()) {
                        notes.add("pass " + pass + ": body did not produce a runId (return it or call setRunId)");
                        return new Report(scen.name(), scen.seed(), false, null, null,
                                          List.of(), notes);
                    }
                    snapshots.add(Snapshot.capture(client, runId, "run-1"));
                }
            }
        }

        Snapshot a = snapshots.get(0), b = snapshots.get(1);
        boolean identical = a.equalsSnapshot(b);
        List<String> diff = new ArrayList<>();
        if (!identical) {
            for (Snapshot.DiffEntry d : a.diff(b)) {
                switch (d.op()) {
                    case "!=" -> diff.add("[" + d.index() + "] " + d.a().kind() + " differs:\n    A: "
                            + d.a().payload() + "\n    B: " + d.b().payload());
                    case ">"  -> diff.add("[" + d.index() + "] only in A: " + d.a().kind());
                    case "<"  -> diff.add("[" + d.index() + "] only in B: " + d.b().kind());
                }
            }
        }
        return new Report(scen.name(), scen.seed(), identical, a, b, diff, notes);
    }
}
