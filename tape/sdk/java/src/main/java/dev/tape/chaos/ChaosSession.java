package dev.tape.chaos;

import dev.tape.TapeClient;
import dev.tape.connectors.Connector;
import dev.tape.connectors.ConnectorRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Applies connector wraps on {@link #enter} and restores them + runs
 * invariants on {@link #exit}. Use try-with-resources via
 * {@link AutoCloseable} for the common shape:
 *
 * <pre>{@code
 * try (ChaosSession sess = new ChaosSession(scen, opts)) {
 *     sess.enter();
 *     runMyAgent(sess);
 * }
 * System.out.println(sess.report());
 * }</pre>
 *
 * <p>Mirrors {@code tape.chaos.Session} (Python/TS/Go).
 */
public final class ChaosSession implements AutoCloseable {

    public static final class Opts {
        public String url = "tape://localhost:7878";
        public String runId = "";
        public ConnectorRegistry registry = ConnectorRegistry.DEFAULT;
    }

    private final Scenario scenario;
    private final String url;
    private String runId;
    private final String failpointsSpec;
    private final ChaosReport report;
    private final Random rng;
    private final ConnectorRegistry registry;
    private final List<Runnable> restores = new ArrayList<>();
    private boolean entered = false;
    private Throwable thrown;

    public ChaosSession(Scenario scen, Opts opts) {
        this.scenario = scen;
        this.url = (opts.url == null || opts.url.isEmpty()) ? "tape://localhost:7878" : opts.url;
        this.runId = opts.runId == null ? "" : opts.runId;
        this.registry = (opts.registry != null) ? opts.registry : ConnectorRegistry.DEFAULT;
        this.failpointsSpec = Faults.failpointsEnv(scen);
        this.report = new ChaosReport(scen.name(), scen.seed(), failpointsSpec);
        this.rng = (scen.seed() != 0L) ? new Random(scen.seed()) : new Random();
    }

    /** The accumulating report. Mutated as the session plays out. */
    public ChaosReport report() { return report; }

    /** The {@code FAILPOINTS} env-var spec the chaos-feature server would consume. */
    public String failpointsSpec() { return failpointsSpec; }

    /** Late-bind the run identifier (for per-run invariants). */
    public ChaosSession setRunId(String id) { this.runId = id == null ? "" : id; return this; }

    /** Record an error thrown by the scenario body — surfaced in the report. */
    public ChaosSession recordThrown(Throwable t) { this.thrown = t; return this; }

    /** Apply connector wraps. Idempotent on double-call. */
    public void enter() {
        if (entered) return;
        entered = true;
        Map<String, List<Fault>> byTarget = new HashMap<>();
        for (Fault f : scenario.faults()) {
            if (f.layer() != Fault.Layer.CONNECTOR) continue;
            byTarget.computeIfAbsent(f.target(), k -> new ArrayList<>()).add(f);
        }
        for (Map.Entry<String, List<Fault>> e : byTarget.entrySet()) {
            String name = e.getKey();
            Connector real;
            try {
                real = registry.get(name);
            } catch (RuntimeException ex) {
                report.notes.add("connector fault for \"" + name + "\" skipped: not registered");
                continue;
            }
            Connector wrapped = new ChaosConnector(real, e.getValue(), rng);
            registry.replace(name, wrapped);
            restores.add(() -> registry.replace(name, real));
        }
    }

    /** Run invariants + restore connectors. Safe to call multiple times. */
    public void exit() {
        // Restore connectors first (in reverse order).
        for (int i = restores.size() - 1; i >= 0; i--) {
            try { restores.get(i).run(); } catch (RuntimeException ignored) { /* swallow */ }
        }
        restores.clear();

        if (thrown != null) {
            report.passed = false;
            report.notes.add("body raised: " + thrown.getClass().getSimpleName() + ": " + thrown.getMessage());
        }

        try (TapeClient client = new TapeClient(url)) {
            for (Invariant inv : scenario.invariants()) {
                InvariantResult ir;
                try {
                    ir = inv.check(client, runId);
                } catch (Exception ex) {
                    ir = InvariantResult.fail(inv.name(),
                            "check threw: " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
                }
                report.invariantResults.add(ir);
                if (!ir.passed()) report.passed = false;
            }
        } catch (Exception ex) {
            report.notes.add("invariant client: " + ex.getMessage());
        }
    }

    @Override public void close() { exit(); }
}
