package dev.tape.embedded.chaos;

import dev.tape.embedded.Connector;
import dev.tape.embedded.TapeSessionService;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Chaos / fault-injection for the embedded ({@code tape-adk-java}) tier —
 * Java port of {@code tape_adk.chaos}.
 *
 * <p>Where the gRPC chaos package targets the Rust tape-server's failpoints
 * + connector registry, this one targets the in-process
 * {@code Map<String, Connector>} the embedded reactor loop dispatches
 * through.
 *
 * <p>Static factories: {@link #loseAck}, {@link #duplicate},
 * {@link #delayConnector}, {@link #scenario}, {@link #session},
 * {@link #run}.
 *
 * <p>Mechanism — see {@link #session}: validate every connector-targeted
 * fault has a connector to attach to (under {@code strictFaults=true}, the
 * default, a missing target FAILS the scenario via a synthetic
 * {@code strict_faults} invariant result — no silent-skip false positives),
 * wrap each targeted connector with {@link ChaosConnector}, yield the
 * wrapped map + a report shell through {@link ChaosSession}, run every
 * invariant on close, finalise the report.
 */
public final class Chaos {

    private Chaos() {}

    // ── fault factories ────────────────────────────────────────────────────

    /**
     * Dispatch returns CONFIRMED → flipped to UNKNOWN. Pass {@code connector=}
     * or {@code tool=}, not both. Default probability 0.3.
     */
    public static Fault loseAck(String connector, String tool, double probability) {
        validateConnectorOrTool("loseAck", connector, tool);
        return new Fault(Fault.LAYER_CONNECTOR,
            connector == null ? "" : connector,
            tool == null ? "" : tool,
            "lose_ack", probability, 0L, 0.0);
    }

    public static Fault loseAck(String connector, String tool) {
        return loseAck(connector, tool, 0.3);
    }

    /** {@code observe()} returns DUPLICATE — the reconciler should register
     *  a compensation. Default probability 0.05. */
    public static Fault duplicate(String connector, String tool, double probability) {
        validateConnectorOrTool("duplicate", connector, tool);
        return new Fault(Fault.LAYER_CONNECTOR,
            connector == null ? "" : connector,
            tool == null ? "" : tool,
            "duplicate", probability, 0L, 0.0);
    }

    public static Fault duplicate(String connector, String tool) {
        return duplicate(connector, tool, 0.05);
    }

    /** Sleep {@code ms} (± {@code jitter} as a fraction) before dispatch. */
    public static Fault delayConnector(String connector, long ms, double jitter) {
        if (connector == null || connector.isEmpty()) {
            throw new IllegalArgumentException("delayConnector requires connector=");
        }
        return new Fault(Fault.LAYER_CONNECTOR, connector, "",
            "delay", 1.0, ms, jitter);
    }

    public static Fault delayConnector(String connector, long ms) {
        return delayConnector(connector, ms, 0.0);
    }

    private static void validateConnectorOrTool(String fn, String connector, String tool) {
        boolean hasC = connector != null && !connector.isEmpty();
        boolean hasT = tool != null && !tool.isEmpty();
        if (hasC && hasT) {
            throw new IllegalArgumentException(
                fn + ": pass connector= or tool=, not both");
        }
        if (!hasC && !hasT) {
            throw new IllegalArgumentException(
                fn + " requires connector= or tool=");
        }
    }

    // ── scenario factory ───────────────────────────────────────────────────

    public static Scenario scenario(String name, List<Fault> faults,
                                     List<Invariant> invariants,
                                     long seed, boolean strictFaults) {
        return new Scenario(name, faults, invariants, seed, strictFaults);
    }

    public static Scenario scenario(String name, List<Fault> faults,
                                     List<Invariant> invariants) {
        return new Scenario(name, faults, invariants, 0L, true);
    }

    // ── session / run ──────────────────────────────────────────────────────

    /**
     * Open a chaos session: wrap connectors with the scenario's faults
     * and bind the {@link DataSource} for invariant queries. Use with
     * try-with-resources — closing the session runs the invariants and
     * finalises the report.
     *
     * <pre>
     * try (var sess = Chaos.session(scen, svc, ds, connectors)) {
     *     // body — drive the system through the wrapped connectors
     *     Reactors.dispatchOutboxOnce(svc, sess.connectors(), "d-1");
     * }
     * // sess.report() now has the invariant results.
     * </pre>
     */
    public static ChaosSession session(
            Scenario scen, TapeSessionService svc, DataSource ds,
            Map<String, Connector> connectors) {
        Random rng = new Random(scen.seed());
        ChaosReport report = new ChaosReport(scen.name(), scen.seed());
        Map<String, Connector> wrapped = new LinkedHashMap<>(connectors);

        Map<String, List<Fault>> byConnector = new HashMap<>();
        List<Fault> toolScoped = new ArrayList<>();

        for (Fault f : scen.faults()) {
            if (!Fault.LAYER_CONNECTOR.equals(f.layer())) {
                noteOrFail(report, scen,
                    "fault layer '" + f.layer() + "' not supported in embedded "
                    + "tier (server failpoints require the gRPC tier)");
                continue;
            }
            if (!f.target().isEmpty()) {
                byConnector.computeIfAbsent(f.target(), k -> new ArrayList<>()).add(f);
            } else if (!f.tool().isEmpty()) {
                toolScoped.add(f);
            } else {
                noteOrFail(report, scen,
                    "connector fault skipped: neither target nor tool set");
            }
        }

        for (Map.Entry<String, List<Fault>> e : byConnector.entrySet()) {
            String connName = e.getKey();
            if (!connectors.containsKey(connName)) {
                noteOrFail(report, scen,
                    "connector fault for '" + connName + "' skipped: "
                    + "connector not in `connectors` dict");
                continue;
            }
            List<Fault> combined = new ArrayList<>(e.getValue());
            combined.addAll(toolScoped);
            wrapped.put(connName, new ChaosConnector(
                connectors.get(connName), combined, rng));
        }
        if (!toolScoped.isEmpty()) {
            if (connectors.isEmpty()) {
                noteOrFail(report, scen,
                    "tool-scoped fault(s) skipped: empty `connectors` dict");
            }
            for (Map.Entry<String, Connector> e : connectors.entrySet()) {
                if (byConnector.containsKey(e.getKey())) continue;
                wrapped.put(e.getKey(), new ChaosConnector(
                    e.getValue(), toolScoped, rng));
            }
        }

        return new ChaosSession(scen, svc, ds, wrapped, report);
    }

    /**
     * One-shot convenience: open a session, call {@code body}, return the
     * report. Mirrors Python's {@code chaos.run(...)}.
     */
    public static ChaosReport run(
            Scenario scen, TapeSessionService svc, DataSource ds,
            Map<String, Connector> connectors,
            ChaosBody body) throws Exception {
        try (ChaosSession sess = session(scen, svc, ds, connectors)) {
            if (body != null) body.accept(sess.connectors());
            return sess.report();
        }
    }

    /** Body that runs inside a chaos session — receives the wrapped map. */
    @FunctionalInterface
    public interface ChaosBody {
        void accept(Map<String, Connector> connectors) throws Exception;
    }

    static void noteOrFail(ChaosReport report, Scenario scen, String message) {
        report.addNote(message);
        if (scen.strictFaults()) {
            report.addResult(new InvariantResult(
                "strict_faults", false, message));
        }
    }
}
