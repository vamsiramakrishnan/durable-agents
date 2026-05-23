package dev.tape.embedded.chaos;

import dev.tape.embedded.Connector;
import dev.tape.embedded.TapeSessionService;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.Map;

/**
 * The handle yielded by {@link Chaos#session}. Implements
 * {@link AutoCloseable} so callers can use try-with-resources — Java idiom
 * for the {@code async with} pattern in the Python reference. Closing the
 * session runs every declared invariant against the live store and
 * finalises the report.
 */
public final class ChaosSession implements AutoCloseable {

    private final Scenario scen;
    private final TapeSessionService svc;
    private final DataSource ds;
    private final Map<String, Connector> connectors;
    private final ChaosReport report;
    private boolean closed = false;

    ChaosSession(Scenario scen, TapeSessionService svc, DataSource ds,
                  Map<String, Connector> connectors, ChaosReport report) {
        this.scen = scen;
        this.svc = svc;
        this.ds = ds;
        this.connectors = Collections.unmodifiableMap(connectors);
        this.report = report;
    }

    /** The (possibly wrapped) connectors. Pass these to the reactor loop. */
    public Map<String, Connector> connectors() { return connectors; }

    /** The accumulating report. Finalised by {@link #close()}. */
    public ChaosReport report() { return report; }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        // Ensure the four embedded tables exist before any invariant tries
        // to query them. Reading from a brand-new service before any
        // mutating call would otherwise fail with "no such table".
        try {
            svc.prepareTables();
        } catch (Exception ex) {
            report.addResult(new InvariantResult(
                "prepare_tables", false,
                "raised " + ex.getClass().getSimpleName() + ": " + ex.getMessage()));
        }

        DataSource prior = Invariants.ACTIVE_DS.get();
        try {
            Invariants.ACTIVE_DS.set(ds);
            for (Invariant inv : scen.invariants()) {
                InvariantResult ir;
                try {
                    ir = inv.check(svc);
                } catch (Exception ex) {
                    ir = new InvariantResult(
                        inv.name(), false,
                        "raised " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
                }
                if (ir == null) {
                    ir = new InvariantResult(inv.name(), false, "null result");
                }
                report.addResult(ir);
            }
        } finally {
            Invariants.ACTIVE_DS.set(prior);
        }
    }
}
