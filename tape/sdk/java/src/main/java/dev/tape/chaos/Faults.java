package dev.tape.chaos;

import java.util.ArrayList;
import java.util.List;

/**
 * Static factories for {@link Fault} + the {@link #failpointsEnv} renderer.
 * Mirrors {@code tape.chaos.crash/delay/error/loseAck/...} (Python/TS/Go).
 */
public final class Faults {
    private Faults() {}

    // ── server-layer ───────────────────────────────────────────────────

    public static Fault crash(String failpoint) {
        return new Fault(Fault.Layer.SERVER, failpoint, "panic", 1.0, 0, 0, 0.0, "", "");
    }

    public static Fault crash(String failpoint, double probability) {
        return new Fault(Fault.Layer.SERVER, failpoint, "panic", probability, 0, 0, 0.0, "", "");
    }

    public static Fault crashAfter(String failpoint, int afterN) {
        return new Fault(Fault.Layer.SERVER, failpoint, "panic", 1.0, afterN, 0, 0.0, "", "");
    }

    public static Fault delay(String failpoint, int ms) {
        return new Fault(Fault.Layer.SERVER, failpoint, "sleep", 1.0, 0, ms, 0.0, "", "");
    }

    public static Fault error(String failpoint, String msg) {
        return new Fault(Fault.Layer.SERVER, failpoint, "return", 1.0, 0, 0, 0.0, "", msg);
    }

    // ── connector-layer ────────────────────────────────────────────────

    public static Fault loseAck(String connector, double probability) {
        return new Fault(Fault.Layer.CONNECTOR, connector, "lose_ack", probability, 0, 0, 0.0, "", "");
    }

    public static Fault duplicate(String connector, double probability) {
        return new Fault(Fault.Layer.CONNECTOR, connector, "duplicate", probability, 0, 0, 0.0, "", "");
    }

    public static Fault delayConnector(String connector, int ms) {
        return new Fault(Fault.Layer.CONNECTOR, connector, "delay", 1.0, 0, ms, 0.0, "", "");
    }

    // ── FAILPOINTS env rendering ───────────────────────────────────────

    /** Render one server-layer fault to the fail-rs spec string. */
    static String toFailSpec(Fault f) {
        String action = f.action();
        switch (action) {
            case "sleep":  action = "sleep(" + f.ms() + ")"; break;
            case "return": action = "return(" + (f.actionMsg().isEmpty() ? "chaos" : f.actionMsg()) + ")"; break;
            case "print":  action = "print("  + (f.actionMsg().isEmpty() ? "chaos" : f.actionMsg()) + ")"; break;
            default: /* panic, off, yield, pause */
        }
        List<String> parts = new ArrayList<>(2);
        if (f.afterN() > 0) parts.add(f.afterN() + "*off");
        double p = f.probability();
        if (p > 0 && p < 1.0) {
            parts.add(formatProb(p) + "*" + action);
        } else {
            parts.add(action);
        }
        return f.target() + "=" + String.join("->", parts);
    }

    private static String formatProb(double p) {
        // Match Python's `%g`: trim trailing zeros, no exponent for normal range.
        String s = String.format("%.6g", p);
        if (s.contains(".") && !s.contains("e") && !s.contains("E")) {
            int i = s.length() - 1;
            while (i > 0 && s.charAt(i) == '0') i--;
            if (s.charAt(i) == '.') i--;
            s = s.substring(0, i + 1);
        }
        return s;
    }

    /**
     * Render the server-layer faults of {@code scen} to the {@code FAILPOINTS}
     * env-var value. Connector-layer faults are applied in-process via
     * {@link ChaosSession}.
     */
    public static String failpointsEnv(Scenario scen) {
        List<String> specs = new ArrayList<>();
        for (Fault f : scen.faults()) {
            if (f.layer() == Fault.Layer.SERVER) specs.add(toFailSpec(f));
        }
        return String.join(";", specs);
    }
}
