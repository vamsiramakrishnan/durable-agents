package dev.tape;

import java.util.List;

/**
 * Tenancy — the DX-correct surface for *future* hard multi-tenancy.
 * Mirrors {@code tape.tenancy} in Python.
 */
public final class Tenancy {
    private Tenancy() {}

    public enum Mode {
        SINGLE("single"),
        TRUSTED_MULTI_APP("trusted_multi_app"),
        HARD_MULTI_TENANT("hard_multi_tenant");

        public final String wire;
        Mode(String w) { this.wire = w; }

        public static Mode fromString(String s) {
            if (s == null || s.isEmpty()) return SINGLE;
            for (Mode m : values()) if (m.wire.equals(s)) return m;
            throw new IllegalArgumentException("unknown tenancy mode: " + s);
        }
    }

    /** Configuration consumed by the SDK / `tape doctor`. */
    public static final class Config {
        public final Mode mode;
        public final String tenantId;
        public Config(Mode m, String t) {
            this.mode = m == null ? Mode.SINGLE : m;
            this.tenantId = (t == null || t.isEmpty()) ? "default" : t;
        }
        public Config() { this(Mode.SINGLE, "default"); }

        public boolean isHard() { return mode == Mode.HARD_MULTI_TENANT; }

        public List<String> warnIfHardButUnenforced() {
            if (!isHard()) return List.of();
            return List.of(
                "tenancy.mode=hard_multi_tenant requested but the Tape proto and stores " +
                "do not yet carry a first-class tenant_id. Cross-tenant data isolation " +
                "cannot be enforced at the runtime; this mode is DESIGN-ONLY today.");
        }
    }

    public static Config defaults() { return new Config(); }

    public static Config fromEnv() {
        return new Config(Mode.fromString(System.getenv("TAPE_TENANCY")),
                          System.getenv("TAPE_TENANT_ID"));
    }
}
