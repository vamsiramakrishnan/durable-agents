package dev.tape.embedded.chaos;

/**
 * Result of one invariant check — Java port of
 * {@code tape_adk.chaos.InvariantResult}.
 */
public record InvariantResult(String name, boolean passed, String detail) {

    public InvariantResult(String name, boolean passed) {
        this(name, passed, "");
    }

    @Override
    public String toString() {
        String mark = passed ? "OK " : "FAIL";
        if (detail == null || detail.isEmpty()) return "[" + mark + "] " + name;
        return "[" + mark + "] " + name + ": " + detail;
    }
}
