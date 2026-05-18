package dev.tape.chaos;

/** Outcome of one {@link Invariant#check} call. */
public record InvariantResult(String name, boolean passed, String detail) {
    public InvariantResult {
        if (detail == null) detail = "";
    }
    public static InvariantResult ok(String name)               { return new InvariantResult(name, true, ""); }
    public static InvariantResult ok(String name, String d)     { return new InvariantResult(name, true, d); }
    public static InvariantResult fail(String name, String d)   { return new InvariantResult(name, false, d); }

    @Override public String toString() {
        String mark = passed ? "OK " : "FAIL";
        return detail.isEmpty() ? "[" + mark + "] " + name
                                : "[" + mark + "] " + name + ": " + detail;
    }
}
