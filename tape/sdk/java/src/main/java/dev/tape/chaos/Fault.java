package dev.tape.chaos;

/**
 * One declared chaos rule. Two layers: SERVER (a named failpoint in the
 * Rust catalogue) and CONNECTOR (a wrap around a registered connector).
 *
 * <p>See {@code design-principles/chaos.md} for the full design and
 * {@link Faults} for static constructors.
 */
public record Fault(
        Layer layer,
        String target,         // failpoint name OR connector name
        String action,         // panic/sleep/return (server) OR lose_ack/duplicate/delay (connector)
        double probability,
        int afterN,            // server only: skip first N hits
        int ms,                // delay length
        double jitter,
        String when,           // selector (Phase 2 CEL); recorded for the report
        String actionMsg       // for `error(msg=...)`
) {
    public enum Layer { SERVER, CONNECTOR }

    public Fault {
        if (target == null) throw new IllegalArgumentException("target required");
        if (action == null) action = "";
        if (when == null)   when   = "";
        if (actionMsg == null) actionMsg = "";
    }
}
