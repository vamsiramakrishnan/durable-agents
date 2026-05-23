package dev.tape.embedded.chaos;

/**
 * One declarative fault — Java port of {@code tape_adk.chaos.Fault}.
 *
 * <p>The embedded module only consumes the connector layer; server-layer
 * failpoints aren't applicable here (there's no separate server). Two
 * scopes: {@code target} (connector name) and {@code tool} (tool name).
 * Exactly one of those should be set; the static factories on
 * {@link Chaos} enforce that.
 *
 * <p>Same shape as {@code dev.tape.chaos.Fault} for portability — but in
 * the embedded tier, the {@code layer} is always implicitly "connector"
 * because we have no separate server failpoint mechanism.
 */
public record Fault(
        String layer,
        String target,        // connector name when target-scoped
        String tool,          // tool name when tool-scoped
        String action,        // "lose_ack" | "duplicate" | "delay"
        double probability,
        long ms,
        double jitter) {

    /** The only supported layer in the embedded tier. */
    public static final String LAYER_CONNECTOR = "connector";

    /** Compact normalising constructor — null strings become empty. */
    public Fault {
        if (layer == null) layer = LAYER_CONNECTOR;
        if (target == null) target = "";
        if (tool == null) tool = "";
        if (action == null) action = "";
    }
}
