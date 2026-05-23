package dev.tape.embedded;

/**
 * What an inverse-operation ({@link Connector#compensate}) call did.
 *
 * <p>Mirrors {@code tape_adk.connectors.CompensationResult}.
 */
public record CompensationResult(
        String status,         // 'compensated' | 'failed'
        Object response,
        Object error,
        long retryAfterMs) {

    public static final String COMPENSATED = "compensated";
    public static final String FAILED      = "failed";

    public static CompensationResult compensated(Object response) {
        return new CompensationResult(COMPENSATED, response, null, 0);
    }

    public static CompensationResult failed(Object error, long retryAfterMs) {
        return new CompensationResult(FAILED, null, error, retryAfterMs);
    }
}
