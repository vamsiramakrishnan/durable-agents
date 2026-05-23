package dev.tape.embedded;

/**
 * What a single {@link Connector#dispatch} attempt produced. {@code status}
 * drives the server-side transition: {@code confirmed} → effect done;
 * {@code unknown} → reconciler takes over; {@code failed} → re-dispatch
 * after backoff (or terminal-failed if {@code retryAfterMs < 0}).
 *
 * <p>Mirrors {@code tape_adk.connectors.DispatchResult}.
 */
public record DispatchResult(
        String status,          // 'confirmed' | 'unknown' | 'failed'
        String externalRef,
        Object response,
        Object error,
        /** Backoff hint for the dispatcher — only honored when status='failed'.
         *  0 means "use the dispatcher's default exponential backoff".
         *  Negative means "give up — mark FAILED terminal". */
        long retryAfterMs) {

    public static final String CONFIRMED = "confirmed";
    public static final String UNKNOWN   = "unknown";
    public static final String FAILED    = "failed";

    public static DispatchResult confirmed(String externalRef, Object response) {
        return new DispatchResult(CONFIRMED, externalRef == null ? "" : externalRef,
                response, null, 0);
    }

    public static DispatchResult unknown(Object error) {
        return new DispatchResult(UNKNOWN, "", null, error, 0);
    }

    public static DispatchResult failed(Object error, long retryAfterMs) {
        return new DispatchResult(FAILED, "", null, error, retryAfterMs);
    }
}
