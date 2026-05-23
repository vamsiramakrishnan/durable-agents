package dev.tape.embedded;

/**
 * What {@link Connector#observe} found on the counterparty's side.
 *
 * <p>Mirrors {@code tape_adk.connectors.ObservationResult}.
 */
public record ObservationResult(
        String status,          // 'confirmed' | 'failed' | 'absent' | 'duplicate'
        String externalRef,
        Object response,
        Object error,
        /** When status='duplicate', the obligation kind the reconciler
         *  should atomically register so the drainer can reverse the
         *  surplus record. Empty string disables compensation registration. */
        String compensateKind) {

    public static final String CONFIRMED = "confirmed";
    public static final String FAILED    = "failed";
    public static final String ABSENT    = "absent";
    public static final String DUPLICATE = "duplicate";

    public static ObservationResult confirmed(String externalRef, Object response) {
        return new ObservationResult(CONFIRMED, externalRef == null ? "" : externalRef,
                response, null, "");
    }

    public static ObservationResult absent() {
        return new ObservationResult(ABSENT, "", null, null, "");
    }

    public static ObservationResult duplicate(String externalRef, String compensateKind) {
        return new ObservationResult(DUPLICATE, externalRef == null ? "" : externalRef,
                null, null, compensateKind == null ? "" : compensateKind);
    }

    public static ObservationResult failed(Object error) {
        return new ObservationResult(FAILED, "", null, error, "");
    }
}
