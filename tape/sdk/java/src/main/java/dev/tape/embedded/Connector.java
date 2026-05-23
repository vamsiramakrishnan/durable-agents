package dev.tape.embedded;

/**
 * Connector protocol for the embedded SQL path — three methods that
 * mirror {@code tape_adk.connectors.Connector}.
 *
 * <p>A connector implements three operations against one upstream system
 * ({@code bank.wire}, {@code payment.charge}, {@code email.send}, ...):
 *
 * <ul>
 *   <li>{@link #dispatch} — actually call the upstream. Returns CONFIRMED
 *       (success), UNKNOWN (call may have landed but the ack was lost),
 *       FAILED (definitively didn't do it), or throws (treated as
 *       retry-after-backoff).</li>
 *   <li>{@link #observe} — ask the upstream by {@code business_key} whether
 *       the operation lives in its records. The reconciler's only window
 *       into the counterparty's reality.</li>
 *   <li>{@link #compensate} — run the inverse (reverse a wire, refund a
 *       charge).</li>
 * </ul>
 *
 * <p>The connector is the ONE place in the system that's allowed to call
 * the upstream — the agent's tool body never does (that's what makes the
 * OUTBOX contract structural).
 *
 * <p>This is a different (and simpler) interface than
 * {@code dev.tape.connectors.Connector}, which is the gRPC-path connector
 * with richer outcome enums and an {@code Effect} POJO. The embedded path
 * intentionally mirrors the Python {@code Connector} protocol so a
 * connector author writing in Python and one writing in Java agree on
 * shape.
 */
public interface Connector {

    /** Registry key — the same string used in the effect's {@code connector}
     *  field, and what {@code dispatchOutboxOnce}/{@code reconcileOnce} look
     *  up the implementation by. */
    String name();

    DispatchResult dispatch(Schema.EffectRecord effect) throws Exception;

    ObservationResult observe(Schema.EffectRecord effect) throws Exception;

    CompensationResult compensate(Schema.ObligationRecord obligation) throws Exception;
}
