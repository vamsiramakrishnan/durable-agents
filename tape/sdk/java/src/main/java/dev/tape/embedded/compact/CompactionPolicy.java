package dev.tape.embedded.compact;

/**
 * Knobs for the compactor — Java port of {@code tape_adk.compact.CompactionPolicy}.
 *
 * <p>Defaults match the Python reference exactly:
 *
 * <ul>
 *   <li>{@code effectTtlMs = 7 days}</li>
 *   <li>{@code sessionTtlMs = 30 days}</li>
 *   <li>{@code archiveTerminalObligations = true}</li>
 *   <li>{@code archiveFiredTimers = true}</li>
 *   <li>{@code maxPerTick = 1000}</li>
 * </ul>
 */
public record CompactionPolicy(
        long effectTtlMs,
        long sessionTtlMs,
        boolean archiveTerminalObligations,
        boolean archiveFiredTimers,
        int maxPerTick) {

    public static final long DEFAULT_EFFECT_TTL_MS = 7L * 24 * 60 * 60 * 1000;
    public static final long DEFAULT_SESSION_TTL_MS = 30L * 24 * 60 * 60 * 1000;
    public static final int  DEFAULT_MAX_PER_TICK = 1000;

    /** Default policy — matches Python reference. */
    public static CompactionPolicy defaults() {
        return new CompactionPolicy(
            DEFAULT_EFFECT_TTL_MS, DEFAULT_SESSION_TTL_MS,
            true, true, DEFAULT_MAX_PER_TICK);
    }

    /** Builder-flavoured factory with the most common knob. */
    public static CompactionPolicy withEffectTtl(long effectTtlMs) {
        return new CompactionPolicy(
            effectTtlMs, DEFAULT_SESSION_TTL_MS,
            true, true, DEFAULT_MAX_PER_TICK);
    }
}
