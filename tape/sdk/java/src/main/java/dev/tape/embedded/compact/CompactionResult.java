package dev.tape.embedded.compact;

/**
 * What one {@code compactOnce} tick did — Java port of
 * {@code tape_adk.compact.CompactionResult}.
 */
public record CompactionResult(
        int effectsPruned,
        int obligationsPruned,
        int timersPruned,
        int sessionsArchived) {

    public static CompactionResult empty() {
        return new CompactionResult(0, 0, 0, 0);
    }

    public int total() {
        return effectsPruned + obligationsPruned + timersPruned + sessionsArchived;
    }

    /** Per-row addition; used internally during a tick. */
    CompactionResult plus(int e, int o, int t, int s) {
        return new CompactionResult(
            effectsPruned + e, obligationsPruned + o,
            timersPruned + t, sessionsArchived + s);
    }
}
