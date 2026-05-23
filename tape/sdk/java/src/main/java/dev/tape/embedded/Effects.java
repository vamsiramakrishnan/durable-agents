package dev.tape.embedded;

import java.util.concurrent.Callable;

/**
 * Java-flavoured equivalent of Python's {@code @effect} decorator. Since
 * Java doesn't have function decorators, expose a wrapper:
 * {@link #tracked(Callable)} returns a {@link TrackedEffect} handle that
 * carries the {@code @effect}-style metadata (semantics=IDEMPOTENT,
 * dispatch_mode=INLINE) and the callable body.
 *
 * <p>A future ADK-Java plugin would read the metadata off this handle,
 * journal an intent via {@link TapeSessionService#beginEffect}, run the
 * body, and call {@link TapeSessionService#completeEffect}. That plugin
 * isn't in this pass — this class just provides the construction-time
 * shape so callers can declare effects today and wire them tomorrow.
 */
public final class Effects {

    private Effects() {}

    /** Metadata + body. The body MUST be safe to call multiple times. */
    public record TrackedEffect<T>(
            String semantics,
            String dispatchMode,
            Callable<T> body) {

        public TrackedEffect {
            if (body == null) throw new IllegalArgumentException("body is required");
            if (semantics == null) semantics = Schema.EffectRecord.IDEMPOTENT;
            if (dispatchMode == null) dispatchMode = Schema.EffectRecord.INLINE;
        }

        /** Run the body directly. Useful for tests and trivial in-process use
         *  where you don't need journaling. */
        public T call() throws Exception {
            return body.call();
        }
    }

    /** Wrap a body as a tracked idempotent inline effect. */
    public static <T> TrackedEffect<T> tracked(Callable<T> body) {
        return new TrackedEffect<>(
            Schema.EffectRecord.IDEMPOTENT,
            Schema.EffectRecord.INLINE,
            body);
    }
}
