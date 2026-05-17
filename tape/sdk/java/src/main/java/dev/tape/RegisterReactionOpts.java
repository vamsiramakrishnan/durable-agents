package dev.tape;

import dev.tape.proto.HandlerKind;

/**
 * Options for {@link TapeClient#registerReaction(RegisterReactionOpts)}.
 *
 * <p>Mirrors the kwargs of {@code register_reaction} in the Python SDK. All
 * fields default to the same defaults the server applies for empty values, so
 * callers only need to set what differs. {@code subjectPattern} and
 * {@code handlerKind} are required (a zero/null value will fail server-side).
 */
public final class RegisterReactionOpts {
    public String reactionId = "";
    public String name = "";
    public String subjectPattern;
    public String predicateCel = "";
    public HandlerKind handlerKind = HandlerKind.HANDLER_KIND_TASK;
    public String agentApp = "";
    public String publishTarget = "";
    public int maxConcurrency = 1;
    public int rateLimitPerS = 0;
    public int debounceMs = 0;
    public int retryMax = 5;
    public int retryBackoffMs = 1000;
    public int dlqAfterN = 5;
    public int numShards = 1;
    public boolean bootstrapFromHead = false;

    public RegisterReactionOpts reactionId(String v) { this.reactionId = v == null ? "" : v; return this; }
    public RegisterReactionOpts name(String v) { this.name = v == null ? "" : v; return this; }
    public RegisterReactionOpts subjectPattern(String v) { this.subjectPattern = v; return this; }
    public RegisterReactionOpts predicateCel(String v) { this.predicateCel = v == null ? "" : v; return this; }
    public RegisterReactionOpts handlerKind(HandlerKind v) { this.handlerKind = v; return this; }
    public RegisterReactionOpts agentApp(String v) { this.agentApp = v == null ? "" : v; return this; }
    public RegisterReactionOpts publishTarget(String v) { this.publishTarget = v == null ? "" : v; return this; }
    public RegisterReactionOpts maxConcurrency(int v) { this.maxConcurrency = v; return this; }
    public RegisterReactionOpts rateLimitPerS(int v) { this.rateLimitPerS = v; return this; }
    public RegisterReactionOpts debounceMs(int v) { this.debounceMs = v; return this; }
    public RegisterReactionOpts retryMax(int v) { this.retryMax = v; return this; }
    public RegisterReactionOpts retryBackoffMs(int v) { this.retryBackoffMs = v; return this; }
    public RegisterReactionOpts dlqAfterN(int v) { this.dlqAfterN = v; return this; }
    public RegisterReactionOpts numShards(int v) { this.numShards = v; return this; }
    public RegisterReactionOpts bootstrapFromHead(boolean v) { this.bootstrapFromHead = v; return this; }
}
