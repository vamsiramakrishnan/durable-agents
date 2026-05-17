package dev.tape;

/**
 * Options for {@link TapeClient#claimTasks(ClaimTasksOpts)}.
 *
 * <p>Mirrors the kwargs of {@code claim_tasks} in the Python SDK. {@code reactionId}
 * and {@code owner} are required.
 */
public final class ClaimTasksOpts {
    public String reactionId;
    public int shard = -1;              // <0 => any shard
    public String owner;
    public long leaseMs = 60_000L;      // <=0 => server default (60s)
    public int max = 16;                // <=0 => server default (16)
    public long nowMs = 0L;             // 0 => server time

    public ClaimTasksOpts reactionId(String v) { this.reactionId = v; return this; }
    public ClaimTasksOpts shard(int v) { this.shard = v; return this; }
    public ClaimTasksOpts owner(String v) { this.owner = v; return this; }
    public ClaimTasksOpts leaseMs(long v) { this.leaseMs = v; return this; }
    public ClaimTasksOpts max(int v) { this.max = v; return this; }
    public ClaimTasksOpts nowMs(long v) { this.nowMs = v; return this; }
}
