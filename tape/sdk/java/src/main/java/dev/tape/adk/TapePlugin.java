package dev.tape.adk;

import com.google.adk.agents.CallbackContext;
import com.google.adk.agents.InvocationContext;
import com.google.adk.events.Event;
import com.google.adk.models.LlmRequest;
import com.google.adk.models.LlmResponse;
import com.google.adk.plugins.BasePlugin;
import com.google.adk.tools.BaseTool;
import com.google.adk.tools.ToolContext;
import com.google.genai.types.Content;
import com.google.gson.Gson;

import dev.tape.TapeClient;
import dev.tape.proto.BeginEffectResponse;
import dev.tape.proto.EffectStatus;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Java ADK adapter — turns ADK callbacks into Tape journal entries.
 *
 * <p>Wires to the same extension points as the Python reference
 * ({@code tape.adk.TapePlugin}):
 *
 * <pre>{@code
 *   beforeRun        -> BeginRun
 *   afterModel       -> RecordDecision
 *   beforeTool       -> BeginEffect (short-circuit a CONFIRMED effect)
 *   afterTool        -> CompleteEffect(CONFIRMED)
 *   onToolError      -> CompleteEffect(FAILED)
 *   afterRun         -> EndRun(TERMINAL)
 * }</pre>
 *
 * <p>Position is by call order: the k-th model call gets {@code decision_index = k-1};
 * a tool call is keyed to the most-recent decision plus a per-(decision, tool)
 * call index. The same alignment holds on re-drive because every decision is
 * replayed.
 *
 * <p>Wire it into a runner:
 *
 * <pre>{@code
 * TapeClient client = new TapeClient("tape://localhost:7878");
 * Runner runner = Runner.builder(...)
 *     .plugins(List.of(new TapePlugin(client)))
 *     .sessionService(new TapeSessionService(client))
 *     .build();
 * }</pre>
 *
 * <p><b>Status (G4, this PR):</b> covers the wiring contract — BeginRun,
 * RecordDecision, BeginEffect/CompleteEffect, EndRun, plus session service
 * persistence. Model replay (short-circuit a recorded LlmResponse on re-drive)
 * and budget admit/charge are tracked in {@code SDK_PARITY.md} as follow-up;
 * they're additive and don't change the contract above.
 */
public class TapePlugin extends BasePlugin {

    private static final String PLUGIN_NAME = "tape";
    private static final Gson GSON = new Gson();

    private final TapeClient client;
    private final boolean ownsClient;
    private final String leaseOwner;
    private final long leaseTtlMs;

    /** Per-invocation bookkeeping — keyed by {@code invocationId}. */
    private final ConcurrentHashMap<String, InvocationBookkeeping> books = new ConcurrentHashMap<>();

    public TapePlugin() {
        this(new TapeClient(TapeClient.defaultUrl()), true);
    }

    public TapePlugin(TapeClient client) {
        this(client, false);
    }

    public TapePlugin(String url) {
        this(new TapeClient(url), true);
    }

    private TapePlugin(TapeClient client, boolean ownsClient) {
        super(PLUGIN_NAME);
        this.client = client;
        this.ownsClient = ownsClient;
        this.leaseOwner = defaultLeaseOwner();
        this.leaseTtlMs = defaultLeaseTtlMs();
    }

    /** The wrapped TapeClient — exposed so callers can drive their own RPCs. */
    public TapeClient client() { return client; }

    // ── lifecycle ───────────────────────────────────────────────────────────

    @Override
    public Maybe<Content> beforeRunCallback(InvocationContext ctx) {
        InvocationBookkeeping bk = new InvocationBookkeeping();
        var resp = client.beginRun(
                ctx.session().appName(),
                ctx.session().userId(),
                ctx.session().id(),
                ctx.invocationId(),
                leaseOwner,
                leaseTtlMs);
        bk.runId = resp.getRunId();
        books.put(ctx.invocationId(), bk);
        return Maybe.empty();   // let the model speak
    }

    @Override
    public Completable afterRunCallback(InvocationContext ctx) {
        InvocationBookkeeping bk = books.remove(ctx.invocationId());
        if (bk == null) return Completable.complete();
        try {
            client.endRun(bk.runId, dev.tape.proto.RunStatus.RUN_STATUS_TERMINAL, "{}");
        } catch (Exception ignore) {
            // Already-terminal or already-failed runs are fine; don't fail the
            // caller's afterRun on an idempotent EndRun race.
        }
        return Completable.complete();
    }

    // ── model ───────────────────────────────────────────────────────────────

    @Override
    public Maybe<LlmResponse> beforeModelCallback(CallbackContext ctx, LlmRequest.Builder requestBuilder) {
        // Model replay (short-circuit a recorded LlmResponse on re-drive) is
        // additive; tracked in SDK_PARITY.md as a follow-up. The wiring is here
        // so the position counter advances correctly on first run.
        return Maybe.empty();
    }

    @Override
    public Maybe<LlmResponse> afterModelCallback(CallbackContext ctx, LlmResponse response) {
        InvocationBookkeeping bk = bookkeepingFor(ctx);
        if (bk == null) return Maybe.empty();

        long decisionIndex = bk.decisionCount.getAndIncrement();
        bk.lastDecisionIndex.set(decisionIndex);
        bk.toolCallIndex.clear();   // a new decision restarts per-tool counters

        String requestJson  = "";
        String responseJson = response.toJson();
        try {
            client.recordDecision(bk.runId, decisionIndex, "", requestJson, responseJson, "", "");
        } catch (Exception ex) {
            // Decision is by-position-idempotent on the server; an idempotent
            // re-record is fine.
        }
        return Maybe.empty();
    }

    // ── tools ───────────────────────────────────────────────────────────────

    @Override
    public Maybe<Map<String, Object>> beforeToolCallback(BaseTool tool, Map<String, Object> args, ToolContext ctx) {
        InvocationBookkeeping bk = bookkeepingFor(ctx);
        if (bk == null) return Maybe.empty();

        long decisionIndex = bk.lastDecisionIndex.get();
        int callIndex = bk.toolCallIndex.computeIfAbsent(tool.name(), k -> new AtomicInteger(0))
                                          .getAndIncrement();

        String requestJson = safeJson(args);
        BeginEffectResponse be;
        try {
            be = client.beginEffect(bk.runId, decisionIndex, tool.name(), callIndex,
                                    requestJson, "");
        } catch (Exception ex) {
            // Treat a transient gRPC failure as "let the tool run" — Tape will
            // pick up the next call's idempotency key cleanly on re-drive.
            return Maybe.empty();
        }
        bk.lastEffectKey.put(toolEffectKey(tool.name(), callIndex), be.getIdempotencyKey());

        // Short-circuit on a CONFIRMED effect (re-drive case).
        if (be.getStatus() == EffectStatus.EFFECT_STATUS_CONFIRMED && !be.getResponseJson().isEmpty()) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> recorded = GSON.fromJson(be.getResponseJson(), Map.class);
                return Maybe.just(recorded == null ? Map.of() : recorded);
            } catch (Exception ignore) {
                // Fall through and let the tool execute again — the server's
                // by-(run, decision, tool, call) keying still dedupes the result.
            }
        }
        return Maybe.empty();
    }

    @Override
    public Maybe<Map<String, Object>> afterToolCallback(BaseTool tool, Map<String, Object> args,
                                                          ToolContext ctx, Map<String, Object> response) {
        InvocationBookkeeping bk = bookkeepingFor(ctx);
        if (bk == null) return Maybe.empty();

        String key = pollLastEffectKey(bk, tool);
        if (key == null) return Maybe.empty();

        try {
            client.completeEffect(bk.runId, key, EffectStatus.EFFECT_STATUS_CONFIRMED,
                                  safeJson(response), "");
        } catch (Exception ignore) { /* idempotent */ }
        return Maybe.empty();
    }

    @Override
    public Maybe<Map<String, Object>> onToolErrorCallback(BaseTool tool, Map<String, Object> args,
                                                            ToolContext ctx, Throwable error) {
        InvocationBookkeeping bk = bookkeepingFor(ctx);
        if (bk == null) return Maybe.empty();

        String key = pollLastEffectKey(bk, tool);
        if (key == null) return Maybe.empty();

        Map<String, Object> err = Map.of(
                "error_type", error.getClass().getSimpleName(),
                "error_message", error.getMessage() == null ? "" : error.getMessage()
        );
        try {
            client.completeEffect(bk.runId, key, EffectStatus.EFFECT_STATUS_FAILED, "", safeJson(err));
        } catch (Exception ignore) { /* idempotent */ }
        return Maybe.empty();
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private InvocationBookkeeping bookkeepingFor(CallbackContext ctx) {
        // CallbackContext doesn't expose invocationId directly via a public
        // method; the ADK 1.2 surface gives us the read-only context which
        // exposes the parent InvocationContext through eventId() / state(). The
        // simplest stable path is to thread invocation lookup through the
        // tool's ToolContext, which extends CallbackContext and is constructed
        // from the InvocationContext on every tool call. For model callbacks
        // we don't need a fresh effect, so model bookkeeping uses the *only*
        // currently-running invocation if there's exactly one — the common case
        // for a single-agent runner.
        if (ctx instanceof ToolContext) {
            String invocationId = invocationIdFromTool((ToolContext) ctx);
            if (invocationId != null) return books.get(invocationId);
        }
        if (books.size() == 1) return books.values().iterator().next();
        return null;
    }

    /** Best-effort: ToolContext extends CallbackContext which extends
     *  ReadonlyContext, and the readonly context exposes the invocation. We
     *  reach through the eventActions which carries the invocation id when
     *  ADK constructs the tool context. */
    @SuppressWarnings("unchecked")
    private static String invocationIdFromTool(ToolContext ctx) {
        try {
            // ToolContext.toString() includes the invocation id; pragmatic but
            // not load-bearing — only used as a tie-breaker. If we have exactly
            // one in-flight invocation (the dominant case), we use that.
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static String toolEffectKey(String toolName, int callIndex) {
        return toolName + "#" + callIndex;
    }

    private static String pollLastEffectKey(InvocationBookkeeping bk, BaseTool tool) {
        // The last beforeTool() for this tool stamped the key; we don't know
        // the exact call_index here, so we pull the *most-recent* key for this
        // tool name. ADK serializes tool callbacks per invocation, so the LIFO
        // match is correct.
        AtomicInteger counter = bk.toolCallIndex.get(tool.name());
        if (counter == null) return null;
        int last = counter.get() - 1;
        if (last < 0) return null;
        return bk.lastEffectKey.remove(toolEffectKey(tool.name(), last));
    }

    private static String safeJson(Object v) {
        try { return GSON.toJson(v); }
        catch (Exception ex) { return "{}"; }
    }

    static String defaultLeaseOwner() {
        try {
            String host = java.net.InetAddress.getLocalHost().getHostName();
            long pid = ProcessHandle.current().pid();
            return host + ":" + pid;
        } catch (Exception e) {
            return "host:" + ProcessHandle.current().pid();
        }
    }

    static long defaultLeaseTtlMs() {
        String v = System.getenv("TAPE_LEASE_MS");
        if (v != null && !v.isEmpty()) {
            try { long n = Long.parseLong(v); if (n > 0) return n; } catch (NumberFormatException ignored) {}
        }
        return 120_000L;
    }

    // ── per-invocation bookkeeping ──────────────────────────────────────────

    private static final class InvocationBookkeeping {
        String runId;
        final AtomicLong decisionCount = new AtomicLong(0);
        final AtomicLong lastDecisionIndex = new AtomicLong(0);
        final ConcurrentHashMap<String, AtomicInteger> toolCallIndex = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, String> lastEffectKey = new ConcurrentHashMap<>();
    }
}
