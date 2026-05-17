package dev.tape;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import dev.tape.proto.*;

/**
 * Tape — the Java client over the {@code tape.v1} gRPC service.
 *
 * <p>URL schemes:
 * <ul>
 *   <li>{@code tape://host:port} — plaintext gRPC (self-hosted, k8s, local).</li>
 *   <li>{@code tapes://host} — TLS on :443 (Cloud Run / any HTTPS endpoint). For
 *       IAM-protected endpoints, pass an ID token via {@link Options#idToken}
 *       (mint it however your app does — google-auth-library-java, GCP metadata
 *       server, etc.).</li>
 * </ul>
 */
public final class TapeClient implements AutoCloseable {

    public static String defaultUrl() {
        String v = System.getenv("TAPE_URL");
        return v != null ? v : "tape://localhost:7878";
    }

    public static final class Options {
        public String idToken;   // optional static Bearer token (for tapes:// + IAM-protected)
        public Options idToken(String t) { this.idToken = t; return this; }
    }

    public final String url;
    private final ManagedChannel channel;
    private final TapeGrpc.TapeBlockingStub stub;

    public TapeClient(String url) { this(url, new Options()); }

    public TapeClient(String url, Options opts) {
        this.url = url;
        boolean secure = url.startsWith("tapes://") || url.startsWith("grpcs://");
        String host = url.replaceFirst("^(tapes?://|grpcs?://)", "");
        if (secure && !host.contains(":")) host = host + ":443";

        ManagedChannelBuilder<?> b;
        if (secure) {
            try {
                b = NettyChannelBuilder.forTarget(host).sslContext(GrpcSslContexts.forClient().build());
            } catch (javax.net.ssl.SSLException e) { throw new RuntimeException(e); }
        } else {
            b = ManagedChannelBuilder.forTarget(host).usePlaintext();
        }
        this.channel = b.build();
        TapeGrpc.TapeBlockingStub s = TapeGrpc.newBlockingStub(this.channel);
        if (opts != null && opts.idToken != null && !opts.idToken.isEmpty()) {
            io.grpc.Metadata md = new io.grpc.Metadata();
            md.put(io.grpc.Metadata.Key.of("authorization", io.grpc.Metadata.ASCII_STRING_MARSHALLER), "Bearer " + opts.idToken);
            s = s.withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(md));
        }
        this.stub = s;
    }

    @Override public void close() {
        try { channel.shutdown().awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
    }

    public TapeGrpc.TapeBlockingStub pb() { return stub; }

    // ── run lifecycle ───────────────────────────────────────────────────────

    public BeginRunResponse beginRun(String app, String user, String session, String invocation,
                                     String leaseOwner, long leaseTtlMs) {
        return stub.beginRun(BeginRunRequest.newBuilder()
                .setAppName(app).setUserId(user).setSessionId(session).setInvocationId(invocation)
                .setLeaseOwner(leaseOwner).setLeaseTtlMs(leaseTtlMs == 0 ? 120_000 : leaseTtlMs).build());
    }

    public EndRunResponse endRun(String runId, RunStatus status, String detailJson) {
        return stub.endRun(EndRunRequest.newBuilder().setRunId(runId)
                .setStatus(status == null ? RunStatus.RUN_STATUS_TERMINAL : status)
                .setDetailJson(detailJson == null ? "" : detailJson).build());
    }

    public RunState getRun(String runId) {
        return stub.getRun(GetRunRequest.newBuilder().setRunId(runId).build());
    }

    public ListRunsToRecoverResponse listRunsToRecover(long limit) {
        return stub.listRunsToRecover(ListRunsToRecoverRequest.newBuilder().setLimit(limit == 0 ? 100 : limit).build());
    }

    // ── decisions ───────────────────────────────────────────────────────────

    public DecisionRecord recordDecision(String runId, long idx, String model, String requestJson,
                                         String responseJson, String rationale, String policyVersion) {
        return stub.recordDecision(RecordDecisionRequest.newBuilder()
                .setRunId(runId).setDecisionIndex(idx).setModel(model == null ? "" : model)
                .setRequestJson(requestJson == null ? "" : requestJson)
                .setResponseJson(responseJson == null ? "" : responseJson)
                .setRationale(rationale == null ? "" : rationale)
                .setPolicyVersion(policyVersion == null ? "" : policyVersion).build());
    }

    public GetDecisionResponse getDecision(String runId, long idx) {
        return stub.getDecision(GetDecisionRequest.newBuilder().setRunId(runId).setDecisionIndex(idx).build());
    }

    // ── effects ─────────────────────────────────────────────────────────────

    /** v1-compatible: idempotent + inline. Use the extended form to opt into
     *  the outbox path for non-idempotent upstreams. */
    public BeginEffectResponse beginEffect(String runId, long decisionIndex, String toolName, int callIndex,
                                           String requestJson, String customKey) {
        return beginEffect(runId, decisionIndex, toolName, callIndex, requestJson, customKey,
                EffectSemantics.EFFECT_SEMANTICS_UNSPECIFIED,
                EffectDispatchMode.EFFECT_DISPATCH_MODE_UNSPECIFIED, "", "");
    }

    /** Extended: declare the outbox contract.
     *
     *  <p>{@code semantics=NON_IDEMPOTENT} requires {@code dispatchMode=OUTBOX};
     *  the server refuses {@code NON_IDEMPOTENT + INLINE} (an inline call to a
     *  counterparty that can't dedupe is unsafe by construction).
     *  {@code businessKey} (when set) is enforced unique on
     *  {@code (connector, businessKey)} across all runs — a second
     *  {@code beginEffect} for the same business identity returns the existing
     *  effect row. */
    public BeginEffectResponse beginEffect(String runId, long decisionIndex, String toolName, int callIndex,
                                           String requestJson, String customKey,
                                           EffectSemantics semantics, EffectDispatchMode dispatchMode,
                                           String businessKey, String connector) {
        return stub.beginEffect(BeginEffectRequest.newBuilder()
                .setRunId(runId).setDecisionIndex(decisionIndex).setToolName(toolName)
                .setCallIndex(callIndex).setRequestJson(requestJson == null ? "" : requestJson)
                .setCustomKey(customKey == null ? "" : customKey)
                .setSemantics(semantics == null ? EffectSemantics.EFFECT_SEMANTICS_UNSPECIFIED : semantics)
                .setDispatchMode(dispatchMode == null ? EffectDispatchMode.EFFECT_DISPATCH_MODE_UNSPECIFIED : dispatchMode)
                .setBusinessKey(businessKey == null ? "" : businessKey)
                .setConnector(connector == null ? "" : connector).build());
    }

    public EffectRecord completeEffect(String runId, String key, EffectStatus status,
                                       String responseJson, String errorJson) {
        return stub.completeEffect(CompleteEffectRequest.newBuilder()
                .setRunId(runId).setIdempotencyKey(key).setStatus(status)
                .setResponseJson(responseJson == null ? "" : responseJson)
                .setErrorJson(errorJson == null ? "" : errorJson).build());
    }

    public GetEffectResponse getEffect(String runId, String key) {
        return stub.getEffect(GetEffectRequest.newBuilder().setRunId(runId).setIdempotencyKey(key).build());
    }

    public EffectRecord reconcileEffect(String runId, String key, EffectStatus resolved,
                                        String responseJson, String errorJson) {
        return stub.reconcileEffect(ReconcileEffectRequest.newBuilder()
                .setRunId(runId).setIdempotencyKey(key).setResolvedStatus(resolved)
                .setResponseJson(responseJson == null ? "" : responseJson)
                .setErrorJson(errorJson == null ? "" : errorJson).build());
    }

    // ── outbox dispatch (for non-idempotent upstreams) ──────────────────────

    /** PENDING+OUTBOX effects whose {@code next_dispatch_at_ms <= now} and
     *  whose lease is empty or expired. {@code connector} scopes the result. */
    public ListEffectsToDispatchResponse listEffectsToDispatch(String connector, long limit, long nowMs) {
        return stub.listEffectsToDispatch(ListEffectsToDispatchRequest.newBuilder()
                .setConnector(connector == null ? "" : connector)
                .setLimit(limit).setNowMs(nowMs).build());
    }

    /** Atomic CAS lease on the dispatch slot. Returns {@code acquired=false}
     *  (with the current row) when another dispatcher holds it — the loser
     *  must not call the upstream. */
    public ClaimEffectDispatchResponse claimEffectDispatch(String runId, String key, String claimer, long leaseTtlMs) {
        return stub.claimEffectDispatch(ClaimEffectDispatchRequest.newBuilder()
                .setRunId(runId).setIdempotencyKey(key)
                .setClaimer(claimer == null ? "" : claimer)
                .setLeaseTtlMs(leaseTtlMs).build());
    }

    /** Report a failed dispatch. {@code nextDispatchAtMs <= 0} drives the
     *  effect to UNKNOWN (the safety exit — no blind retry; the reconciler
     *  resolves via observe()); a positive value schedules a retry. */
    public EffectRecord recordDispatchAttempt(String runId, String key, String error, long nextDispatchAtMs) {
        return stub.recordDispatchAttempt(RecordDispatchAttemptRequest.newBuilder()
                .setRunId(runId).setIdempotencyKey(key)
                .setError(error == null ? "" : error)
                .setNextDispatchAtMs(nextDispatchAtMs).build());
    }

    /** Record what the counterparty said about an effect — the reconciler's
     *  write path. {@code DUPLICATE + compensateOnDuplicateKind} registers a
     *  compensation obligation atomically with the observation. */
    public EffectRecord recordExternalObservation(String runId, String key, EffectResolution resolution,
                                                   String externalRef, String responseJson, String errorJson,
                                                   String compensateOnDuplicateKind) {
        return stub.recordExternalObservation(RecordExternalObservationRequest.newBuilder()
                .setRunId(runId).setIdempotencyKey(key)
                .setResolution(resolution == null ? EffectResolution.EFFECT_RESOLUTION_UNSPECIFIED : resolution)
                .setExternalRef(externalRef == null ? "" : externalRef)
                .setResponseJson(responseJson == null ? "" : responseJson)
                .setErrorJson(errorJson == null ? "" : errorJson)
                .setCompensateOnDuplicateKind(compensateOnDuplicateKind == null ? "" : compensateOnDuplicateKind)
                .build());
    }

    // ── obligations ─────────────────────────────────────────────────────────
    //
    // The state machine:
    //   register_compensation  →  PENDING  (queued; eligible immediately)
    //   claim_obligation       →  COMMITTED with lease (CAS — one drainer wins)
    //   resolve_obligation     →  COMPENSATED | STUCK (terminal)
    //   record_obligation_attempt → PENDING with backoff, or STUCK if exhausted

    public ObligationRecord registerCompensation(String runId, String effectKey, String kind, String payloadJson) {
        return registerCompensation(runId, effectKey, kind, payloadJson, "", 0);
    }

    /** Extended form: `compensatorRef` ("module:attr") lets a generic drainer
     *  resolve the inverse without importing the agent. `maxAttempts==0` falls
     *  back to the server default (5). */
    public ObligationRecord registerCompensation(String runId, String effectKey, String kind, String payloadJson,
                                                  String compensatorRef, int maxAttempts) {
        return stub.registerCompensation(RegisterCompensationRequest.newBuilder()
                .setRunId(runId).setEffectKey(effectKey).setKind(kind)
                .setPayloadJson(payloadJson == null ? "" : payloadJson)
                .setCompensatorRef(compensatorRef == null ? "" : compensatorRef)
                .setMaxAttempts(maxAttempts).build());
    }

    public ListObligationsResponse listObligations(String runId, boolean onlyUnresolved) {
        return listObligations(runId, onlyUnresolved, ObligationStatus.OBLIGATION_STATUS_UNSPECIFIED);
    }

    /** Extended form: `statusFilter==UNSPECIFIED` (the default) means "any
     *  status"; otherwise it's an exact match. */
    public ListObligationsResponse listObligations(String runId, boolean onlyUnresolved, ObligationStatus statusFilter) {
        return stub.listObligations(ListObligationsRequest.newBuilder()
                .setRunId(runId).setOnlyUnresolved(onlyUnresolved)
                .setStatusFilter(statusFilter == null ? ObligationStatus.OBLIGATION_STATUS_UNSPECIFIED : statusFilter)
                .build());
    }

    public ObligationRecord resolveObligation(String runId, long obligationSeq, ObligationStatus status, String resultJson) {
        return stub.resolveObligation(ResolveObligationRequest.newBuilder()
                .setRunId(runId).setObligationSeq(obligationSeq).setStatus(status)
                .setResultJson(resultJson == null ? "" : resultJson).build());
    }

    /** Cross-run drainer feed. Defaults (include_pending=true,
     *  include_committed_expired=true) match the obligations reactor's hot set. */
    public ListUnresolvedObligationsResponse listUnresolvedObligations(int limit, long nowMs,
                                                                        boolean includePending,
                                                                        boolean includeStuck,
                                                                        boolean includeCommittedExpired) {
        return stub.listUnresolvedObligations(ListUnresolvedObligationsRequest.newBuilder()
                .setLimit(limit).setNowMs(nowMs)
                .setIncludePending(includePending).setIncludeStuck(includeStuck)
                .setIncludeCommittedExpired(includeCommittedExpired).build());
    }

    /** Atomic CAS lease. Returns acquired=false (with the current row) on
     *  contention. `leaseTtlMs==0` uses the server default (60s). */
    public ClaimObligationResponse claimObligation(String runId, long obligationSeq, String claimer, long leaseTtlMs) {
        return stub.claimObligation(ClaimObligationRequest.newBuilder()
                .setRunId(runId).setObligationSeq(obligationSeq)
                .setClaimer(claimer == null ? "" : claimer)
                .setLeaseTtlMs(leaseTtlMs).build());
    }

    /** Report a failed attempt. The server reschedules (PENDING + backoff) or
     *  marks STUCK (when retries are exhausted, or {@code nextAttemptAtMs <= 0}). */
    public ObligationRecord recordObligationAttempt(String runId, long obligationSeq, String error, long nextAttemptAtMs) {
        return stub.recordObligationAttempt(RecordObligationAttemptRequest.newBuilder()
                .setRunId(runId).setObligationSeq(obligationSeq)
                .setError(error == null ? "" : error)
                .setNextAttemptAtMs(nextAttemptAtMs).build());
    }

    // ── budget ──────────────────────────────────────────────────────────────

    public BudgetState setBudget(String runId, double usdCap, long tokenCap) {
        return stub.setBudget(SetBudgetRequest.newBuilder().setRunId(runId).setUsdCap(usdCap).setTokenCap(tokenCap).build());
    }

    public AdmitBudgetResponse admitBudget(String runId, double usd, long tokens) {
        return stub.admitBudget(AdmitBudgetRequest.newBuilder().setRunId(runId).setUsdEstimate(usd).setTokenEstimate(tokens).build());
    }

    public BudgetState chargeBudget(String runId, double usd, long tokens) {
        return stub.chargeBudget(ChargeBudgetRequest.newBuilder().setRunId(runId).setUsd(usd).setTokens(tokens).build());
    }

    // ── gates / signals ─────────────────────────────────────────────────────

    public AwaitSignalResponse awaitSignal(String runId, String gate, String payloadJson) {
        return stub.awaitSignal(AwaitSignalRequest.newBuilder().setRunId(runId).setGateName(gate)
                .setPayloadJson(payloadJson == null ? "" : payloadJson).build());
    }

    public SendSignalResponse sendSignal(String runId, String app, String user, String session,
                                          String gate, String resolutionJson) {
        return stub.sendSignal(SendSignalRequest.newBuilder()
                .setRunId(runId == null ? "" : runId).setAppName(app == null ? "" : app)
                .setUserId(user == null ? "" : user).setSessionId(session == null ? "" : session)
                .setGateName(gate).setResolutionJson(resolutionJson == null ? "" : resolutionJson).build());
    }

    // ── reconciliation / timers / WAL tail ──────────────────────────────────

    public ListPendingEffectsResponse listPendingEffects(long olderThanMs, boolean includePending,
                                                          boolean includeUnknown, long limit) {
        return stub.listPendingEffects(ListPendingEffectsRequest.newBuilder()
                .setOlderThanMs(olderThanMs).setIncludePending(includePending)
                .setIncludeUnknown(includeUnknown).setLimit(limit == 0 ? 200 : limit).build());
    }

    public TimerRecord setTimer(String runId, String timerId, long fireAtMs, String kind, String payloadJson) {
        return stub.setTimer(SetTimerRequest.newBuilder()
                .setRunId(runId).setTimerId(timerId == null ? "" : timerId).setFireAtMs(fireAtMs)
                .setKind(kind == null ? "" : kind).setPayloadJson(payloadJson == null ? "" : payloadJson).build());
    }

    public CancelTimerResponse cancelTimer(String runId, String timerId) {
        return stub.cancelTimer(CancelTimerRequest.newBuilder().setRunId(runId).setTimerId(timerId).build());
    }

    public ListDueTimersResponse listDueTimers(long nowMs, long limit, boolean claim) {
        return stub.listDueTimers(ListDueTimersRequest.newBuilder()
                .setNowMs(nowMs).setLimit(limit == 0 ? 200 : limit).setClaim(claim).build());
    }

    public Iterator<EventEntry> subscribeEvents(long fromTsMs, String runId, String kind) {
        return stub.subscribeEvents(SubscribeEventsRequest.newBuilder()
                .setFromTsMs(fromTsMs).setRunId(runId == null ? "" : runId)
                .setKind(kind == null ? "" : kind).build());
    }

    /**
     * Subject-routed, global-seq-cursored bus stream. Use {@code "/tape/<kind>/<verb>/.../**"}
     * with {@code *}/{@code **} wildcards. {@code predicateCel} may be empty (always-true).
     * {@code fromGlobalSeq} of 0 starts from the earliest available entry.
     */
    public Iterator<EventEntry> subscribeBySubject(String subjectPattern, String predicateCel, long fromGlobalSeq) {
        return stub.subscribeBySubject(SubscribeBySubjectRequest.newBuilder()
                .setSubjectPattern(subjectPattern == null ? "" : subjectPattern)
                .setPredicateCel(predicateCel == null ? "" : predicateCel)
                .setFromGlobalSeq(fromGlobalSeq).build());
    }

    // ── reactions & tasks (see design-principles/tape-event-bus.md) ─────────

    /** Register a server-side reaction. Returns the persisted {@link Reaction}. */
    public Reaction registerReaction(RegisterReactionOpts opts) {
        if (opts == null) throw new IllegalArgumentException("opts is required");
        if (opts.subjectPattern == null || opts.subjectPattern.isEmpty())
            throw new IllegalArgumentException("subjectPattern is required");
        HandlerKind kind = opts.handlerKind == null ? HandlerKind.HANDLER_KIND_TASK : opts.handlerKind;
        Reaction r = Reaction.newBuilder()
                .setReactionId(opts.reactionId == null ? "" : opts.reactionId)
                .setName(opts.name == null ? "" : opts.name)
                .setSubjectPattern(opts.subjectPattern)
                .setPredicateCel(opts.predicateCel == null ? "" : opts.predicateCel)
                .setHandlerKind(kind)
                .setAgentApp(opts.agentApp == null ? "" : opts.agentApp)
                .setPublishTarget(opts.publishTarget == null ? "" : opts.publishTarget)
                .setMaxConcurrency(opts.maxConcurrency)
                .setRateLimitPerS(opts.rateLimitPerS)
                .setDebounceMs(opts.debounceMs)
                .setRetryMax(opts.retryMax)
                .setRetryBackoffMs(opts.retryBackoffMs)
                .setDlqAfterN(opts.dlqAfterN)
                .setNumShards(opts.numShards)
                .setBootstrapFromHead(opts.bootstrapFromHead)
                .build();
        return stub.registerReaction(r);
    }

    public boolean deregisterReaction(String reactionId) {
        return stub.deregisterReaction(DeregisterReactionRequest.newBuilder()
                .setReactionId(reactionId == null ? "" : reactionId).build()).getDeregistered();
    }

    /** List reactions. Pass {@code null} or {@code ""} for all. */
    public List<Reaction> listReactions(String subjectPattern) {
        ListReactionsResponse resp = stub.listReactions(ListReactionsRequest.newBuilder()
                .setSubjectPattern(subjectPattern == null ? "" : subjectPattern).build());
        return resp.getReactionsList();
    }

    public List<Task> claimTasks(ClaimTasksOpts opts) {
        if (opts == null) throw new IllegalArgumentException("opts is required");
        if (opts.reactionId == null || opts.reactionId.isEmpty())
            throw new IllegalArgumentException("reactionId is required");
        if (opts.owner == null || opts.owner.isEmpty())
            throw new IllegalArgumentException("owner is required");
        ClaimTasksResponse resp = stub.claimTasks(ClaimTasksRequest.newBuilder()
                .setReactionId(opts.reactionId)
                .setShard(opts.shard)
                .setOwner(opts.owner)
                .setLeaseMs(opts.leaseMs)
                .setMax(opts.max)
                .setNowMs(opts.nowMs)
                .build());
        return resp.getTasksList();
    }

    public Task completeTask(String taskId, String owner) {
        return stub.completeTask(CompleteTaskRequest.newBuilder()
                .setTaskId(taskId == null ? "" : taskId)
                .setOwner(owner == null ? "" : owner).build()).getTask();
    }

    public Task nackTask(String taskId, String owner, String error, boolean permanent) {
        return stub.nackTask(NackTaskRequest.newBuilder()
                .setTaskId(taskId == null ? "" : taskId)
                .setOwner(owner == null ? "" : owner)
                .setError(error == null ? "" : error)
                .setPermanent(permanent).build()).getTask();
    }

    public List<Task> listTasks(String reactionId, TaskStatus status, int limit) {
        ListTasksResponse resp = stub.listTasks(ListTasksRequest.newBuilder()
                .setReactionId(reactionId == null ? "" : reactionId)
                .setStatus(status == null ? TaskStatus.TASK_STATUS_UNSPECIFIED : status)
                .setLimit(limit <= 0 ? 200 : limit).build());
        return resp.getTasksList();
    }

    // ── ADK SessionService shim ─────────────────────────────────────────────

    public Session createSession(String app, String user, String session, String stateJson) {
        return stub.createSession(CreateSessionRequest.newBuilder()
                .setAppName(app).setUserId(user).setSessionId(session == null ? "" : session)
                .setStateJson(stateJson == null ? "{}" : stateJson).build());
    }

    public GetSessionResponse getSession(String app, String user, String session, long maxEvents) {
        return stub.getSession(GetSessionRequest.newBuilder()
                .setAppName(app).setUserId(user).setSessionId(session).setMaxEvents(maxEvents).build());
    }

    public AppendEventResponse appendEvent(String app, String user, String session,
                                            EventRecord event, String stateDeltaJson) {
        return stub.appendEvent(AppendEventRequest.newBuilder()
                .setAppName(app).setUserId(user).setSessionId(session).setEvent(event)
                .setStateDeltaJson(stateDeltaJson == null ? "{}" : stateDeltaJson).build());
    }
}
