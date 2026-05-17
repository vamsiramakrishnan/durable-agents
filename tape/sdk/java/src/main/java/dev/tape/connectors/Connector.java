package dev.tape.connectors;

import java.util.Map;

/**
 * Capability connector — the thing the outbox reactor calls to actually
 * perform a side effect. Three operations, all idempotent on
 * {@code (runId, idempotencyKey)}.
 */
public interface Connector {

    String name();

    Result dispatch(Effect effect) throws Exception;

    Observation observe(Effect effect) throws Exception;

    Compensation compensate(Obligation obligation) throws Exception;

    enum DispatchOutcome { CONFIRMED, PENDING, UNKNOWN, FAILED }
    enum ObservationOutcome { CONFIRMED, ABSENT, DUPLICATE, STUCK, UNKNOWN }
    enum CompensationOutcome { COMPENSATED, PENDING, STUCK, FAILED }

    /** The intent the outbox reactor wants dispatched. */
    final class Effect {
        public String runId;
        public String idempotencyKey;
        public String toolName;
        public String connector;
        public Map<String, Object> payload;
        public String businessKey = "";
        public int    attempt = 1;
        public String semantics = "idempotent";
        public String tenantId = "";
        public String appName = "";

        public Effect runId(String v) { this.runId = v; return this; }
        public Effect idempotencyKey(String v) { this.idempotencyKey = v; return this; }
        public Effect toolName(String v) { this.toolName = v; return this; }
        public Effect connector(String v) { this.connector = v; return this; }
        public Effect payload(Map<String, Object> v) { this.payload = v; return this; }
        public Effect businessKey(String v) { this.businessKey = v == null ? "" : v; return this; }
        public Effect attempt(int v) { this.attempt = v; return this; }
        public Effect semantics(String v) { this.semantics = v; return this; }
    }

    final class Obligation {
        public String runId;
        public String effectKey;
        public String kind;
        public Map<String, Object> payload;
        public int    attempt = 1;
        public String compensatorRef = "";
    }

    final class Result {
        public DispatchOutcome outcome;
        public Object  response;
        public String  error = "";
        public String  dispatchId = "";
        public int     retryAfterMs = 0;
        public Result(DispatchOutcome o) { this.outcome = o; }
        public Result response(Object r) { this.response = r; return this; }
        public Result error(String e)    { this.error = e == null ? "" : e; return this; }
        public Result dispatchId(String id) { this.dispatchId = id == null ? "" : id; return this; }
    }

    final class Observation {
        public ObservationOutcome outcome;
        public Object response;
        public String error = "";
        public int    count = 0;
        public Observation(ObservationOutcome o) { this.outcome = o; }
        public Observation response(Object r) { this.response = r; return this; }
        public Observation error(String e)    { this.error = e == null ? "" : e; return this; }
        public Observation count(int n)       { this.count = n; return this; }
    }

    final class Compensation {
        public CompensationOutcome outcome;
        public Object response;
        public String error = "";
        public Compensation(CompensationOutcome o) { this.outcome = o; }
        public Compensation response(Object r) { this.response = r; return this; }
        public Compensation error(String e)    { this.error = e == null ? "" : e; return this; }
    }
}
