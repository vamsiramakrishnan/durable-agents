package dev.tape.connectors;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Enqueue an HTTP target on Google Cloud Tasks. Cloud Tasks owns retries,
 * backoff, and scheduling; this connector just creates the task. The
 * {@code google-cloud-tasks} library is an optional dependency, loaded
 * reflectively. When absent, methods return UNKNOWN / STUCK with a clear
 * error message.
 *
 * <p>To enable, add to your pom.xml:
 * <pre>{@code
 *   <dependency>
 *     <groupId>com.google.cloud</groupId>
 *     <artifactId>google-cloud-tasks</artifactId>
 *   </dependency>
 * }</pre>
 */
public final class CloudTasksConnector implements Connector {

    public static final class Opts {
        public String name;
        public String project;
        public String location;
        public String queue;
        public String targetUrl;
        public String serviceAccountEmail;
        public String observeUrl;
        public String compensateUrl;

        public Opts name(String v)                { this.name = v; return this; }
        public Opts project(String v)             { this.project = v; return this; }
        public Opts location(String v)            { this.location = v; return this; }
        public Opts queue(String v)               { this.queue = v; return this; }
        public Opts targetUrl(String v)           { this.targetUrl = v; return this; }
        public Opts serviceAccountEmail(String v) { this.serviceAccountEmail = v; return this; }
        public Opts observeUrl(String v)          { this.observeUrl = v; return this; }
        public Opts compensateUrl(String v)       { this.compensateUrl = v; return this; }
    }

    private final Opts opts;

    public CloudTasksConnector(Opts opts) {
        if (opts == null || opts.project == null || opts.location == null
                || opts.queue == null || opts.targetUrl == null) {
            throw new IllegalArgumentException(
                "CloudTasksConnector: project / location / queue / targetUrl are required");
        }
        if (opts.name == null) opts.name = "tasks:" + opts.queue;
        this.opts = opts;
    }

    @Override public String name() { return opts.name; }

    static final Pattern TASK_SAFE = Pattern.compile("[^a-zA-Z0-9\\-_]");
    static String safeTaskId(String key) {
        String s = TASK_SAFE.matcher(key == null ? "" : key).replaceAll("-");
        return s.length() > 500 ? s.substring(0, 500) : s;
    }

    String queuePath() {
        return "projects/" + opts.project + "/locations/" + opts.location + "/queues/" + opts.queue;
    }

    @Override public Result dispatch(Effect e) {
        try {
            Class<?> ctc = Class.forName("com.google.cloud.tasks.v2.CloudTasksClient");
            Class<?> taskCls = Class.forName("com.google.cloud.tasks.v2.Task");
            Class<?> httpReq = Class.forName("com.google.cloud.tasks.v2.HttpRequest");
            Class<?> methodEnum = Class.forName("com.google.cloud.tasks.v2.HttpMethod");
            Class<?> byteString = Class.forName("com.google.protobuf.ByteString");

            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", "application/json");
            headers.put("X-Tape-Idempotency-Key", e.idempotencyKey == null ? "" : e.idempotencyKey);
            headers.put("X-Tape-Run-Id", e.runId == null ? "" : e.runId);
            headers.put("X-Tape-Business-Key", e.businessKey == null ? "" : e.businessKey);

            Object httpReqBuilder = httpReq.getMethod("newBuilder").invoke(null);
            Object postEnum = methodEnum.getMethod("valueOf", String.class).invoke(null, "POST");
            httpReqBuilder = httpReqBuilder.getClass().getMethod("setHttpMethod", methodEnum).invoke(httpReqBuilder, postEnum);
            httpReqBuilder = httpReqBuilder.getClass().getMethod("setUrl", String.class).invoke(httpReqBuilder, opts.targetUrl);
            httpReqBuilder = httpReqBuilder.getClass().getMethod("putAllHeaders", Map.class).invoke(httpReqBuilder, headers);
            Object bodyBytes = byteString.getMethod("copyFromUtf8", String.class)
                .invoke(null, LogConnector.toJson(e.payload));
            httpReqBuilder = httpReqBuilder.getClass().getMethod("setBody", byteString).invoke(httpReqBuilder, bodyBytes);
            if (opts.serviceAccountEmail != null && !opts.serviceAccountEmail.isEmpty()) {
                Class<?> oidcCls = Class.forName("com.google.cloud.tasks.v2.OidcToken");
                Object oidcBuilder = oidcCls.getMethod("newBuilder").invoke(null);
                oidcBuilder = oidcBuilder.getClass().getMethod("setServiceAccountEmail", String.class)
                    .invoke(oidcBuilder, opts.serviceAccountEmail);
                oidcBuilder = oidcBuilder.getClass().getMethod("setAudience", String.class)
                    .invoke(oidcBuilder, opts.targetUrl);
                Object oidc = oidcBuilder.getClass().getMethod("build").invoke(oidcBuilder);
                httpReqBuilder = httpReqBuilder.getClass().getMethod("setOidcToken", oidcCls).invoke(httpReqBuilder, oidc);
            }
            Object req = httpReqBuilder.getClass().getMethod("build").invoke(httpReqBuilder);

            Object taskBuilder = taskCls.getMethod("newBuilder").invoke(null);
            taskBuilder = taskBuilder.getClass().getMethod("setName", String.class)
                .invoke(taskBuilder, queuePath() + "/tasks/" + safeTaskId(e.idempotencyKey));
            taskBuilder = taskBuilder.getClass().getMethod("setHttpRequest", httpReq).invoke(taskBuilder, req);
            Object task = taskBuilder.getClass().getMethod("build").invoke(taskBuilder);

            Object client = ctc.getMethod("create").invoke(null);
            try {
                Class<?> queueName = Class.forName("com.google.cloud.tasks.v2.QueueName");
                Object parent = queueName.getMethod("of", String.class, String.class, String.class)
                    .invoke(null, opts.project, opts.location, opts.queue);
                Object created = client.getClass()
                    .getMethod("createTask", queueName, taskCls).invoke(client, parent, task);
                String createdName = (String) created.getClass().getMethod("getName").invoke(created);
                return new Result(DispatchOutcome.PENDING)
                    .response(Map.of("name", createdName)).dispatchId(createdName);
            } finally {
                client.getClass().getMethod("close").invoke(client);
            }
        } catch (ClassNotFoundException cnf) {
            return new Result(DispatchOutcome.UNKNOWN).error(MISSING_DEP);
        } catch (Exception ex) {
            String m = ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage();
            if (m != null && (m.contains("ALREADY_EXISTS") || m.contains("AlreadyExists"))) {
                return new Result(DispatchOutcome.CONFIRMED).response(Map.of("deduped", true));
            }
            return new Result(DispatchOutcome.UNKNOWN).error(String.valueOf(m));
        }
    }

    @Override public Observation observe(Effect e) {
        if (opts.observeUrl == null || opts.observeUrl.isEmpty()) {
            return new Observation(ObservationOutcome.UNKNOWN).error("no observeUrl configured");
        }
        return new HttpConnector(new HttpConnector.Opts()
            .url(opts.observeUrl).observeUrl(opts.observeUrl)).observe(e);
    }

    @Override public Compensation compensate(Obligation o) {
        if (opts.compensateUrl == null || opts.compensateUrl.isEmpty()) {
            return new Compensation(CompensationOutcome.STUCK).error("no compensateUrl configured");
        }
        return new HttpConnector(new HttpConnector.Opts()
            .url(opts.compensateUrl).compensateUrl(opts.compensateUrl)).compensate(o);
    }

    static final String MISSING_DEP =
        "google-cloud-tasks is not on the classpath. Add it to your pom.xml " +
        "(groupId=com.google.cloud, artifactId=google-cloud-tasks).";
}
