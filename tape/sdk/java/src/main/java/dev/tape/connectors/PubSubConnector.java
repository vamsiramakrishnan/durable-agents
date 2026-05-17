package dev.tape.connectors;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Publish the intent as a Google Cloud Pub/Sub message. The
 * {@code google-cloud-pubsub} library is an optional dependency —
 * accessed via reflection so the core jar stays light. When the class is
 * absent on the classpath, all three methods return an UNKNOWN /
 * STUCK outcome carrying a clear error.
 *
 * <p>To enable: add to your pom.xml:
 * <pre>{@code
 *   <dependency>
 *     <groupId>com.google.cloud</groupId>
 *     <artifactId>google-cloud-pubsub</artifactId>
 *   </dependency>
 * }</pre>
 */
public final class PubSubConnector implements Connector {

    public static final class Opts {
        public String name;
        public String project;
        public String topic;
        public String compensateTopic;
        public String tapeUrl;

        public Opts name(String v)            { this.name = v; return this; }
        public Opts project(String v)         { this.project = v; return this; }
        public Opts topic(String v)           { this.topic = v; return this; }
        public Opts compensateTopic(String v) { this.compensateTopic = v; return this; }
        public Opts tapeUrl(String v)         { this.tapeUrl = v; return this; }
    }

    private final Opts opts;

    public PubSubConnector(Opts opts) {
        if (opts == null || opts.project == null || opts.topic == null)
            throw new IllegalArgumentException("PubSubConnector: project + topic required");
        if (opts.name == null) opts.name = "pubsub:" + opts.topic;
        this.opts = opts;
    }

    @Override public String name() { return opts.name; }

    @Override public Result dispatch(Effect e) {
        try {
            String id = publish(opts.topic, e.payload, e.runId, headersForEffect(e));
            return new Result(DispatchOutcome.CONFIRMED)
                .response(Map.of("message_id", id))
                .dispatchId(id);
        } catch (ClassNotFoundException cnf) {
            return new Result(DispatchOutcome.UNKNOWN).error(MISSING_DEP);
        } catch (Exception ex) {
            return new Result(DispatchOutcome.UNKNOWN).error(ex.getMessage());
        }
    }

    @Override public Observation observe(Effect e) {
        return new Observation(ObservationOutcome.UNKNOWN)
            .error("PubSubConnector.observe: chain HttpConnector.observeUrl or subclass.");
    }

    @Override public Compensation compensate(Obligation o) {
        if (opts.compensateTopic == null || opts.compensateTopic.isEmpty()) {
            return new Compensation(CompensationOutcome.STUCK).error("no compensateTopic configured");
        }
        try {
            Map<String, String> attrs = new HashMap<>();
            attrs.put("tape_obligation_kind", o.kind);
            attrs.put("tape_effect_key", o.effectKey);
            attrs.put("tape_run_id", o.runId);
            String id = publish(opts.compensateTopic, o.payload, o.runId, attrs);
            return new Compensation(CompensationOutcome.COMPENSATED).response(Map.of("message_id", id));
        } catch (ClassNotFoundException cnf) {
            return new Compensation(CompensationOutcome.STUCK).error(MISSING_DEP);
        } catch (Exception ex) {
            return new Compensation(CompensationOutcome.PENDING).error(ex.getMessage());
        }
    }

    static final String MISSING_DEP =
        "google-cloud-pubsub is not on the classpath. Add it to your pom.xml " +
        "(groupId=com.google.cloud, artifactId=google-cloud-pubsub).";

    private Map<String, String> headersForEffect(Effect e) {
        Map<String, String> a = new HashMap<>();
        a.put("tape_idempotency_key", e.idempotencyKey == null ? "" : e.idempotencyKey);
        a.put("tape_run_id", e.runId == null ? "" : e.runId);
        a.put("tape_business_key", e.businessKey == null ? "" : e.businessKey);
        a.put("tape_tool", e.toolName == null ? "" : e.toolName);
        a.put("tape_attempt", String.valueOf(e.attempt));
        return a;
    }

    /**
     * Reflective publish — keeps the core jar free of the google-cloud-pubsub
     * dependency. The call shape:
     *   Publisher pub = Publisher.newBuilder(TopicName.of(project, topic)).setEnableMessageOrdering(true).build();
     *   PubsubMessage msg = PubsubMessage.newBuilder().setData(...).setOrderingKey(orderingKey).putAllAttributes(attrs).build();
     *   String id = pub.publish(msg).get();
     *   pub.shutdown();
     *   pub.awaitTermination(30, SECONDS);
     */
    private String publish(String topic, Object payload, String orderingKey, Map<String, String> attrs) throws Exception {
        Class<?> publisher = Class.forName("com.google.cloud.pubsub.v1.Publisher");
        Class<?> topicName = Class.forName("com.google.pubsub.v1.TopicName");
        Class<?> pubsubMsg = Class.forName("com.google.pubsub.v1.PubsubMessage");
        Class<?> byteString = Class.forName("com.google.protobuf.ByteString");

        Object name = topicName.getMethod("of", String.class, String.class).invoke(null, opts.project, topic);
        Object builder = publisher.getMethod("newBuilder", topicName).invoke(null, name);
        builder = builder.getClass().getMethod("setEnableMessageOrdering", boolean.class).invoke(builder, true);
        Object pub = builder.getClass().getMethod("build").invoke(builder);
        try {
            Object data = byteString.getMethod("copyFromUtf8", String.class)
                .invoke(null, LogConnector.toJson(payload));
            Object mb = pubsubMsg.getMethod("newBuilder").invoke(null);
            mb = mb.getClass().getMethod("setData", byteString).invoke(mb, data);
            mb = mb.getClass().getMethod("setOrderingKey", String.class).invoke(mb, orderingKey == null ? "" : orderingKey);
            mb = mb.getClass().getMethod("putAllAttributes", Map.class).invoke(mb, attrs);
            Object msg = mb.getClass().getMethod("build").invoke(mb);
            Object future = pub.getClass().getMethod("publish", pubsubMsg).invoke(pub, msg);
            Method get = future.getClass().getMethod("get");
            return (String) get.invoke(future);
        } finally {
            try {
                pub.getClass().getMethod("shutdown").invoke(pub);
                pub.getClass().getMethod("awaitTermination", long.class, java.util.concurrent.TimeUnit.class)
                    .invoke(pub, 30L, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }
    }
}
