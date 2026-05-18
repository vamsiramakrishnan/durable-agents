package dev.tape.sinks;

import dev.tape.proto.EventEntry;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Publish each entry to Google Cloud Pub/Sub. The Pub/Sub client jar is a
 * runtime-optional dependency — this sink loads it reflectively. If the
 * {@code com.google.cloud:google-cloud-pubsub} jar is on the classpath, the
 * sink works; otherwise the constructor throws a clear "missing dependency"
 * error.
 *
 * <p>{@code orderingKey = run_id} so subscribers can preserve per-run order.
 * The {@code tape-event-id = run_id/seq} attribute is what consumers dedup on
 * (Pub/Sub assigns its own message_id).
 */
public final class PubSubSink implements Sink {

    /** Configuration. */
    public static final class Opts {
        public String project;
        public String topic;
        public Opts project(String v) { this.project = v; return this; }
        public Opts topic(String v) { this.topic = v; return this; }
    }

    private final Opts opts;
    private final Object publisher; // com.google.cloud.pubsub.v1.Publisher
    private final Method publishMethod;
    private final Class<?> pubsubMessageClass;
    private final Class<?> byteStringClass;

    public PubSubSink(Opts opts) {
        if (opts == null || opts.project == null || opts.project.isEmpty()
                || opts.topic == null || opts.topic.isEmpty()) {
            throw new IllegalArgumentException("PubSubSink: project and topic required");
        }
        this.opts = opts;
        try {
            Class<?> topicNameClass = Class.forName("com.google.pubsub.v1.TopicName");
            Class<?> publisherClass = Class.forName("com.google.cloud.pubsub.v1.Publisher");
            Object topicName = topicNameClass.getMethod("of", String.class, String.class)
                    .invoke(null, opts.project, opts.topic);
            Object builder = publisherClass.getMethod("newBuilder", topicNameClass).invoke(null, topicName);
            builder = builder.getClass().getMethod("setEnableMessageOrdering", boolean.class)
                    .invoke(builder, true);
            this.publisher = builder.getClass().getMethod("build").invoke(builder);
            this.pubsubMessageClass = Class.forName("com.google.pubsub.v1.PubsubMessage");
            this.publishMethod = publisherClass.getMethod("publish", pubsubMessageClass);
            this.byteStringClass = Class.forName("com.google.protobuf.ByteString");
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(
                "PubSubSink requires com.google.cloud:google-cloud-pubsub on the classpath", ex);
        } catch (ReflectiveOperationException ex) {
            throw new RuntimeException("PubSubSink: cannot construct Publisher", ex);
        }
    }

    @Override public void publish(EventEntry e) throws Exception {
        String body = LogSink.entryJson(e);
        Object data = byteStringClass.getMethod("copyFrom", byte[].class)
                .invoke(null, (Object) body.getBytes(StandardCharsets.UTF_8));

        Object msgBuilder = pubsubMessageClass.getMethod("newBuilder").invoke(null);
        msgBuilder = msgBuilder.getClass().getMethod("setData", byteStringClass).invoke(msgBuilder, data);
        msgBuilder = msgBuilder.getClass().getMethod("setOrderingKey", String.class)
                .invoke(msgBuilder, e.getRunId());

        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("tape-event-id", e.getRunId() + "/" + e.getSeq());
        attrs.put("kind", e.getKind());
        msgBuilder = msgBuilder.getClass().getMethod("putAllAttributes", Map.class).invoke(msgBuilder, attrs);

        Object msg = msgBuilder.getClass().getMethod("build").invoke(msgBuilder);
        Object future = publishMethod.invoke(publisher, msg);
        // ApiFuture<String>.get() blocks until publish completes.
        future.getClass().getMethod("get").invoke(future);
    }

    @Override public void close() throws Exception {
        publisher.getClass().getMethod("shutdown").invoke(publisher);
    }
}
