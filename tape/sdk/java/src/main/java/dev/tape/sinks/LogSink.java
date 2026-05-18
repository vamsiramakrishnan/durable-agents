package dev.tape.sinks;

import com.google.gson.Gson;
import dev.tape.proto.EventEntry;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Appends one JSON line per entry. {@code path == null} or {@code ":stderr"}
 * writes to stderr; {@code ":stdout"} to stdout; anything else is a file path
 * (created with parents).
 */
public final class LogSink implements Sink {

    private static final Gson GSON = new Gson();

    private final Writer writer;
    private final PrintStream stream;
    private final boolean ownsWriter;

    public LogSink(String path) throws IOException {
        if (path == null || path.isEmpty() || ":stderr".equals(path)) {
            this.writer = null; this.stream = System.err; this.ownsWriter = false;
        } else if (":stdout".equals(path)) {
            this.writer = null; this.stream = System.out; this.ownsWriter = false;
        } else {
            Path p = Path.of(path);
            if (p.getParent() != null) Files.createDirectories(p.getParent());
            this.writer = new BufferedWriter(Files.newBufferedWriter(p, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND));
            this.stream = null; this.ownsWriter = true;
        }
    }

    static String entryJson(EventEntry e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("run_id", e.getRunId());
        m.put("seq", e.getSeq());
        m.put("kind", e.getKind());
        m.put("payload_json", e.getPayloadJson());
        m.put("ts_ms", e.getTsMs());
        return GSON.toJson(m);
    }

    @Override public synchronized void publish(EventEntry e) throws IOException {
        String line = entryJson(e) + System.lineSeparator();
        if (writer != null) { writer.write(line); writer.flush(); }
        else { stream.print(line); stream.flush(); }
    }

    @Override public synchronized void close() throws IOException {
        if (ownsWriter && writer != null) writer.close();
    }
}
