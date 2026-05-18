package dev.tape.cli;

import com.google.gson.Gson;
import dev.tape.TapeClient;
import dev.tape.connectors.ConnectorRegistry;
import dev.tape.connectors.LogConnector;
import dev.tape.reactors.OutboxReactor;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * tape-outbox — Java counterpart of {@code python -m tape.reactors.outbox}.
 *
 * <p>Connectors must be registered before the loop starts. The simplest way
 * is to spawn this from your agent process after registering your own
 * connectors; the {@code --register-log-connector} flag is provided for
 * tests and demos. Run with:
 *
 * <pre>{@code
 * mvn -pl tape/sdk/java exec:java \
 *     -Dexec.mainClass=dev.tape.cli.TapeOutbox \
 *     -Dexec.args="--url tape://localhost:7878 --register-log-connector --once"
 * }</pre>
 */
public final class TapeOutbox {

    private TapeOutbox() {}

    public static void main(String[] args) throws Exception {
        String  url            = "tape://localhost:7878";
        String  connector      = "";
        long    intervalMs     = 1000;
        int     maxAttempts    = 5;
        String  claimer        = "";
        boolean once           = false;
        boolean registerLog    = false;
        String  logPath        = "";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--url":             url = args[++i]; break;
                case "--connector":       connector = args[++i]; break;
                case "--interval":        intervalMs = Long.parseLong(args[++i]); break;
                case "--max-attempts":    maxAttempts = Integer.parseInt(args[++i]); break;
                case "--claimer":         claimer = args[++i]; break;
                case "--once":            once = true; break;
                case "--register-log-connector":   registerLog = true; break;
                case "--log-connector-path":       logPath = args[++i]; break;
                case "-h": case "--help": System.out.println(help()); return;
                default:
                    System.err.println("unknown flag: " + args[i]);
                    System.out.println(help());
                    System.exit(2);
            }
        }

        if (registerLog) {
            String p = logPath == null || logPath.isEmpty() ? "/tmp/tape-outbox.jsonl" : logPath;
            ConnectorRegistry.DEFAULT.replace("log", new LogConnector(p));
        }

        TapeClient client = new TapeClient(url);
        OutboxReactor.RunOptions opt = new OutboxReactor.RunOptions();
        opt.outbox.connector(connector).claimer(claimer).dispatchMaxAttempts(maxAttempts);
        opt.intervalMs = intervalMs;
        opt.once = once;
        Gson gson = new Gson();
        opt.onTick = outs -> {
            if (outs == null || outs.isEmpty()) return;
            Map<String, Object> wire = new LinkedHashMap<>();
            wire.put("outbox", outs.stream().map(OutboxReactor.Outcome::toMap).toList());
            System.out.println(gson.toJson(wire));
        };

        AtomicBoolean keepGoing = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> keepGoing.set(false)));
        try {
            OutboxReactor.run(client, opt, keepGoing);
        } finally {
            client.close();
        }
    }

    private static String help() {
        return String.join("\n",
            "tape-outbox — run Tape's outbox dispatcher (Java port).",
            "",
            "Flags:",
            "  --url URL                       Tape server URL (default: tape://localhost:7878)",
            "  --connector NAME                Restrict to one connector name",
            "  --interval MS                   Poll interval (default: 1000)",
            "  --max-attempts N                Mark FAILED after N attempts (default: 5)",
            "  --claimer ID                    Identity recorded as dispatch_claimed_by",
            "  --once                          Run one pass and exit",
            "  --register-log-connector        Register the built-in LogConnector under 'log'",
            "  --log-connector-path PATH       Path the LogConnector writes to",
            "  -h, --help                      This help"
        );
    }
}
