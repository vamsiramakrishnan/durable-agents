package dev.tape;

import java.util.Objects;

/**
 * The wiring entrypoint for an ADK-style agent on Tape — Java port of
 * Python's {@code tape.adk.durable_app(...)}.
 *
 * <p>The Java port of ADK isn't shipped yet (the protocol is the contract;
 * the adapter is mechanical), so {@link #wire(Config)} returns the wiring
 * values an adapter needs: the resolved Tape URL, the connected
 * {@link TapeClient}, the lease-owner string, and the chosen {@link Budget}.
 * When a Java ADK port lands, its {@code Runner} constructor will accept a
 * {@code DurableApp} directly.
 *
 * <pre>{@code
 * try (DurableApp app = DurableApp.wire(new DurableApp.Config()
 *         .name("treasury")
 *         .budget(new DurableApp.Budget(50.0, 2_000_000)))) {
 *   var run = app.client().beginRun(...);
 *   // ...
 * }
 * }</pre>
 */
public final class DurableApp implements AutoCloseable {

    public static final class Budget {
        public final double usdCap;
        public final long   tokenCap;
        public Budget()                          { this(0.0, 0); }
        public Budget(double usdCap, long token) { this.usdCap = usdCap; this.tokenCap = token; }
    }

    public static final class Config {
        public String   name;
        public String   tapeUrl;
        public Budget   budget;
        public boolean  resumable = true;
        public boolean  checkCancellation = true;
        public String   leaseOwner;
        public long     leaseTtlMs;
        public TapeClient.Options clientOptions;

        public Config name(String v)             { this.name = v; return this; }
        public Config tapeUrl(String v)          { this.tapeUrl = v; return this; }
        public Config budget(Budget v)           { this.budget = v; return this; }
        public Config resumable(boolean v)       { this.resumable = v; return this; }
        public Config checkCancellation(boolean v){ this.checkCancellation = v; return this; }
        public Config leaseOwner(String v)       { this.leaseOwner = v; return this; }
        public Config leaseTtlMs(long v)         { this.leaseTtlMs = v; return this; }
        public Config clientOptions(TapeClient.Options v) { this.clientOptions = v; return this; }
    }

    private final Config config;
    private final String url;
    private final TapeClient client;
    private final String leaseOwner;
    private final long   leaseTtlMs;

    private DurableApp(Config c, String url, TapeClient client, String owner, long ttl) {
        this.config = c; this.url = url; this.client = client;
        this.leaseOwner = owner; this.leaseTtlMs = ttl;
    }

    public Config config()         { return config; }
    public String url()            { return url; }
    public TapeClient client()     { return client; }
    public String leaseOwner()     { return leaseOwner; }
    public long   leaseTtlMs()     { return leaseTtlMs; }
    public Budget budget()         { return config.budget == null ? new Budget() : config.budget; }
    public String name()           { return config.name; }
    public boolean resumable()     { return config.resumable; }
    public boolean checkCancellation() { return config.checkCancellation; }

    /** Wire a Tape-backed ADK app. The returned bundle owns the client. */
    public static DurableApp wire(Config cfg) {
        Objects.requireNonNull(cfg, "cfg");
        if (cfg.name == null || cfg.name.isEmpty()) {
            throw new IllegalArgumentException("DurableApp.wire: name is required");
        }
        String url = cfg.tapeUrl;
        if (url == null || url.isEmpty()) {
            url = TapeClient.defaultUrl();
        }
        TapeClient c = cfg.clientOptions != null
                ? new TapeClient(url, cfg.clientOptions)
                : new TapeClient(url);
        String owner = (cfg.leaseOwner == null || cfg.leaseOwner.isEmpty())
                ? defaultLeaseOwner()
                : cfg.leaseOwner;
        long ttl = cfg.leaseTtlMs > 0 ? cfg.leaseTtlMs : defaultLeaseTtlMs();
        return new DurableApp(cfg, url, c, owner, ttl);
    }

    @Override public void close() { client.close(); }

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
}
