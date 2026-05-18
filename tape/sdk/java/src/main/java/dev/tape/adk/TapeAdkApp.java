package dev.tape.adk;

import dev.tape.DurableApp;
import dev.tape.TapeClient;

/**
 * The Java port of Python's {@code tape.adk.durable_app(...)} — a convenience
 * builder that returns a {@link TapePlugin} + {@link TapeSessionService} pair
 * sharing one {@link TapeClient}.
 *
 * <pre>{@code
 * try (TapeAdkApp app = TapeAdkApp.wire(new DurableApp.Config().name("treasury"))) {
 *   Runner runner = Runner.builder(...)
 *       .plugins(List.of(app.plugin()))
 *       .sessionService(app.sessionService())
 *       .build();
 * }
 * }</pre>
 */
public final class TapeAdkApp implements AutoCloseable {

    private final DurableApp app;
    private final TapePlugin plugin;
    private final TapeSessionService sessionService;

    private TapeAdkApp(DurableApp app, TapePlugin plugin, TapeSessionService sessionService) {
        this.app = app; this.plugin = plugin; this.sessionService = sessionService;
    }

    public DurableApp durableApp()                  { return app; }
    public TapeClient client()                      { return app.client(); }
    public TapePlugin plugin()                      { return plugin; }
    public TapeSessionService sessionService()      { return sessionService; }

    public static TapeAdkApp wire(DurableApp.Config cfg) {
        DurableApp app = DurableApp.wire(cfg);
        TapeClient client = app.client();
        return new TapeAdkApp(app, new TapePlugin(client), new TapeSessionService(client));
    }

    @Override
    public void close() {
        // DurableApp owns the client, so closing it cascades.
        app.close();
    }
}
