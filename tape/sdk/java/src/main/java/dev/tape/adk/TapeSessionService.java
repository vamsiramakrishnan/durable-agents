package dev.tape.adk;

import com.google.adk.events.Event;
import com.google.adk.sessions.BaseSessionService;
import com.google.adk.sessions.GetSessionConfig;
import com.google.adk.sessions.ListEventsResponse;
import com.google.adk.sessions.ListSessionsResponse;
import com.google.adk.sessions.Session;
import com.google.gson.Gson;

import dev.tape.TapeClient;
import dev.tape.proto.AppendEventRequest;
import dev.tape.proto.EventRecord;
import dev.tape.proto.GetSessionResponse;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link BaseSessionService} backed by the Tape server.
 *
 * <p>Why a custom session service rather than just a plugin? Because this is
 * the seam that gives single-transaction atomicity: routing ADK's
 * {@code appendEvent} through Tape lets the server commit "the ADK event +
 * the session state delta + the tape projection (decision / effect /
 * obligation) this event corresponds to" together. The event stream and the
 * journal never disagree.
 *
 * <p>Event (de)serialization uses ADK's own JSON round-trip (the {@code Event}
 * is stored as JSON in the record's {@code content_json} field, so nothing is
 * lost). State deltas come from the event's {@code EventActions.stateDelta}
 * with the {@code temp:} prefix stripped (matching the Python adapter and
 * ADK's own {@code BaseSessionService.appendEvent}).
 */
public class TapeSessionService implements BaseSessionService {

    private static final Gson GSON = new Gson();

    private final TapeClient client;
    private final boolean ownsClient;

    public TapeSessionService(String url) {
        this(new TapeClient(url), true);
    }

    public TapeSessionService(TapeClient client) {
        this(client, false);
    }

    private TapeSessionService(TapeClient client, boolean ownsClient) {
        this.client = client;
        this.ownsClient = ownsClient;
    }

    public TapeClient client() { return client; }

    // ── createSession ────────────────────────────────────────────────────────

    @Override
    public Single<Session> createSession(String appName, String userId,
                                         ConcurrentMap<String, Object> state,
                                         String sessionId) {
        return Single.fromCallable(() -> {
            String stateJson = GSON.toJson(state == null ? Map.of() : state);
            dev.tape.proto.Session s = client.createSession(appName, userId,
                    sessionId == null ? "" : sessionId, stateJson);
            return toAdkSession(s);
        });
    }

    // ── getSession ──────────────────────────────────────────────────────────

    @Override
    public Maybe<Session> getSession(String appName, String userId, String sessionId,
                                      Optional<GetSessionConfig> config) {
        return Maybe.fromCallable(() -> {
            long maxEvents = 0;
            if (config.isPresent() && config.get().numRecentEvents().isPresent()) {
                maxEvents = config.get().numRecentEvents().get();
            }
            GetSessionResponse resp = client.getSession(appName, userId, sessionId, maxEvents);
            if (!resp.getFound()) return null;
            return toAdkSession(resp.getSession());
        });
    }

    // ── listSessions ────────────────────────────────────────────────────────

    @Override
    public Single<ListSessionsResponse> listSessions(String appName, String userId) {
        return Single.fromCallable(() -> {
            dev.tape.proto.ListSessionsResponse resp = client.listSessions(appName, userId);
            List<Session> sessions = new ArrayList<>(resp.getSessionsCount());
            for (dev.tape.proto.Session s : resp.getSessionsList()) {
                sessions.add(toAdkSession(s));
            }
            return ListSessionsResponse.builder().sessions(sessions).build();
        });
    }

    // ── deleteSession ───────────────────────────────────────────────────────

    @Override
    public Completable deleteSession(String appName, String userId, String sessionId) {
        return Completable.fromAction(() ->
            client.deleteSession(appName, userId, sessionId));
    }

    // ── listEvents ──────────────────────────────────────────────────────────

    @Override
    public Single<ListEventsResponse> listEvents(String appName, String userId, String sessionId) {
        return Single.fromCallable(() -> {
            // Tape's GetSession returns the events inline; for a paginated
            // ADK ListEvents we just re-use that and ignore page tokens.
            GetSessionResponse resp = client.getSession(appName, userId, sessionId, 0);
            List<Event> events = new ArrayList<>();
            if (resp.getFound()) {
                for (EventRecord rec : resp.getSession().getEventsList()) {
                    events.add(toAdkEvent(rec));
                }
            }
            return ListEventsResponse.builder().events(events).build();
        });
    }

    // ── appendEvent ─────────────────────────────────────────────────────────

    @Override
    public Single<Event> appendEvent(Session session, Event event) {
        // Honour the base contract: apply the event in-memory (state delta +
        // append). Then persist the durable shape — but only for non-partial
        // (committed) events. Partial streaming events stay in memory.
        return BaseSessionService.super.appendEvent(session, event)
                .flatMap(applied -> Single.fromCallable(() -> {
                    boolean partial = applied.partial().orElse(false);
                    if (partial) return applied;

                    Map<String, Object> stateDelta = filteredStateDelta(applied);
                    EventRecord record = toEventRecord(applied);
                    client.appendEvent(
                            session.appName(), session.userId(), session.id(),
                            record, GSON.toJson(stateDelta));
                    return applied;
                }));
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static Session toAdkSession(dev.tape.proto.Session s) {
        ConcurrentMap<String, Object> state = parseStateMap(s.getStateJson());
        List<Event> events = new ArrayList<>(s.getEventsCount());
        for (EventRecord rec : s.getEventsList()) {
            events.add(toAdkEvent(rec));
        }
        Instant t = Instant.ofEpochMilli(s.getLastUpdateTimeMs());
        return Session.builder(s.getSessionId())
                .appName(s.getAppName())
                .userId(s.getUserId())
                .state(state)
                .events(events)
                .lastUpdateTime(t)
                .build();
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentMap<String, Object> parseStateMap(String json) {
        ConcurrentMap<String, Object> m = new ConcurrentHashMap<>();
        if (json == null || json.isEmpty() || "{}".equals(json)) return m;
        try {
            Map<String, Object> parsed = GSON.fromJson(json, Map.class);
            if (parsed != null) {
                for (Map.Entry<String, Object> e : parsed.entrySet()) {
                    if (e.getValue() != null) m.put(e.getKey(), e.getValue());
                }
            }
        } catch (Exception ignore) { /* leave empty */ }
        return m;
    }

    private static Event toAdkEvent(EventRecord rec) {
        String json = rec.getContentJson();
        if (json == null || json.isEmpty()) {
            // Bootstrap a minimal JSON envelope and round-trip through
            // Event.fromJsonString — Event's constructor is package-private,
            // so JSON is the only stable public path.
            json = String.format(
                "{\"id\":\"%s\",\"invocationId\":\"%s\",\"author\":\"%s\",\"timestamp\":%d}",
                rec.getId(), rec.getInvocationId(), rec.getAuthor(), rec.getTimestampMs() / 1000);
        }
        try {
            return Event.fromJsonString(json, Event.class);
        } catch (Exception ex) {
            throw new RuntimeException(
                "TapeSessionService: failed to deserialize Event from content_json", ex);
        }
    }

    private static EventRecord toEventRecord(Event event) {
        return EventRecord.newBuilder()
                .setId(event.id() == null ? "" : event.id())
                .setInvocationId(event.invocationId() == null ? "" : event.invocationId())
                .setAuthor(event.author() == null ? "" : event.author())
                .setBranch(event.branch().orElse(""))
                .setContentJson(event.toJson())
                .setActionsJson("")
                .setTimestampMs(event.timestamp() * 1000)
                .build();
    }

    private static Map<String, Object> filteredStateDelta(Event event) {
        Map<String, Object> out = new HashMap<>();
        if (event.actions() == null) return out;
        Map<String, Object> raw = event.actions().stateDelta();
        if (raw == null) return out;
        for (Map.Entry<String, Object> e : raw.entrySet()) {
            String k = e.getKey();
            if (k != null && !k.startsWith("temp:")) {
                out.put(k, e.getValue());
            }
        }
        return out;
    }
}
