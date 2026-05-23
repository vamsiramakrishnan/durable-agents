package dev.tape;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Identity &amp; authorization context attached to a Tape run.
 *
 * <p>Populated either explicitly when calling {@link TapeClient#beginRun} or
 * implicitly by {@link #fromEnv()}, which reads the conventional
 * {@code AIPLEX_*} environment variables an AIPlex-deployed agent receives
 * at startup. See the Python reference in
 * {@code tape/sdk/python/tape/adk/identity.py}.
 *
 * <p>Convention, not contract: the {@code AIPLEX_*} env-var prefix is
 * informational. Tape itself does not depend on AIPlex; any deployer can
 * set the same vars to thread identity through Tape.
 */
public final class RunIdentity {
    public final String tenantId;
    public final String actor;
    public final String subject;
    public final String agentId;
    public final String aiplexInstanceId;
    public final String gatewayRoute;
    public final List<String> scopes;
    public final Map<String, String> labels;

    public static final RunIdentity EMPTY = new RunIdentity(
            "", "", "", "", "", "",
            Collections.emptyList(), Collections.emptyMap());

    public RunIdentity(String tenantId, String actor, String subject, String agentId,
                       String aiplexInstanceId, String gatewayRoute,
                       List<String> scopes, Map<String, String> labels) {
        this.tenantId = nz(tenantId);
        this.actor = nz(actor);
        this.subject = nz(subject);
        this.agentId = nz(agentId);
        this.aiplexInstanceId = nz(aiplexInstanceId);
        this.gatewayRoute = nz(gatewayRoute);
        this.scopes = scopes == null ? Collections.emptyList() : scopes;
        this.labels = labels == null ? Collections.emptyMap() : labels;
    }

    public static RunIdentity fromEnv() {
        return fromEnv(System.getenv());
    }

    public static RunIdentity fromEnv(Map<String, String> env) {
        return new RunIdentity(
                env.getOrDefault("AIPLEX_TENANT_ID", ""),
                env.getOrDefault("AIPLEX_ACTOR", ""),
                env.getOrDefault("AIPLEX_SUBJECT", ""),
                env.getOrDefault("AIPLEX_AGENT_ID", ""),
                env.getOrDefault("AIPLEX_INSTANCE_ID", ""),
                env.getOrDefault("AIPLEX_ROUTE", ""),
                parseScopes(env.getOrDefault("AIPLEX_SCOPES", "")),
                parseLabels(env.getOrDefault("AIPLEX_LABELS", "")));
    }

    private static String nz(String s) { return s == null ? "" : s; }

    private static List<String> parseScopes(String s) {
        if (s == null || s.isEmpty()) return Collections.emptyList();
        String[] tokens = s.replace(',', ' ').split("\\s+");
        java.util.ArrayList<String> out = new java.util.ArrayList<>();
        for (String t : tokens) if (!t.isEmpty()) out.add(t);
        return out;
    }

    private static Map<String, String> parseLabels(String s) {
        if (s == null || s.isEmpty()) return Collections.emptyMap();
        java.util.HashMap<String, String> out = new java.util.HashMap<>();
        for (String pair : s.split(",")) {
            pair = pair.trim();
            int eq = pair.indexOf('=');
            if (eq <= 0) continue;
            String k = pair.substring(0, eq).trim();
            if (!k.isEmpty()) out.put(k, pair.substring(eq + 1).trim());
        }
        return out;
    }
}
