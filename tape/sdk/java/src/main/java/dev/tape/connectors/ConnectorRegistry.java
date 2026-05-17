package dev.tape.connectors;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Process-local registry of {@link Connector}s keyed by name. The static
 * {@link #DEFAULT} singleton is what most projects use; tests construct a
 * fresh {@link ConnectorRegistry} to avoid global state.
 */
public final class ConnectorRegistry {

    public static final ConnectorRegistry DEFAULT = new ConnectorRegistry();

    private final ConcurrentMap<String, Connector> items = new ConcurrentHashMap<>();

    public void register(String name, Connector c) {
        Connector prior = items.putIfAbsent(name, c);
        if (prior != null) {
            throw new IllegalStateException("connector " + name + " already registered");
        }
    }

    public void replace(String name, Connector c) { items.put(name, c); }

    public Connector get(String name) {
        Connector c = items.get(name);
        if (c == null) {
            throw new IllegalArgumentException(
                "unknown connector " + name + "; known: " + items.keySet());
        }
        return c;
    }

    public boolean has(String name) { return items.containsKey(name); }

    public Set<String> names() { return Collections.unmodifiableSet(items.keySet()); }

    public void clear() { items.clear(); }
}
