package dev.tape.embedded;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.UUID;
import java.util.logging.Logger;

/** Tiny SQLite DataSource for tests. Uses a temp file (NOT shared-cache
 *  in-memory) so concurrent JDBC connections coordinate via OS file
 *  locking + WAL mode — the same posture the production embedded path
 *  uses, and which the Python reference's CAS lock + StaticPool also
 *  effectively serialises. Sets {@code busy_timeout=5000} so writers
 *  wait briefly under contention instead of throwing immediately. */
final class SqliteDataSource implements DataSource {

    private final String url;
    private final Path dbFile;

    SqliteDataSource() {
        try {
            this.dbFile = Files.createTempFile(
                "tape-test-" + UUID.randomUUID().toString().substring(0, 8), ".db");
        } catch (java.io.IOException e) {
            throw new RuntimeException("failed to create temp sqlite file", e);
        }
        this.url = "jdbc:sqlite:" + dbFile.toAbsolutePath();
        try {
            this.keepalive = openConnection();
            // Enable WAL so a writer doesn't block readers (matches the
            // posture a production embedded SQLite deployment would use).
            try (Statement st = keepalive.createStatement()) {
                st.execute("PRAGMA journal_mode = WAL");
            }
        } catch (SQLException e) {
            throw new RuntimeException("failed to open sqlite keepalive", e);
        }
    }

    @SuppressWarnings("unused") // intentionally retained
    private final Connection keepalive;

    String url() { return url; }

    private Connection openConnection() throws SQLException {
        Connection c = DriverManager.getConnection(url);
        try (Statement st = c.createStatement()) {
            st.execute("PRAGMA busy_timeout = 5000");
        }
        return c;
    }

    @Override public Connection getConnection() throws SQLException {
        return openConnection();
    }

    @Override public Connection getConnection(String username, String password) throws SQLException {
        return openConnection();
    }

    void shutdown() {
        try { keepalive.close(); } catch (SQLException ignore) {}
        try { Files.deleteIfExists(dbFile); } catch (java.io.IOException ignore) {}
        try { Files.deleteIfExists(Path.of(dbFile.toString() + "-wal")); } catch (java.io.IOException ignore) {}
        try { Files.deleteIfExists(Path.of(dbFile.toString() + "-shm")); } catch (java.io.IOException ignore) {}
    }

    @Override public PrintWriter getLogWriter() { return null; }
    @Override public void setLogWriter(PrintWriter out) {}
    @Override public void setLoginTimeout(int seconds) {}
    @Override public int getLoginTimeout() { return 0; }
    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return (T) this;
        throw new SQLException("not a wrapper for " + iface);
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
}
