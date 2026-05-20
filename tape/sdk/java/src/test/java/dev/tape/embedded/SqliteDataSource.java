package dev.tape.embedded;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.UUID;
import java.util.logging.Logger;

/** Tiny in-memory SQLite DataSource for tests. Each test instance uses a
 *  unique shared-cache URL so concurrent connections see the same DB. */
final class SqliteDataSource implements DataSource {

    private final String url;

    SqliteDataSource() {
        // file::memory: + cache=shared lets multiple JDBC connections share
        // the same in-memory DB. A unique name keeps tests isolated.
        String name = "tape-test-" + UUID.randomUUID().toString().substring(0, 8);
        this.url = "jdbc:sqlite:file:" + name + "?mode=memory&cache=shared";
        // Keep one "keepalive" connection open for the lifetime of this
        // DataSource so the shared in-memory DB doesn't get reclaimed
        // between checkouts. Stored on the instance to keep it reachable.
        try {
            this.keepalive = DriverManager.getConnection(url);
        } catch (SQLException e) {
            throw new RuntimeException("failed to open sqlite keepalive", e);
        }
    }

    @SuppressWarnings("unused") // intentionally retained
    private final Connection keepalive;

    String url() { return url; }

    @Override public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url);
    }

    @Override public Connection getConnection(String username, String password) throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    void shutdown() {
        try { keepalive.close(); } catch (SQLException ignore) {}
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
