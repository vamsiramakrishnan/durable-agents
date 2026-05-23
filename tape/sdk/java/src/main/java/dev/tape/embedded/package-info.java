/**
 * Tape — embedded SQL parity for the Java SDK. Mirrors the Python
 * {@code tape_adk} package shape: the four storage tables ({@link
 * dev.tape.embedded.Schema}), the {@link dev.tape.embedded.TapeSessionService}
 * with effect/obligation/timer/KV ledgers, the {@link
 * dev.tape.embedded.Connector} protocol with three result records, four
 * stateless {@link dev.tape.embedded.Reactors} loop functions, and
 * decorator-equivalent declaration handles ({@link
 * dev.tape.embedded.Effects}, {@link dev.tape.embedded.OutboxTools}).
 *
 * <p>Works against any {@link javax.sql.DataSource}: SQLite (via
 * {@code org.xerial:sqlite-jdbc}, brought in at test scope only) and
 * Postgres (via {@code org.postgresql:postgresql}, not a hard dep).
 *
 * <p>The schema column names + types match the Python schema exactly, so
 * a Python writer + a Java reader against the same SQLite file (or
 * Postgres database) are wire-compatible.
 *
 * <p>This package does NOT include an ADK-Java plugin integration —
 * that's separate design work tracked in {@code SDK_PARITY.md}. The
 * embedded service + reactors are usable standalone today; the
 * {@link dev.tape.embedded.Effects} and {@link dev.tape.embedded.OutboxTools}
 * declaration shapes are the construction-time hook that a future plugin
 * will consume.
 */
package dev.tape.embedded;
