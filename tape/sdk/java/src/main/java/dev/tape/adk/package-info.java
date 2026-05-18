/**
 * The Tape ↔ Google ADK (Java) adapter.
 *
 * <p>Two classes wire Tape into an ADK {@code Runner} the same way Python's
 * {@code tape.adk} does:
 *
 * <ul>
 *   <li>{@link dev.tape.adk.TapePlugin} — turns ADK's callbacks (beforeRun,
 *       afterModel, before/after tool, afterRun) into Tape journal entries.</li>
 *   <li>{@link dev.tape.adk.TapeSessionService} — implements ADK's
 *       {@code BaseSessionService} over Tape's {@code AppendEvent} /
 *       {@code CreateSession} / {@code GetSession} so the conversation and the
 *       journal commit together.</li>
 * </ul>
 *
 * <p>The ADK dependency ({@code com.google.adk:google-adk}) is declared at
 * {@code provided} scope in this SDK — agents that use the adapter pull
 * google-adk in themselves; non-ADK callers of {@code TapeClient} are not
 * forced to take it on.
 */
package dev.tape.adk;
