"""server_failpoint.py — a server-layer failpoint (the *post-DB* crash).

Where the connector-layer scenarios in this directory chaos the
upstream calls, this one chaoses the Tape server itself. The server's
`tape::begin_effect::post_db` failpoint sits right after the SQL commit
that durably journals the effect intent — perfect for asserting that
even if the server replies *after* writing but *before* the client sees
the response, the journal is consistent (the client retries, the row
short-circuits, no double row).

**Requires a `tape-server` built with `--features chaos`:**

    cd tape/server && cargo build --features chaos --release
    ./target/release/tape-server --listen 127.0.0.1:7878 \\
        --store sqlite::memory: &

Without `--features chaos` the server ignores the FAILPOINTS env var
and the scenario passes trivially — `tape chaos doctor` will warn you.

    tape chaos run tape/examples/chaos_scenarios/server_failpoint.py
"""

from __future__ import annotations

import tape.chaos as chaos


SCENARIO = chaos.Scenario(
    name="server-post-db-crash",
    faults=(
        # The server commits the BeginEffect row, then crashes before
        # returning. The client times out and retries; the second call
        # short-circuits on the existing row.
        chaos.crash("tape::begin_effect::post_db"),
    ),
    invariants=(
        chaos.no_stuck_obligations,
        chaos.no_blind_non_idempotent_retry,
    ),
    seed=99,
)


def body(client, session):
    # When `chaos.crash(...)` is in the scenario, `tape chaos run` prints
    # the FAILPOINTS env-var spec it computed. Pass that to the server's
    # process environment (the doctor command prints the exact line).
    #
    # For an agent-driven body, invoke your runner here — the server
    # will have the failpoint armed for the duration of this scenario.
    pass
