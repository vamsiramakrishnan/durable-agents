"""End-to-end smoke tests for `tape inspect` + `tape tail`.

Drives the CLI against a real tape-server (the shared `tape_server` fixture
from conftest.py). The journal is seeded by talking to TapeClient directly:
one decision, one OUTBOX effect, one obligation, one gate. We then assert
that `tape inspect <id> --print` and `--raw` and `--summary` render the
expected shape, and that `tape tail` picks up the same entries.

The Textual app itself is NOT driven here — Textual's pilot needs a TTY-ish
context and these tests run headless. The TUI's logic is decomposed enough
(_journal.py, status bar, detail pane) that the rich-rendered modes give us
good coverage of the decoder + status colors.
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SDK_PY = ROOT / "tape" / "sdk" / "python"
CLI = ROOT / "tape" / "cli"
sys.path.insert(0, str(SDK_PY))
sys.path.insert(0, str(CLI))


@pytest.fixture()
def seeded_run(tape_server):
    """Create a fresh run with a journal that exercises every decoder branch:
    run-start, decision, effect (pending → confirmed), obligation, gate
    (waiting → released)."""
    from tape.client import TapeClient
    from tape._gen.tape_pb2 import (
        EFFECT_DISPATCH_MODE_OUTBOX,
        EFFECT_SEMANTICS_NON_IDEMPOTENT,
        EFFECT_STATUS_CONFIRMED,
    )

    url = tape_server["url"]
    invocation_id = f"inv-{uuid.uuid4().hex[:10]}"
    with TapeClient(url) as c:
        # 1. fresh run
        r = c.begin_run(
            app_name="treasury", user_id="cfo",
            session_id=f"sess-{uuid.uuid4().hex[:6]}",
            invocation_id=invocation_id, lease_owner="test-runner",
            # PR 12: non-idempotent effects need a granted scope.
            scopes=["mcp:tools:bank.wire"])
        run_id = r.run_id

        # 2. one decision
        c.record_decision(run_id=run_id, decision_index=0,
                          model="gemini-2.0", request_json='{"q":"sweep"}',
                          response_json='{"a":"yes"}', rationale="excess USD")

        # 3. one OUTBOX + NON_IDEMPOTENT effect, then complete it
        eff = c.begin_effect(
            run_id=run_id, decision_index=0,
            tool_name="bank.wire", call_index=0,
            request_json='{"amount":2000000}',
            semantics=EFFECT_SEMANTICS_NON_IDEMPOTENT,
            dispatch_mode=EFFECT_DISPATCH_MODE_OUTBOX,
            business_key="acct1:2m:2026-05-18",
            connector="bank.wire",
            scope="mcp:tools:bank.wire")
        c.complete_effect(run_id=run_id, idempotency_key=eff.idempotency_key,
                          status=EFFECT_STATUS_CONFIRMED,
                          response_json='{"wire_id":"abc123"}')

        # 4. an obligation
        c.register_compensation(run_id=run_id, effect_key=eff.idempotency_key,
                                kind="reverse_wire",
                                payload_json='{"amount":2000000}')

        # 5. a gate cycle
        c.await_signal(run_id=run_id, gate_name="cfo_approval",
                       payload_json='{"context":"large wire"}')
        c.send_signal(run_id=run_id, gate_name="cfo_approval",
                      resolution_json='{"decision":"approve"}')

    return {"url": url, "run_id": run_id}


# ── helpers ────────────────────────────────────────────────────────────────


def _invoke(args, env=None):
    """Run the typer app in-process, capturing stdout.

    Force a wide terminal so rich doesn't ellipsis-truncate type / status /
    details cells. CliRunner runs without a real TTY, which makes rich pick
    a narrow default width that hides "decisi…" / "obliga…" mid-cell.
    """
    from typer.testing import CliRunner
    from tape_cli.main import app
    runner = CliRunner()
    merged = {"COLUMNS": "240", "TERM": "dumb"}
    if env:
        merged.update(env)
    return runner.invoke(app, args, env=merged)


# ── tests ──────────────────────────────────────────────────────────────────


def test_inspect_print_mode_renders_all_kinds(seeded_run):
    """--print mode dumps a one-shot snapshot. Verify every primitive kind
    we journaled appears in the rendered output."""
    r = _invoke(["inspect", seeded_run["run_id"], "--print",
                 "--url", seeded_run["url"]])
    assert r.exit_code == 0, r.stdout + (r.stderr or "")
    out = r.stdout
    # Header bits
    assert "RUN" in out
    assert seeded_run["run_id"] in out
    # Every kind we seeded should be in the body
    for kind in ("run", "decision", "effect", "obligation", "gate"):
        assert kind in out, f"missing kind {kind!r} in:\n{out}"
    # The status badge for the completed effect
    assert "confirmed" in out
    # The OUTBOX / NON-IDEMPOTENT tags
    assert "NI" in out or "non" in out.lower()
    # The bank.wire tool name
    assert "bank.wire" in out


def test_inspect_summary_exit_zero_when_clean(seeded_run):
    """--summary exits 0 when no UNKNOWN / FAILED effects, no STUCK obligations."""
    r = _invoke(["inspect", seeded_run["run_id"], "--summary",
                 "--url", seeded_run["url"]])
    assert r.exit_code == 0, r.stdout + (r.stderr or "")
    assert "clean" in r.stdout.lower()


def test_inspect_summary_exit_one_on_unknown(tape_server):
    """When an effect ends UNKNOWN, --summary must exit non-zero. This makes
    `tape inspect --summary` usable as a CI smoke gate."""
    from tape.client import TapeClient
    from tape._gen.tape_pb2 import EFFECT_STATUS_UNKNOWN

    url = tape_server["url"]
    with TapeClient(url) as c:
        rid = c.begin_run(
            app_name="t", user_id="u", session_id="s",
            invocation_id=f"inv-{uuid.uuid4().hex[:8]}",
            lease_owner="x").run_id
        c.record_decision(run_id=rid, decision_index=0, model="m")
        eff = c.begin_effect(run_id=rid, decision_index=0, tool_name="bank.x",
                             call_index=0)
        c.complete_effect(run_id=rid, idempotency_key=eff.idempotency_key,
                          status=EFFECT_STATUS_UNKNOWN,
                          error_json='{"err":"ack lost"}')
    r = _invoke(["inspect", rid, "--summary", "--url", url])
    assert r.exit_code == 1, r.stdout
    assert "unknown" in r.stdout.lower() or "non-clean" in r.stdout.lower()


def test_inspect_raw_is_jsonl(seeded_run):
    """--raw emits JSONL — one JournalEntry per line, parseable, ordered by seq."""
    r = _invoke(["inspect", seeded_run["run_id"], "--raw", "--no-follow",
                 "--url", seeded_run["url"]])
    assert r.exit_code == 0, r.stdout + (r.stderr or "")
    lines = [ln for ln in r.stdout.splitlines() if ln.strip()]
    assert len(lines) >= 5, f"expected at least 5 journal lines, got {len(lines)}:\n{r.stdout}"
    parsed = [json.loads(ln) for ln in lines]
    # Every line has the canonical fields
    for p in parsed:
        assert {"seq", "kind", "ts_ms"} <= set(p.keys())
    # Strictly increasing seq
    seqs = [p["seq"] for p in parsed]
    assert seqs == sorted(seqs), f"seq order broken: {seqs}"
    # All the kinds we expect are present
    kinds = {p["kind"] for p in parsed}
    for k in ("run", "decision", "effect", "obligation", "gate"):
        assert k in kinds, f"--raw missing kind {k}: kinds={kinds}"


def test_inspect_no_args_lists_recoverable(tape_server):
    """`tape inspect` with no args lists recoverable runs — the operator's
    hot set. With none, it prints a friendly hint."""
    r = _invoke(["inspect", "--url", tape_server["url"]])
    assert r.exit_code == 0, r.stdout + (r.stderr or "")
    assert ("Recoverable runs" in r.stdout
            or "no recoverable runs" in r.stdout)


def test_inspect_dash_l_lists_recoverable(tape_server):
    """`tape inspect --list` (or `-l`) is the explicit alias."""
    r = _invoke(["inspect", "--list", "--url", tape_server["url"]])
    assert r.exit_code == 0, r.stdout + (r.stderr or "")
    assert ("Recoverable" in r.stdout or "no recoverable" in r.stdout)


def test_inspect_unknown_run_id_dies(tape_server):
    """A typo / stale run id should fail loudly, not drop into a blank TUI."""
    r = _invoke(["inspect", "no-such-run-id", "--print",
                 "--url", tape_server["url"]])
    assert r.exit_code != 0
    assert "no such run" in r.stdout.lower() or "no such run" in (r.stderr or "").lower()


def test_inspect_limit_truncates_raw(seeded_run):
    """--limit caps how many entries the streaming modes emit."""
    r = _invoke(["inspect", seeded_run["run_id"], "--raw", "--no-follow",
                 "--limit", "2", "--url", seeded_run["url"]])
    assert r.exit_code == 0, r.stdout + (r.stderr or "")
    lines = [ln for ln in r.stdout.splitlines() if ln.strip()]
    assert len(lines) == 2


def test_tail_picks_up_seeded_entries(seeded_run):
    """`tape tail --limit N --no-... ` should stream existing journal entries
    too (global_seq starts at 0). Drains a few + bails."""
    r = _invoke(["tail", "--limit", "5", "--raw",
                 "--from-global-seq", "0",
                 "--url", seeded_run["url"]])
    assert r.exit_code == 0, r.stdout + (r.stderr or "")
    lines = [ln for ln in r.stdout.splitlines() if ln.strip()]
    assert len(lines) >= 1
    # Every line is JSON
    for ln in lines:
        json.loads(ln)


def test_tail_subject_filter(seeded_run):
    """A subject pattern over the bus filters server-side. Drains effects."""
    r = _invoke(["tail", "--subject", "/tape/effect/**", "--limit", "1",
                 "--raw", "--url", seeded_run["url"]])
    assert r.exit_code == 0, r.stdout + (r.stderr or "")
    lines = [ln for ln in r.stdout.splitlines() if ln.strip()]
    # When the bus actually emits subject-routed events we'll see ≥1; when the
    # store doesn't (e.g. an older snapshot) we tolerate 0 — the important
    # thing is the CLI plumbing doesn't blow up.
    for ln in lines:
        p = json.loads(ln)
        if p.get("subject"):
            assert p["subject"].startswith("/tape/effect/"), p["subject"]


# ── decoder unit tests (no server needed) ──────────────────────────────────


def test_decoder_handles_sim_payload_shape():
    """The in-memory sim store writes ad-hoc-format!() payload strings; the
    SQL store writes serde_json. The decoder must handle both gracefully."""
    from tape_cli.commands._journal import decode_entry

    sim_effect = (
        '{"run_id":"r","status":"pending","tool":"bank.wire",'
        '"idempotency_key":"r/d-0/bank.wire/0","decision_index":0,'
        '"semantics":2,"dispatch_mode":2,"business_key":"x","connector":"y"}'
    )
    d = decode_entry("effect", sim_effect)
    assert d.type == "effect"
    assert d.status == "pending"
    assert "bank.wire" in d.summary
    assert "NI" in d.summary    # non-idempotent tag
    assert "OUT" in d.summary   # outbox tag


def test_decoder_handles_sql_payload_shape():
    """SQL store writes serde_json shapes; the decoder reads the same fields."""
    from tape_cli.commands._journal import decode_entry

    sql_gate = '{"gate":"cfo_approval","status":"waiting"}'
    d = decode_entry("gate", sql_gate)
    assert d.type == "gate"
    assert d.status == "waiting"
    assert "cfo_approval" in d.summary


def test_decoder_unknown_status_is_loud():
    """The whole point of the inspector is to make UNKNOWN visible. The
    style for status=unknown must be the loudest one we have."""
    from tape_cli.commands._journal import decode_entry, status_style

    d = decode_entry("effect", '{"status":"unknown","tool":"x"}')
    assert d.status == "unknown"
    # The style must include 'red' — that's how we make UNKNOWN catch the eye.
    assert "red" in d.style.lower()
    assert "red" in status_style("unknown").lower()


def test_decoder_run_lifecycle_kinds():
    """The `run` kind covers begin/end transitions — exercise both styles."""
    from tape_cli.commands._journal import decode_entry

    d_run = decode_entry("run", '{"status":"running","app":"t","user":"u","session":"s","run_id":"r"}')
    assert d_run.type == "run"
    assert d_run.status == "running"

    d_term = decode_entry("run", '{"status":"terminal","app":"t","user":"u","session":"s","run_id":"r"}')
    assert d_term.status == "terminal"


def test_decoder_resilient_to_bad_payload():
    """A garbage payload mustn't crash the inspector — the journal sometimes
    holds payloads written by older server versions or by buggy custom code."""
    from tape_cli.commands._journal import decode_entry

    for bad in ("", "not json", "{", "null", "[1,2,3]"):
        d = decode_entry("effect", bad)
        assert d is not None
        assert d.type == "effect"


# ── replay-pair decoder tests (the FIRST RUN / REPLAY mapping) ─────────────


def test_replay_pair_decision():
    """A decision entry maps to RecordDecision (first) and GetDecision (replay).
    The replay side must say it does NOT call the model — that's the whole point."""
    from tape_cli.commands._replay import replay_pair

    p = replay_pair("decision",
                    '{"model":"gemini-2.0","decision_index":0,"rationale":"x"}')
    assert "RecordDecision" in p.first_run
    assert "GetDecision" in p.replay
    assert "without calling the model" in p.replay.lower()
    assert p.is_read_only


def test_replay_pair_effect_statuses():
    """Each effect status (pending / confirmed / failed / unknown) maps to a
    distinct first-run / replay pair; the replay column must never re-call
    the tool. UNKNOWN must mention the reconciler."""
    from tape_cli.commands._replay import replay_pair

    pend = replay_pair("effect",
                       '{"status":"pending","tool":"bank.wire"}')
    assert "PENDING" in pend.first_run
    assert "PENDING" in pend.replay

    conf = replay_pair("effect",
                       '{"status":"confirmed","tool":"bank.wire"}')
    assert "short-circuits" in conf.replay.lower()
    assert "without calling the tool" in conf.replay.lower()

    fail = replay_pair("effect",
                       '{"status":"failed","tool":"bank.wire"}')
    assert "short-circuits" in fail.replay.lower()

    unk = replay_pair("effect",
                      '{"status":"unknown","tool":"bank.wire"}')
    assert "reconciler" in unk.replay.lower()


def test_replay_pair_gate_lifecycle():
    """Gates: waiting → AwaitSignal first / re-park on replay;
    released → SendSignal first / read recorded resolution on replay."""
    from tape_cli.commands._replay import replay_pair

    w = replay_pair("gate", '{"gate":"cfo_approval","status":"waiting"}')
    assert "AwaitSignal" in w.first_run

    r = replay_pair("gate", '{"gate":"cfo_approval","status":"released"}')
    assert "SendSignal" in r.first_run
    assert "without parking" in r.replay.lower()


def test_replay_pair_run_lifecycle():
    """Run begin: BeginRun (fresh) → BeginRun (resumed=true).
    Run terminal: EndRun → no replay (terminal is terminal)."""
    from tape_cli.commands._replay import replay_pair

    started = replay_pair("run", '{"status":"running"}')
    assert "BeginRun" in started.first_run
    assert "resumed=true" in started.replay.lower()

    ended = replay_pair("run", '{"status":"terminal"}')
    assert "EndRun" in ended.first_run
    assert "terminal" in ended.replay.lower()


def test_replay_pair_obligation_value_timer():
    """Smoke test the remaining primitives."""
    from tape_cli.commands._replay import replay_pair

    ob = replay_pair("obligation", '{"kind":"reverse_wire"}')
    assert "RegisterCompensation" in ob.first_run

    v = replay_pair("value", '{"value":{"namespace":"x","key":"y","version":3}}')
    assert "WriteValue" in v.first_run
    assert "GetValue" in v.replay

    tm = replay_pair("timer", '{"kind":"redrive"}')
    assert "SetTimer" in tm.first_run


# ── Textual app headless tests ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_textual_app_boots_and_filters(seeded_run):
    """Drive the Textual app via its pilot: confirm the journal renders, the
    `e`/`o`/`a` filter bindings flip filter_kind + rebuild the table, the
    `/` + escape pair toggles the search bar, and `f` toggles auto-follow.

    Skipped if Textual isn't installed (the rest of the inspector still works
    via the rich modes)."""
    pytest.importorskip("textual")
    from tape.client import TapeClient
    from tape_cli.commands._inspector_app import TapeInspectorApp
    from textual.widgets import DataTable

    client = TapeClient(seeded_run["url"])
    try:
        app = TapeInspectorApp(client, seeded_run["run_id"],
                               url=seeded_run["url"])
        async with app.run_test(size=(200, 60)) as pilot:
            # Give the stream worker a beat to drain the seeded entries.
            await pilot.pause(1.5)
            table = app.query_one(DataTable)
            # We seeded: run, decision, effect (pending+confirmed = 2), obligation, gate (waiting+released = 2) = 7
            assert table.row_count >= 5, f"expected at least 5 rows, got {table.row_count}"
            initial_rows = table.row_count

            # `e` → effects only
            await pilot.press("e")
            await pilot.pause(0.3)
            assert app.filter_kind == "effect"
            assert table.row_count == 2, table.row_count

            # `o` → obligations only
            await pilot.press("o")
            await pilot.pause(0.3)
            assert app.filter_kind == "obligation"
            assert table.row_count == 1, table.row_count

            # `g` → gates only
            await pilot.press("g")
            await pilot.pause(0.3)
            assert app.filter_kind == "gate"
            assert table.row_count == 2, table.row_count

            # `a` → all
            await pilot.press("a")
            await pilot.pause(0.3)
            assert app.filter_kind == ""
            assert table.row_count == initial_rows

            # `f` flips follow
            assert app.follow is True
            await pilot.press("f")
            await pilot.pause(0.1)
            assert app.follow is False

            # `/` then 'bank' filters to entries mentioning bank.wire —
            # both effects (tool name) and the obligation (effect_key
            # contains the tool name) match. Three entries in our fixture.
            await pilot.press("slash")
            await pilot.pause(0.2)
            for ch in "bank":
                await pilot.press(ch)
            await pilot.pause(0.3)
            assert app.search_query == "bank"
            assert table.row_count == 3, table.row_count

            # Escape clears search
            await pilot.press("escape")
            await pilot.pause(0.3)
            assert app.search_query == ""
            assert table.row_count == initial_rows
    finally:
        client.close()


@pytest.mark.asyncio
async def test_replay_screen_pushes_and_syncs_cursor(seeded_run):
    """Press `R` in the inspector → ReplayScreen lands on top of the stack,
    with one row per journal entry on each side. Moving the cursor on one
    side moves the cursor on the other (synchronized scrolling)."""
    pytest.importorskip("textual")
    from tape.client import TapeClient
    from tape_cli.commands._inspector_app import TapeInspectorApp
    from textual.widgets import DataTable

    client = TapeClient(seeded_run["url"])
    try:
        app = TapeInspectorApp(client, seeded_run["run_id"],
                               url=seeded_run["url"])
        async with app.run_test(size=(220, 60)) as pilot:
            await pilot.pause(1.5)
            n_entries = len(app.entries)
            assert n_entries >= 5

            # Open the replay diff via the binding.
            await pilot.press("R")
            await pilot.pause(0.5)
            screen = app.screen
            assert type(screen).__name__ == "ReplayScreen"

            # Both DataTables have exactly one row per journaled entry.
            first = screen.query_one("#first-run-table", DataTable)
            rep = screen.query_one("#replay-table", DataTable)
            assert first.row_count == n_entries
            assert rep.row_count == n_entries

            # Cursor sync: move down on the left, the right tracks.
            first.focus()
            await pilot.press("down")
            await pilot.pause(0.15)
            await pilot.press("down")
            await pilot.pause(0.15)
            assert first.cursor_row == rep.cursor_row
            assert first.cursor_row >= 2

            # `escape` pops back to the timeline screen.
            await pilot.press("escape")
            await pilot.pause(0.3)
            assert type(app.screen).__name__ != "ReplayScreen"
    finally:
        client.close()


@pytest.mark.asyncio
async def test_replay_screen_direct_launch_via_flag(seeded_run):
    """`tape inspect <id> --replay` (start_in_replay=True) should land on
    the replay screen automatically once the journal is drained."""
    pytest.importorskip("textual")
    from tape.client import TapeClient
    from tape_cli.commands._inspector_app import TapeInspectorApp

    client = TapeClient(seeded_run["url"])
    try:
        app = TapeInspectorApp(client, seeded_run["run_id"],
                               url=seeded_run["url"],
                               start_in_replay=True)
        async with app.run_test(size=(220, 60)) as pilot:
            # Wait long enough for the stream worker to drain + the
            # _maybe_push_replay tick to land us on the screen.
            await pilot.pause(2.5)
            assert type(app.screen).__name__ == "ReplayScreen"
    finally:
        client.close()
