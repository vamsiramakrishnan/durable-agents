"""The kill-and-resume test, run against the Cloud Bigtable backend.

This self-bootstraps the Bigtable emulator if the binaries are available — the
`cbtemulator` (a.k.a. `emulator` from `cloud.google.com/go/bigtable/cmd/emulator`)
and `cbt` — looked up on PATH, in $TAPE_BT_BIN_DIR, or in /tmp/gobin. If they're
not there, the test skips. Install them once with::

    GOBIN=/tmp/gobin go install cloud.google.com/go/bigtable/cmd/emulator@latest
    GOBIN=/tmp/gobin go install cloud.google.com/go/cbt@latest

(Then `pytest tape/tests/test_bigtable.py` picks them up.) Or point the test at
an already-running emulator / a real instance by setting BIGTABLE_EMULATOR_HOST
and TAPE_TEST_BIGTABLE_URL.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import grpc
import pytest

ROOT = Path(__file__).resolve().parents[2]
SERVER_BIN = ROOT / "tape" / "server" / "target" / "debug" / "tape-server"
SDK_PY = ROOT / "tape" / "sdk" / "python"
EXAMPLES = ROOT / "tape" / "examples"
sys.path.insert(0, str(SDK_PY))
sys.path.insert(0, str(EXAMPLES))


def _which(*names):
    for n in names:
        p = shutil.which(n)
        if p:
            return p
    for d in (os.environ.get("TAPE_BT_BIN_DIR", ""), "/tmp/gobin"):
        for n in names:
            cand = Path(d) / n if d else None
            if cand and cand.exists():
                return str(cand)
    return None


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _wait_up(target: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            ch = grpc.insecure_channel(target)
            grpc.channel_ready_future(ch).result(timeout=1.0)
            ch.close()
            return
        except Exception as ex:  # noqa: BLE001
            last = ex
            time.sleep(0.1)
    raise RuntimeError(f"not up at {target}: {last}")


@pytest.fixture(scope="module")
def bigtable_server():
    if not SERVER_BIN.exists():
        pytest.skip(f"tape-server not built — run `cargo build` in {SERVER_BIN.parent.parent}")
    emu_url = os.environ.get("BIGTABLE_EMULATOR_HOST")
    procs = []
    cbt_env = dict(os.environ)
    if not emu_url:
        emu_bin = _which("cbtemulator", "emulator")
        cbt_bin = _which("cbt")
        if not emu_bin or not cbt_bin:
            pytest.skip("Bigtable emulator / cbt not found (see test_bigtable.py docstring)")
        port = _free_port()
        emu_url = f"localhost:{port}"
        procs.append(subprocess.Popen([emu_bin, "-port", str(port)],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        cbt_env["BIGTABLE_EMULATOR_HOST"] = emu_url
        # wait for the emulator, then create the table + family
        time.sleep(1.5)
        for args in (["createtable", "tape"], ["createfamily", "tape", "m"], ["setgcpolicy", "tape", "m", "maxversions=1"]):
            subprocess.run([cbt_bin, "-project", "demo", "-instance", "demo", *args],
                           env=cbt_env, capture_output=True, text=True)
    else:
        cbt_bin = _which("cbt")
        if cbt_bin:
            for args in (["createtable", "tape"], ["createfamily", "tape", "m"]):
                subprocess.run([cbt_bin, "-project", "demo", "-instance", "demo", *args],
                               env=cbt_env, capture_output=True, text=True)

    url_path = os.environ.get("TAPE_TEST_BIGTABLE_URL", "bigtable://demo/demo/tape")
    sport = _free_port()
    tape_url = f"tape://127.0.0.1:{sport}"
    srv_env = dict(os.environ)
    srv_env["BIGTABLE_EMULATOR_HOST"] = emu_url
    srv_env["RUST_LOG"] = "tape_server=warn"
    procs.append(subprocess.Popen([str(SERVER_BIN), "--listen", f"127.0.0.1:{sport}", "--store", url_path],
                                  env=srv_env, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT))
    try:
        _wait_up(f"127.0.0.1:{sport}")
        yield {"url": tape_url}
    finally:
        for p in reversed(procs):
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


def _run_example(env, *args, expect_crash=False):
    proc = subprocess.run([sys.executable, "-m", "treasury.run", *args],
                          env=env, cwd=str(EXAMPLES), capture_output=True, text=True, timeout=120)
    if expect_crash:
        assert proc.returncode != 0, f"expected a crash\n{proc.stdout}\n{proc.stderr}"
    else:
        assert proc.returncode == 0, f"failed rc={proc.returncode}\n{proc.stdout}\n{proc.stderr}"
    return proc


def _bank_wires(ledger_dir: Path) -> int:
    p = ledger_dir / "bank.json"
    if not p.exists():
        return 0
    return sum(1 for k in json.loads(p.read_text()) if not k.startswith("reverse:"))


def test_bigtable_event_bus_register_and_claim(bigtable_server):
    """Event-bus surface on Bigtable: RegisterReaction → matcher creates a
    task → ClaimTasks returns it → CompleteTask finishes it."""
    from tape.client import (
        HANDLER_KIND_TASK, TASK_STATUS_DONE, TASK_STATUS_PENDING, TapeClient,
    )

    url = bigtable_server["url"]
    with TapeClient(url) as c:
        r = c.register_reaction(
            name="bt-event-bus-1",
            subject_pattern="/tape/value/changed/btns/**",
            handler_kind=HANDLER_KIND_TASK,
        )
        rid = r.reaction_id
        assert rid

        # Trigger a matching journal entry.
        c.write_value(namespace="btns", key="k1",
                      value_json='{"v":1}', writer="t")

        # Wait for the matcher (1 s poll) to create the task.
        deadline = time.time() + 8.0
        tasks = []
        while time.time() < deadline:
            tasks = c.list_tasks(reaction_id=rid, limit=10)
            if tasks:
                break
            time.sleep(0.25)
        assert tasks, "matcher should have created one task within 8 s"
        assert tasks[0].subject.startswith("/tape/value/changed/btns/"), tasks[0].subject

        # Claim it.
        claimed = c.claim_tasks(reaction_id=rid, owner="dispatcher-A",
                                lease_ms=60_000, max=10)
        assert len(claimed) == 1
        assert claimed[0].task_id == tasks[0].task_id

        # Complete it.
        done = c.complete_task(task_id=claimed[0].task_id, owner="dispatcher-A")
        assert done.status == TASK_STATUS_DONE

        # And confirm via list_tasks(status=DONE).
        done_list = c.list_tasks(reaction_id=rid, status=TASK_STATUS_DONE, limit=10)
        assert any(t.task_id == claimed[0].task_id for t in done_list)


def test_bigtable_bootstrap_from_head_skips_backlog(bigtable_server):
    """`bootstrap_from_head=True` seeds the reaction's cursor at the current
    journal head — so writes that happened before registration produce zero
    tasks, and writes after produce one each."""
    from tape.client import HANDLER_KIND_TASK, TapeClient

    url = bigtable_server["url"]
    with TapeClient(url) as c:
        # Write a few value entries BEFORE registering — these should be the
        # backlog that the reaction must skip.
        for i in range(3):
            c.write_value(namespace="bt-boot", key=f"backlog-{i}",
                          value_json=str(i), writer="t")
        # Small pause so the meta#global_seq counter is definitely past the
        # backlog before we register.
        time.sleep(0.2)

        # Register with bootstrap_from_head=True. The Python SDK helper doesn't
        # expose the flag, so we build the proto directly.
        from tape._gen import tape_pb2 as pb
        r = c.stub.RegisterReaction(pb.Reaction(
            name="bt-bootstrap",
            subject_pattern="/tape/value/changed/bt-boot/**",
            handler_kind=HANDLER_KIND_TASK,
            bootstrap_from_head=True,
            max_concurrency=1, num_shards=1,
        ))
        rid = r.reaction_id

        # Give the matcher a tick to process the backlog (which it should skip
        # because the cursor was bootstrapped past it).
        time.sleep(2.0)
        backlog_tasks = c.list_tasks(reaction_id=rid, limit=10)
        assert backlog_tasks == [], (
            f"bootstrap_from_head must skip pre-registration entries; "
            f"got {len(backlog_tasks)} tasks: "
            f"{[t.subject for t in backlog_tasks]}")

        # Now write a fresh entry post-registration — it MUST produce a task.
        c.write_value(namespace="bt-boot", key="post",
                      value_json="post", writer="t")
        deadline = time.time() + 8.0
        post_tasks = []
        while time.time() < deadline:
            post_tasks = c.list_tasks(reaction_id=rid, limit=10)
            if post_tasks:
                break
            time.sleep(0.25)
        assert post_tasks, "post-registration write must yield a task"
        assert any("/tape/value/changed/bt-boot/" in t.subject for t in post_tasks)


def test_bigtable_crash_then_resume_closes_the_book_once(bigtable_server, tmp_path):
    from tape.client import TapeClient
    import tape.client as tc

    ledger_dir = tmp_path / "ledgers"
    ledger_dir.mkdir()
    env = dict(os.environ)
    env["TAPE_URL"] = bigtable_server["url"]
    env["TAPE_EXAMPLE_DIR"] = str(ledger_dir)
    env["TAPE_LEASE_MS"] = "1500"
    env["TAPE_SESSION"] = f"bt-{os.urandom(4).hex()}"
    env["PYTHONPATH"] = os.pathsep.join([str(SDK_PY), str(EXAMPLES), env.get("PYTHONPATH", "")])

    _run_example(dict(env, TAPE_CRASH_AFTER="execute_sweep"), "--reset", expect_crash=True)
    assert _bank_wires(ledger_dir) == 1

    client = TapeClient(bigtable_server["url"])
    deadline = time.time() + 12
    runs = []
    while time.time() < deadline:
        runs = [r for r in client.list_runs_to_recover(limit=50).runs if r.session_id == env["TAPE_SESSION"]]
        if runs:
            break
        time.sleep(0.3)
    assert runs, "no recoverable run after the crash"
    run_id = runs[0].run_id
    assert runs[0].status != tc.RUN_STATUS_TERMINAL

    _run_example(env, "--recover")

    assert _bank_wires(ledger_dir) == 1, "the wire must not have happened twice"
    gl = ledger_dir / "gl.json"
    assert gl.exists() and len(json.loads(gl.read_text())) == 1, "exactly one GL batch"
    fresh = client.get_run(run_id)
    assert fresh.status == tc.RUN_STATUS_TERMINAL
    client.close()
