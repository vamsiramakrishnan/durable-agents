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
