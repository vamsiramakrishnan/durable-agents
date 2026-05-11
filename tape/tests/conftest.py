"""Test fixtures: a real Tape server (subprocess) backed by an ephemeral SQLite
file, plus the environment the example subprocesses need."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import grpc
import pytest

ROOT = Path(__file__).resolve().parents[2]            # repo root
SERVER_BIN = ROOT / "tape" / "server" / "target" / "debug" / "tape-server"
SDK_PY = ROOT / "tape" / "sdk" / "python"
EXAMPLES = ROOT / "tape" / "examples"

# Make the SDK + examples importable in this process and in subprocesses.
sys.path.insert(0, str(SDK_PY))
sys.path.insert(0, str(EXAMPLES))


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_up(url: str, timeout: float = 15.0) -> None:
    from tape.client import _target
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            ch = grpc.insecure_channel(_target(url))
            grpc.channel_ready_future(ch).result(timeout=1.0)
            ch.close()
            return
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(0.1)
    raise RuntimeError(f"tape server never came up at {url}: {last}")


@pytest.fixture(scope="session")
def tape_server(tmp_path_factory):
    if not SERVER_BIN.exists():
        pytest.skip(f"tape-server binary not built — run `cargo build` in {SERVER_BIN.parent.parent}")
    d = tmp_path_factory.mktemp("tape")
    db = d / "tape.db"
    port = _free_port()
    url = f"tape://127.0.0.1:{port}"
    env = dict(os.environ)
    env["RUST_LOG"] = env.get("RUST_LOG", "tape_server=warn")
    proc = subprocess.Popen(
        [str(SERVER_BIN), "--listen", f"127.0.0.1:{port}", "--db", str(db)],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    try:
        _wait_up(url)
        yield {"url": url, "db": str(db), "port": port, "proc": proc}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture()
def example_env(tape_server, tmp_path):
    import uuid

    ledger_dir = tmp_path / "ledgers"
    ledger_dir.mkdir()
    env = dict(os.environ)
    env["TAPE_URL"] = tape_server["url"]
    env["TAPE_EXAMPLE_DIR"] = str(ledger_dir)
    env["TAPE_APP"] = "treasury"
    env["TAPE_USER"] = "cfo"
    env["TAPE_SESSION"] = f"sess-{uuid.uuid4().hex[:10]}"   # unique per test (the DB is shared)
    env["TAPE_LEASE_MS"] = "1500"          # so a crashed run is recoverable quickly
    env["PYTHONPATH"] = os.pathsep.join([str(SDK_PY), str(EXAMPLES), env.get("PYTHONPATH", "")])
    # Run example subprocesses from tape/examples (so `python -m treasury.run`
    # works) — NOT from the repo root, where the dir `tape/` would shadow the
    # installed `tape` package on sys.path[0].
    return {"env": env, "url": tape_server["url"], "db": tape_server["db"],
            "ledger_dir": ledger_dir, "cwd": str(EXAMPLES)}
