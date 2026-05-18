"""Cross-SDK outbox parity (G3).

For each language ∈ {python, typescript, go, java}, drive **one pass** of that
language's outbox dispatcher against the same Tape server and assert the
PENDING+OUTBOX effect transitions to CONFIRMED.

If a language's toolchain isn't installed in the test environment, the
relevant test is skipped (not failed) — CI runs them all; local devs may
only have a subset.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from .scenario import (
    make_pending_outbox_effect,
    EFFECT_STATUS_CONFIRMED,
)


ROOT     = Path(__file__).resolve().parents[3]
SDK_PY   = ROOT / "tape" / "sdk" / "python"
SDK_TS   = ROOT / "tape" / "sdk" / "typescript"
SDK_GO   = ROOT / "tape" / "sdk" / "go"
SDK_JAVA = ROOT / "tape" / "sdk" / "java"


def _run(cmd, *, cwd=None, env=None, timeout=60):
    res = subprocess.run(
        cmd, cwd=cwd, env=env, timeout=timeout,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    return res


# ── Python ──────────────────────────────────────────────────────────────────

def test_outbox_parity_python(tape_server):
    """Python: invoke `python -m tape.reactors.outbox --once` after loading a
    fake 'log' connector module the harness writes to a temp directory."""
    url = tape_server["url"]
    scn = make_pending_outbox_effect(url, language_tag="python")

    # Write a tiny "log" connector module that the CLI can --load.
    helper_dir = Path(tape_server["db"]).parent
    helper_file = helper_dir / "parity_log_connector.py"
    helper_file.write_text("""
from tape.connectors import register, DispatchResult, ObservationResult, CompensationResult

class _LogConnector:
    name = "log"
    def dispatch(self, effect):
        return DispatchResult(status="confirmed", external_ref="logged",
                              response={"logged": True})
    def observe(self, effect):
        return ObservationResult(status="confirmed", external_ref="logged")
    def compensate(self, obligation):
        return CompensationResult(status="compensated")

register(_LogConnector())
""")

    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(SDK_PY), str(helper_dir),
                                          env.get("PYTHONPATH", "")])
    res = _run(
        [sys.executable, "-m", "tape.reactors.outbox",
         "--url", url, "--once",
         "--load", "parity_log_connector"],
        env=env, timeout=30,
    )
    assert res.returncode == 0, (
        f"python outbox failed: rc={res.returncode}\n"
        f"stdout:\n{res.stdout.decode()}\nstderr:\n{res.stderr.decode()}"
    )

    eff = scn.wait_for_status(EFFECT_STATUS_CONFIRMED)
    assert eff.status == EFFECT_STATUS_CONFIRMED


# ── TypeScript ──────────────────────────────────────────────────────────────

def _have_node_22() -> bool:
    if shutil.which("node") is None: return False
    try:
        out = subprocess.check_output(["node", "--version"], timeout=5).decode().strip()
        # need >= v22 for stable --experimental-strip-types
        major = int(out.lstrip("v").split(".")[0])
        return major >= 22
    except Exception:
        return False


def test_outbox_parity_typescript(tape_server):
    if not _have_node_22():
        pytest.skip("node >= 22 not installed")
    if not (SDK_TS / "node_modules").exists():
        # tests/conftest doesn't install npm deps; do it here on demand.
        npm = shutil.which("npm") or pytest.skip("npm not installed")
        rc = subprocess.run([npm, "install", "--silent"], cwd=SDK_TS,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            timeout=120).returncode
        if rc != 0:
            pytest.skip("npm install failed in tape/sdk/typescript")

    url = tape_server["url"]
    scn = make_pending_outbox_effect(url, language_tag="typescript")

    res = _run(
        ["node", "--experimental-strip-types", "--no-warnings",
         "bin/tape-outbox-ts.ts",
         "--url", url, "--once",
         "--register-log-connector",
         "--log-connector-path", str(Path(tape_server["db"]).parent / "ts-outbox.jsonl")],
        cwd=SDK_TS, timeout=60,
    )
    assert res.returncode == 0, (
        f"tape-outbox-ts failed: rc={res.returncode}\n"
        f"stdout:\n{res.stdout.decode()}\nstderr:\n{res.stderr.decode()}"
    )

    eff = scn.wait_for_status(EFFECT_STATUS_CONFIRMED)
    assert eff.status == EFFECT_STATUS_CONFIRMED


# ── Go ──────────────────────────────────────────────────────────────────────

def test_outbox_parity_go(tape_server):
    if shutil.which("go") is None:
        pytest.skip("go not installed")
    url = tape_server["url"]
    scn = make_pending_outbox_effect(url, language_tag="go")

    res = _run(
        ["go", "run", "./cmd/tape-outbox",
         "--url", url, "--once",
         "--register-log-connector",
         "--log-connector-path", str(Path(tape_server["db"]).parent / "go-outbox.jsonl")],
        cwd=SDK_GO, timeout=180,
    )
    assert res.returncode == 0, (
        f"tape-outbox (go) failed: rc={res.returncode}\n"
        f"stdout:\n{res.stdout.decode()}\nstderr:\n{res.stderr.decode()}"
    )

    eff = scn.wait_for_status(EFFECT_STATUS_CONFIRMED)
    assert eff.status == EFFECT_STATUS_CONFIRMED


# ── Java ────────────────────────────────────────────────────────────────────

def test_outbox_parity_java(tape_server):
    if shutil.which("mvn") is None:
        pytest.skip("mvn not installed")
    url = tape_server["url"]
    scn = make_pending_outbox_effect(url, language_tag="java")

    # Build a fat-ish classpath (compile + runtime + the built jar) then run
    # TapeOutbox directly. Cheaper than `mvn exec:java` on cold start, and
    # works in CI cache.
    target = SDK_JAVA / "target"
    classes = target / "classes"
    if not classes.exists():
        rc = subprocess.run(
            ["mvn", "-q", "-DskipTests", "package"],
            cwd=SDK_JAVA, timeout=300,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        ).returncode
        if rc != 0 or not classes.exists():
            pytest.skip("mvn package failed")

    # Generate the runtime classpath via Maven (cached after first call).
    cp_file = target / "cp.txt"
    if not cp_file.exists():
        rc = subprocess.run(
            ["mvn", "-q", "dependency:build-classpath",
             f"-Dmdep.outputFile={cp_file}", "-Dmdep.includeScope=runtime"],
            cwd=SDK_JAVA, timeout=180,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        ).returncode
        if rc != 0 or not cp_file.exists():
            pytest.skip("could not build java classpath")
    cp = f"{classes}:{cp_file.read_text().strip()}"

    res = _run(
        ["java", "-cp", cp, "dev.tape.cli.TapeOutbox",
         "--url", url, "--once",
         "--register-log-connector",
         "--log-connector-path", str(Path(tape_server["db"]).parent / "java-outbox.jsonl")],
        timeout=60,
    )
    assert res.returncode == 0, (
        f"tape-outbox (java) failed: rc={res.returncode}\n"
        f"stdout:\n{res.stdout.decode()}\nstderr:\n{res.stderr.decode()}"
    )

    eff = scn.wait_for_status(EFFECT_STATUS_CONFIRMED)
    assert eff.status == EFFECT_STATUS_CONFIRMED
