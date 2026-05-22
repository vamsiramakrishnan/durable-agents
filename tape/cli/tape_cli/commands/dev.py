"""`tape dev` — local: server + reactors + agent, with optional emulators."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import typer

from ..config import TapeProject, find_project_root
from ..util import console, ok, warn, info, fail, which, run_cmd, die


def _start(cmd: list[str], cwd: Path, env: dict) -> subprocess.Popen:
    info(f"[dim]$ {' '.join(cmd)}[/dim]")
    return subprocess.Popen(cmd, cwd=str(cwd), env={**os.environ, **env})


def run(
    store: Optional[str] = typer.Option(None, "--store", help="Override store: sqlite | bigtable-emulator | postgres-emulator."),
    events: Optional[str] = typer.Option(None, "--events", help="Override events: none | pubsub-emulator."),
    use_docker: Optional[bool] = typer.Option(None, "--docker/--no-docker",
        help="Run via Docker Compose (default: yes if available and store != sqlite)."),
    server_binary: Optional[str] = typer.Option(None, "--server-binary",
        help="Path to a built `tape-server` binary (native mode)."),
    kill_resume_demo: bool = typer.Option(False, "--kill-resume-demo",
        help="Crash mid-run; the recovery reactor resumes; verifies one effect lands."),
    port: int = typer.Option(7878, "--port"),
):
    root = find_project_root()
    project = TapeProject.load(root / "tape.yaml")

    # Embedded tier — no server. Run the reactor loop + the live journal.
    if project.project.tier == "adk":
        _adk_dev(root, project)
        return

    eff_store = store or project.tape.store.kind
    eff_events = events or project.tape.events.kind

    docker = which("docker")
    if use_docker is None:
        use_docker = bool(docker) and (eff_store != "sqlite" or eff_events.endswith("-emulator"))
    if use_docker and not docker:
        die("Docker requested but `docker` is not on PATH.")

    if use_docker:
        _docker_dev(root, project, eff_store, eff_events, port)
    else:
        _native_dev(root, project, eff_store, server_binary, port, kill_resume_demo)


def _adk_dev(root: Path, project) -> None:
    """`tape dev` for the embedded (`tier: adk`) tier.

    No server. Starts the reactor loop (`python -m tape_adk`) as a
    subprocess and opens the live journal view. The developer drives
    their agent separately (`adk run app`, `adk web`, or a script) — its
    effects appear in the live view.
    """
    emb = project.embedded
    if emb is None:
        die("tape.yaml has `project.tier: adk` but no `embedded:` section.",
            )
    db_url = emb.db_url
    # SQLite file stores: make sure the parent dir exists.
    if db_url.startswith("sqlite") and ":///" in db_url:
        db_path = db_url.split(":///", 1)[1]
        if db_path and db_path != ":memory:":
            Path(db_path).resolve().parent.mkdir(parents=True, exist_ok=True)

    # The reactor loop needs the project importable (for `--connectors
    # app.connectors:CONNECTORS`).
    child_env = {"PYTHONPATH": os.pathsep.join(
        [str(root), os.environ.get("PYTHONPATH", "")])}

    reactor_cmd = [sys.executable, "-m", "tape_adk",
                   "--db-url", db_url,
                   "--tick-ms", str(emb.reactor_interval_ms)]
    if emb.connectors:
        reactor_cmd += ["--connectors", emb.connectors]

    ok(f"embedded tier — db: {db_url}")
    procs: list[subprocess.Popen] = []
    try:
        procs.append(_start(reactor_cmd, cwd=root, env=child_env))
        ok("reactor loop started (outbox · reconciler · compensation · timers)")
        info("")
        info("[dim]Drive your agent in another terminal — e.g. "
             "`adk run app` — effects appear below. Ctrl-C to stop.[/dim]")
        info("")
        time.sleep(0.6)  # let the reactor log its startup line first

        # Open the live journal. Blocks until Ctrl-C.
        from .inspect_adk import live_journal
        live_journal(db_url,
                     reactor_note="[green]running[/green] "
                                  f"(tick {emb.reactor_interval_ms}ms)")
    finally:
        for p in procs:
            try:
                p.terminate()
            except Exception:  # noqa: BLE001
                pass


def _native_dev(root: Path, project, store: str, server_binary: Optional[str],
                port: int, kill_resume_demo: bool) -> None:
    if store != "sqlite":
        warn(f"native mode currently supports sqlite only; you asked for {store}. "
             "Falling back to sqlite for the server.")
    db = root / ".tape" / "dev.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    binary = server_binary or _find_server_binary()
    if not binary:
        die("no tape-server binary found. Build with `cargo build --release` in tape/server, "
            "or run `tape dev` with Docker.",
            )
    server_env = {"TAPE_LISTEN": f"127.0.0.1:{port}",
                  "TAPE_STORE": f"sqlite:{db}",
                  "RUST_LOG": "tape_server=info"}
    procs: list[subprocess.Popen] = []
    try:
        procs.append(_start([binary, "--listen", f"127.0.0.1:{port}",
                             "--store", f"sqlite:{db}"],
                            cwd=root, env=server_env))
        ok(f"tape-server listening on 127.0.0.1:{port}")
        time.sleep(1.0)

        agent_env = {"TAPE_URL": f"tape://127.0.0.1:{port}",
                     "PYTHONPATH": str(root)}
        # Start reactors (if reactors are enabled and a runner_factory is set).
        if project.tape.reactors.enabled_names() and project.agent.runner_factory:
            procs.append(_start([sys.executable, "-m", "tape.reactors",
                                 "--runner-from", project.agent.runner_factory,
                                 "--url", agent_env["TAPE_URL"]],
                                cwd=root, env=agent_env))
            ok("tape-reactors started")

        if kill_resume_demo:
            info("\n[bold]kill-resume demo:[/bold] start the agent, the server is up — "
                 "run your scripted scenario, then `tape doctor` to see the post-mortem.")
        info("\nPress Ctrl-C to stop.")
        signal.pause() if hasattr(signal, "pause") else _wait_forever(procs)
    finally:
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass


def _docker_dev(root: Path, project, store: str, events: str, port: int) -> None:
    compose_path = root / "docker-compose.tape.yaml"
    if not compose_path.exists():
        # Fallback to the project's docker-compose.yaml if the overlay isn't there.
        compose_path = root / "docker-compose.yaml"
    if not compose_path.exists():
        die(f"no docker-compose at {compose_path}. Run `tape init` to scaffold one.")
    info(f"Using {compose_path.relative_to(root)}")
    rc = run_cmd(["docker", "compose", "-f", str(compose_path), "up", "--build"],
                 cwd=root, check=False).returncode
    sys.exit(rc)


def _find_server_binary() -> Optional[str]:
    for cand in ("./target/release/tape-server",
                 "../tape/server/target/release/tape-server",
                 "/usr/local/bin/tape-server",
                 "tape-server"):
        p = Path(cand)
        if p.exists() and p.is_file() and os.access(p, os.X_OK):
            return str(p.resolve())
        binp = which(cand)
        if binp:
            return binp
    return None


def _wait_forever(procs):  # pragma: no cover — windows
    try:
        for p in procs:
            p.wait()
    except KeyboardInterrupt:
        pass
