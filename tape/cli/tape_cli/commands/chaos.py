"""`tape chaos` — drive scenarios, replay determinism, derive LDFI cuts.

Phase 5 of TapeChaos. Mirrors the SDK surface (`tape.chaos.*`) at the
command line so a chaos run is one shell command — not a Python REPL
session.

    tape chaos run scenarios/bank_wire_under_chaos.py
    tape chaos replay scenarios/bank_wire_under_chaos.py --seed 42
    tape chaos lineage --run r-abc
    tape chaos derive --run r-abc
    tape chaos doctor

Each subcommand loads scenarios from a Python module that exposes a
module-level `SCENARIO: tape.chaos.Scenario` and (for `run`/`replay`) a
`body(client, session)` callable. This keeps the scenarios versionable
alongside the agent code, where they belong.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

import typer

from ..util import console, die, fail, info, ok, section, table, warn

app = typer.Typer(no_args_is_help=True, help="Drive chaos scenarios + replay.")


# ── module loading ──────────────────────────────────────────────────────────

def _load_module(path: Path) -> Any:
    """Import a scenario file by path. Adds its parent dir to sys.path so
    relative imports work (`from .lib import build_runner`)."""
    if not path.exists():
        die(f"scenario file not found: {path}")
    if path.is_dir():
        die(f"scenario path is a directory; pass the .py file: {path}")
    sys.path.insert(0, str(path.parent.resolve()))
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        die(f"could not load scenario module: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _scenario_from(mod: Any):
    """Pull the SCENARIO attribute, validating the type."""
    import tape.chaos as chaos

    scen = getattr(mod, "SCENARIO", None)
    if scen is None:
        die("scenario module must define a module-level `SCENARIO = chaos.scenario(...)`")
    if not isinstance(scen, chaos.Scenario):
        die(f"`SCENARIO` must be a tape.chaos.Scenario; got {type(scen).__name__}")
    return scen


def _body_from(mod: Any):
    body = getattr(mod, "body", None)
    if body is None or not callable(body):
        die("scenario module must define `def body(client, session): ...`")
    return body


def _default_url(url: Optional[str]) -> str:
    if url:
        return url
    return os.environ.get("TAPE_URL", "tape://localhost:7878")


# ── tape chaos run ──────────────────────────────────────────────────────────

@app.command(help="Run a scenario once and print the report.")
def run(
    scenario: Path = typer.Argument(..., exists=True, dir_okay=False,
                                     help="Path to a scenario .py file."),
    url: Optional[str] = typer.Option(None, "--url", "-u",
                                       help="Tape server URL (default $TAPE_URL)."),
):
    try:
        import tape.chaos as chaos
        from tape.client import TapeClient
    except ImportError:
        die("tape-py not installed.", code=2)

    mod = _load_module(scenario)
    scen = _scenario_from(mod)
    body = _body_from(mod)
    target = _default_url(url)

    section(f"Scenario: {scen.name}")
    info(f"  seed: {scen.seed}")
    info(f"  faults: {len(scen.faults)}")
    info(f"  invariants: {len(scen.invariants)}")
    failpoints = chaos.failpoints_env(scen)
    if failpoints:
        info(f"  server FAILPOINTS: {failpoints}")

    with chaos.session(scen, url=target) as sess:
        try:
            body(TapeClient(target), sess)
        except Exception as ex:
            warn(f"body raised: {type(ex).__name__}: {ex}")

    report = sess.report
    if report.passed:
        ok(f"{scen.name}: PASS")
    else:
        fail(f"{scen.name}: FAIL")
    for ir in report.invariant_results:
        mark = ok if ir.passed else fail
        mark(f"  {ir}")
    for note in report.notes:
        info(f"  ! {note}")
    if not report.passed:
        raise typer.Exit(code=1)


# ── tape chaos replay ───────────────────────────────────────────────────────

@app.command(help="Replay a scenario twice with the same seed and check determinism.")
def replay(
    scenario: Path = typer.Argument(..., exists=True, dir_okay=False),
    seed: Optional[int] = typer.Option(None, "--seed", "-s",
                                        help="Override the scenario's seed."),
    url: Optional[str] = typer.Option(None, "--url", "-u"),
):
    try:
        import tape.chaos as chaos
    except ImportError:
        die("tape-py not installed.")

    mod = _load_module(scenario)
    scen = _scenario_from(mod)
    body = _body_from(mod)
    target = _default_url(url)

    if seed is not None:
        scen = chaos.scenario(name=scen.name, faults=scen.faults,
                               invariants=scen.invariants, seed=int(seed))

    section(f"Replay: {scen.name}  (seed={scen.seed})")
    report = chaos.replay(scen, body, url=target, deadline_s=5.0)
    if report.bit_identical:
        ok(f"{scen.name}: DETERMINISTIC")
        info(f"  journal: {len(report.snap_a.lines)} canonical lines")
    else:
        fail(f"{scen.name}: DRIFTED")
        a_len = len(report.snap_a.lines) if report.snap_a else 0
        b_len = len(report.snap_b.lines) if report.snap_b else 0
        info(f"  journal lengths: {a_len} vs {b_len}")
        for ln in report.diff_summary[:10]:
            info(f"  - {ln}")
        if len(report.diff_summary) > 10:
            info(f"  ... and {len(report.diff_summary) - 10} more")
    for note in report.notes:
        info(f"  ! {note}")
    if not report.bit_identical:
        raise typer.Exit(code=1)


# ── tape chaos lineage ──────────────────────────────────────────────────────

@app.command(help="Walk one run's lineage DAG and print each node + its breaking failpoint.")
def lineage(
    run_id: str = typer.Option(..., "--run", "-r", help="The run_id to walk."),
    url: Optional[str] = typer.Option(None, "--url", "-u"),
):
    try:
        import tape.chaos as chaos
        from tape.client import TapeClient
    except ImportError:
        die("tape-py not installed.")

    target = _default_url(url)
    section(f"Lineage: run={run_id}")
    with TapeClient(target) as c:
        graph = chaos.LineageGraph.from_run(c, run_id, deadline_s=3.0)
    if not graph.nodes:
        warn("no journal entries for this run (yet)")
        return
    t = table(f"{len(graph.nodes)} lineage nodes", ["seq", "kind", "parent", "breaks at"])
    for n in graph.nodes:
        t.add_row(str(n.seq), n.kind, str(n.parent_seq),
                  n.breaking_failpoint or "—")
    console.print(t)


# ── tape chaos derive ───────────────────────────────────────────────────────

@app.command(help="LDFI: derive chaos scenarios from one successful run's lineage.")
def derive(
    run_id: str = typer.Option(..., "--run", "-r", help="The baseline run_id."),
    url: Optional[str] = typer.Option(None, "--url", "-u"),
    max_cut_size: int = typer.Option(1, "--max-cut", help="Maximum cut size (1 = singletons; >=2 multiplies)."),
):
    try:
        import tape.chaos as chaos
        from tape.client import TapeClient
    except ImportError:
        die("tape-py not installed.")

    target = _default_url(url)
    section(f"Derive LDFI scenarios: run={run_id}")
    with TapeClient(target) as c:
        graph = chaos.LineageGraph.from_run(c, run_id, deadline_s=3.0)
    derived = chaos.derive_scenarios(graph, max_cut_size=max_cut_size)
    if not derived:
        warn("no scenarios derived (lineage was empty)")
        return
    info(f"derived {len(derived)} scenario(s) from {len(graph.nodes)} lineage node(s)")
    t = table("derived scenarios", ["scenario", "faults", "FAILPOINTS spec"])
    for s in derived:
        t.add_row(s.name, str(len(s.faults)), chaos.failpoints_env(s))
    console.print(t)
    info("")
    info("Drive one with `tape chaos run <scenario.py>` against a chaos-feature server:")
    info("  RUSTFLAGS='' cargo build --features chaos --release   # in tape/server")
    info("  FAILPOINTS=<spec> ./target/release/tape-server --listen 0.0.0.0:7878 --store ...")


# ── tape chaos doctor ──────────────────────────────────────────────────────

@app.command(help="Verify the local chaos surface is wired correctly.")
def doctor(
    url: Optional[str] = typer.Option(None, "--url", "-u"),
):
    section("TapeChaos surface")
    # 1. tape.chaos importable
    try:
        import tape.chaos as chaos
        ok(f"tape.chaos importable ({len(chaos.__all__)} exports)")
    except ImportError as ex:
        fail(f"tape.chaos not importable: {ex}",
              hint="pip install -e tape/sdk/python")
        raise typer.Exit(code=2)

    # 2. chaos-feature server build (best-effort, optional)
    repo = Path(__file__).resolve().parents[4]
    chaos_bin_dev = repo / "tape" / "server" / "target" / "debug" / "tape-server"
    chaos_bin_rel = repo / "tape" / "server" / "target" / "release" / "tape-server"
    if chaos_bin_dev.exists() or chaos_bin_rel.exists():
        ok(f"tape-server built (debug={chaos_bin_dev.exists()}, release={chaos_bin_rel.exists()})")
        info("  To activate failpoints, build with `cargo build --features chaos`")
    else:
        warn("tape-server not built locally")
        info("  cd tape/server && cargo build --features chaos --release")

    # 3. Pick the chaos-feature build if available
    info("")
    info("Tip: server-layer faults require a `--features chaos` build:")
    info("  FAILPOINTS='tape::begin_effect::post_db=panic' tape-server ...")

    # 4. Connectivity to the configured URL (optional)
    target = _default_url(url)
    info("")
    info(f"Target Tape server: {target}")
    try:
        from tape.client import TapeClient
        with TapeClient(target) as c:
            c.list_runs_to_recover(limit=1)
        ok(f"connected to {target}")
    except Exception as ex:
        warn(f"could not reach {target}: {ex}")
        info("  Set --url or $TAPE_URL to point at a running server.")
