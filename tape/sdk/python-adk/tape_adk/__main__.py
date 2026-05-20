"""`python -m tape_adk` (or the `tape-adk-reactors` console script) — runs
all four reactors in a loop against a `TapeSessionService`. Drop this into
a Cloud Run Job, a GKE CronJob, or a docker sidecar.

The CLI is intentionally minimal — no daemonisation, no service discovery,
no health-check endpoint. It runs the four reactor functions on a tick,
sleeps, repeats. Compose it with the operating system's process supervisor
(systemd / docker / k8s) for restarts and observability.

Connectors are loaded via `--connectors module.path:attr_name` — the
referenced attribute should be a dict (or callable returning a dict)
mapping connector names to Connector instances. Multiple `--connectors`
flags are allowed.

    python -m tape_adk \\
        --db-url postgresql+asyncpg://… \\
        --connectors my_app.connectors:CONNECTORS \\
        --claimer "$(hostname)-$$" \\
        --tick-ms 1000

`--once` runs a single tick then exits — convenient for cron-style
invocations where the scheduler handles cadence.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import os
import signal
import socket
import sys
from typing import Any, Mapping

from .connectors import Connector
from .reactors import (
    dispatch_outbox_once,
    drain_obligations_once,
    fire_due_timers_once,
    reconcile_once,
)
from .service import TapeSessionService


log = logging.getLogger("tape_adk.reactors")


def _load_connectors(specs: list[str]) -> dict[str, Connector]:
    """Each spec is `module.path:attr_name`. The attribute is either:
      * a dict[str, Connector], or
      * a callable returning a dict[str, Connector].

    All loaded dicts are merged; a later spec's keys overwrite an
    earlier's, so the user can compose `defaults + overrides`."""
    out: dict[str, Connector] = {}
    for spec in specs or []:
        mod_path, _, attr = spec.partition(":")
        if not attr:
            raise ValueError(
                f"--connectors {spec!r}: expected `module.path:attr_name`")
        module = importlib.import_module(mod_path)
        thing: Any = getattr(module, attr)
        if callable(thing):
            thing = thing()
        if not isinstance(thing, Mapping):
            raise ValueError(
                f"--connectors {spec!r}: expected a dict (or a callable "
                f"returning one), got {type(thing).__name__}")
        out.update(thing)
    return out


async def _tick(svc: TapeSessionService, *,
                connectors: dict[str, Connector],
                claimer: str,
                enable_outbox: bool, enable_reconcile: bool,
                enable_drain: bool, enable_timers: bool) -> None:
    """One pass through every enabled reactor. Each step is best-effort:
    if one raises, we log + continue. The reactors themselves are
    idempotent so a partial tick doesn't corrupt state."""
    if enable_outbox:
        try:
            r = await dispatch_outbox_once(
                svc, connectors=connectors, claimer=claimer)
            if r:
                log.info("outbox: %d action(s)", len(r))
        except Exception as ex:  # noqa: BLE001
            log.exception("outbox tick crashed: %s", ex)
    if enable_reconcile:
        try:
            r = await reconcile_once(svc, connectors=connectors)
            if r:
                log.info("reconcile: %d action(s)", len(r))
        except Exception as ex:  # noqa: BLE001
            log.exception("reconcile tick crashed: %s", ex)
    if enable_drain:
        try:
            r = await drain_obligations_once(
                svc, connectors=connectors, claimer=claimer)
            if r:
                log.info("drain: %d action(s)", len(r))
        except Exception as ex:  # noqa: BLE001
            log.exception("drain tick crashed: %s", ex)
    if enable_timers:
        try:
            r = await fire_due_timers_once(svc)
            if r:
                log.info("timers: %d action(s)", len(r))
        except Exception as ex:  # noqa: BLE001
            log.exception("timers tick crashed: %s", ex)


async def _run(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    log.info("starting reactor loop against %s", args.db_url)

    svc = TapeSessionService(db_url=args.db_url)
    connectors = _load_connectors(args.connectors)
    log.info("loaded %d connector(s): %s", len(connectors),
              ", ".join(sorted(connectors)) or "<none>")

    claimer = args.claimer or f"{socket.gethostname()}-{os.getpid()}"
    stop_event = asyncio.Event()

    def _on_signal(_signum, _frame):
        log.info("stop signal received; exiting after the current tick")
        try:
            asyncio.get_running_loop().call_soon_threadsafe(stop_event.set)
        except RuntimeError:
            pass

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    tick_s = max(args.tick_ms / 1000.0, 0.05)
    try:
        while not stop_event.is_set():
            await _tick(svc,
                        connectors=connectors,
                        claimer=claimer,
                        enable_outbox=not args.no_outbox,
                        enable_reconcile=not args.no_reconcile,
                        enable_drain=not args.no_drain,
                        enable_timers=not args.no_timers)
            if args.once:
                break
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=tick_s)
            except asyncio.TimeoutError:
                pass
    finally:
        log.info("reactor loop exiting")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(
        prog="python -m tape_adk.reactors",
        description=("Run the tape-adk reactor loop (outbox dispatcher, "
                     "reconciler, compensation drainer, timer firer) "
                     "against a TapeSessionService's SQLAlchemy backend."))
    p.add_argument(
        "--db-url",
        default=os.environ.get("TAPE_ADK_DB_URL",
                                "sqlite+aiosqlite:///./tape.db"),
        help=("SQLAlchemy URL for the store ADK is using. Default: "
              "$TAPE_ADK_DB_URL or sqlite+aiosqlite:///./tape.db"))
    p.add_argument(
        "--connectors", action="append", default=[],
        metavar="MODULE:ATTR",
        help=("Path to a dict[str, Connector] (or callable returning "
              "one). May be repeated; later specs override earlier."))
    p.add_argument(
        "--claimer", default="",
        help=("Identity used in CAS leases (so doctor / inspect can show "
              "which dispatcher holds what). Default: <hostname>-<pid>."))
    p.add_argument(
        "--tick-ms", type=int, default=1000,
        help="Pause between ticks in ms. Default: 1000.")
    p.add_argument(
        "--once", action="store_true",
        help="Run one tick and exit (good for cron-style invocations).")
    p.add_argument(
        "--no-outbox", action="store_true",
        help="Skip the outbox dispatcher tick.")
    p.add_argument(
        "--no-reconcile", action="store_true",
        help="Skip the reconciler tick.")
    p.add_argument(
        "--no-drain", action="store_true",
        help="Skip the compensation drain tick.")
    p.add_argument(
        "--no-timers", action="store_true",
        help="Skip the timer firer tick.")
    p.add_argument("--log-level", default="INFO",
                    help="DEBUG | INFO | WARNING | ERROR. Default: INFO.")

    args = p.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
