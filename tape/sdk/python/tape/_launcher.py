"""`tape run -- python my_adk_app.py` — the zero-touch mode.

It import-time-wraps `google.adk.runners.Runner.__init__` so that any runner the
target program builds gets a `TapePlugin` and a `TapeSessionService` injected,
without editing the program. This is a monkeypatch; the spec says so plainly
(design-principles/tape.md §3). Use the explicit two-line wiring for anything you
own; use this for retrofitting an app you'd rather not touch.

    tape run --url tape://localhost:7878 -- python my_adk_app.py [args...]
"""

from __future__ import annotations

import os
import runpy
import sys


def _install(url: str) -> None:
    from google.adk.runners import Runner
    from .adk import TapePlugin, TapeSessionService

    if getattr(Runner, "_tape_patched", False):
        return
    orig_init = Runner.__init__

    def patched_init(self, *args, **kwargs):
        plugins = list(kwargs.get("plugins") or [])
        if not any(getattr(p, "name", "") == "tape" for p in plugins):
            plugins.append(TapePlugin(url))
        kwargs["plugins"] = plugins
        if "session_service" not in kwargs or kwargs["session_service"] is None:
            kwargs["session_service"] = TapeSessionService(url)
        return orig_init(self, *args, **kwargs)

    Runner.__init__ = patched_init
    Runner._tape_patched = True


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in ("run",):
        print("usage: tape run [--url tape://host:port] -- <program> [args...]", file=sys.stderr)
        return 2
    argv = argv[1:]
    url = os.environ.get("TAPE_URL", "tape://localhost:7878")
    if argv and argv[0] == "--url":
        url = argv[1]
        argv = argv[2:]
    if argv and argv[0] == "--":
        argv = argv[1:]
    if not argv:
        print("usage: tape run [--url tape://host:port] -- <program> [args...]", file=sys.stderr)
        return 2

    os.environ.setdefault("TAPE_URL", url)
    _install(url)

    # Run the target program as if it were invoked directly.
    target = argv[0]
    sys.argv = argv
    if target == "python" or target.endswith("/python") or target == sys.executable:
        # `tape run -- python foo.py args` -> run foo.py
        rest = argv[1:]
        if rest and rest[0] == "-m":
            sys.argv = rest[1:]
            runpy.run_module(rest[1], run_name="__main__", alter_sys=True)
        elif rest:
            sys.argv = rest
            runpy.run_path(rest[0], run_name="__main__")
        else:
            print("tape run: nothing to run", file=sys.stderr)
            return 2
    else:
        runpy.run_path(target, run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
