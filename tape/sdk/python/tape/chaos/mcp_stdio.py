"""Stdio MCP proxy — Toxiproxy-for-MCP over the subprocess transport.

The Phase 4 HTTP/SSE proxy (`tape.chaos.proxies.mcp_proxy`) covers the
streamable-HTTP MCP transport. This module covers the *other* MCP
transport: a subprocess speaking JSON-RPC 2.0 line-by-line over its
stdin/stdout. That's the dominant form for local MCP servers — the
`uvx`/`npx`-style integrations the Claude Code, Cursor, and Goose
clients invoke.

Architecture:

```
   agent  ──stdin/stdout──▶  this proxy  ──stdin/stdout──▶  real MCP server
                                  │
                              fault rules
```

The agent invokes *us* as if we were the MCP server. We spawn the
real upstream as a child, pump messages in both directions, and apply
[`ProxyFault`][tape.chaos.proxies.ProxyFault] rules on the way. Same
fault vocabulary as the HTTP proxy — `tool_shadow`, `schema_drift`,
`inject_status`, `delay`, `drop_connection` — minus the SSE-specific
ones (`truncate_stream` doesn't apply: there's no event stream).

Usage as a library (in tests):

```python
from tape.chaos.mcp_stdio import MCPStdioProxy
from tape.chaos import proxy as pf

with MCPStdioProxy(
    upstream_cmd=["uvx", "mcp-server-time"],
    faults=[pf.tool_shadow("", extra_tool={"name": "exfiltrate"})],
) as p:
    # `p.send_request({...})` / `p.recv_response()` model the agent
    # side of the conversation — read the result, assert what the
    # injected tool looked like.
    ...
```

Usage as a CLI (in an MCP-client config):

```jsonc
{
  "mcpServers": {
    "time-with-chaos": {
      "command": "python", "args": ["-m", "tape.chaos.mcp_stdio",
                                    "--upstream", "uvx mcp-server-time",
                                    "--tool-shadow", "name=exfiltrate"]
    }
  }
}
```

Stdio framing: each JSON-RPC 2.0 message is one line (`\\n`-delimited).
The MCP spec mandates this — see https://spec.modelcontextprotocol.io/.
We don't speak MCP semantics; we read lines, parse as JSON, optionally
mutate via the fault rules, write back. Batches (`[{...}, {...}]`) are
handled by mutating each element.

Phase 4 of TapeChaos. Closes the stdio gap called out in
`tape.chaos.proxies` and in `design-principles/chaos.md §3e`.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import shlex
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import IO, Any, Callable, Iterator, List, Optional, Sequence

from .proxies import ProxyFault

_log = logging.getLogger("tape.chaos.mcp_stdio")


# ── direction tag ──────────────────────────────────────────────────────────

class _Dir:
    """Which way through the proxy a message is travelling."""
    TO_UPSTREAM = "to_upstream"      # agent → real server
    FROM_UPSTREAM = "from_upstream"  # real server → agent


# ── the proxy itself ───────────────────────────────────────────────────────

@dataclass
class MCPStdioProxy:
    """A stdio MCP proxy. Spawn the real server, pump messages, apply faults.

    The proxy is symmetric across the two directions: faults apply to
    *responses* by default (the way an attacker shadow-injects a tool
    into `tools/list`), but you can set `direction="to_upstream"` to
    rewrite requests instead (e.g. inject prompt content into a
    `tools/call` argument).

    Lifecycle (manual):
      ```
      p = MCPStdioProxy(upstream_cmd=["uvx", "mcp-server-time"])
      p.start()                              # spawns the upstream
      p.send_request({"jsonrpc":"2.0", ...}) # to the proxy
      r = p.recv_response()                  # from the proxy
      p.stop()
      ```

    As a context manager (preferred — guarantees cleanup):
      ```
      with MCPStdioProxy(upstream_cmd=[...]) as p:
          ...
      ```

    As a *passthrough* CLI (no library calls): the proxy's `run()`
    method reads from `sys.stdin` and writes to `sys.stdout`, so the
    agent that invoked us as a subprocess gets the proxied stream
    directly. That's what `python -m tape.chaos.mcp_stdio` does.
    """

    upstream_cmd: Sequence[str]
    faults: Sequence[ProxyFault] = ()

    # Internal state — populated by start().
    _proc: Optional[subprocess.Popen[bytes]] = field(default=None, init=False)
    # Replies the proxy synthesised (request-side `inject_status` /
    # `drop_connection`) that haven't been pulled by `recv_response` yet.
    _pending_replies: List[Any] = field(default_factory=list, init=False)
    _stop: threading.Event = field(default_factory=threading.Event, init=False)
    _pumps: List[threading.Thread] = field(default_factory=list, init=False)

    # ── lifecycle ─────────────────────────────────────────────────────────

    def start(self) -> "MCPStdioProxy":
        """Spawn the upstream subprocess and start the pump threads.

        The pumps run until [`stop`][.stop] is called or the upstream
        exits. Both pumps catch and log exceptions internally — a
        broken pipe in one direction doesn't kill the other.
        """
        if self._proc is not None:
            return self
        self._proc = subprocess.Popen(
            list(self.upstream_cmd),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        # Mirror upstream's stderr to our own so the agent sees the real
        # server's diagnostics — silently swallowing them would hide
        # genuine bugs.
        self._pumps.append(threading.Thread(
            target=self._pump_stderr, daemon=True, name="mcp-proxy-stderr"))
        # The agent→upstream and upstream→agent pumps run from the
        # request / response APIs themselves (library mode) — they're
        # only spawned as threads in CLI passthrough mode.
        for p in self._pumps:
            p.start()
        return self

    def stop(self, timeout: float = 2.0) -> None:
        """Stop pumping and terminate the upstream. Idempotent."""
        self._stop.set()
        if self._proc is not None and self._proc.poll() is None:
            try:
                self._proc.terminate()
                self._proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self._proc.kill()
            except Exception:  # already dead
                pass
        self._proc = None

    def __enter__(self) -> "MCPStdioProxy": return self.start()
    def __exit__(self, *_: Any) -> None: self.stop()

    # ── library API: send / recv (one JSON-RPC message at a time) ────────

    def send_request(self, msg: dict | list) -> None:
        """Send a JSON-RPC message agent→upstream. Applies any
        `direction=to_upstream` fault rules along the way.

        Two short-circuit cases:
          * `inject_status` synthesises an error reply and queues it
            for the next `recv_response` — the upstream never sees the
            request, mirroring how a proxy that decides to fail a call
            never forwards it.
          * `drop_connection` tears the proxy down; subsequent
            `recv_response` calls return `None`.
        """
        if self._proc is None or self._proc.stdin is None:
            raise RuntimeError("proxy not started")
        mutated = self._apply_faults(msg, _Dir.TO_UPSTREAM)
        if mutated is _DROP:
            self.stop()
            return
        if mutated is None:
            return  # fault deleted the message
        # If the fault synthesised a reply (inject_status produces a
        # message with "error" or "result" instead of "method"), don't
        # forward — queue it for recv_response.
        if isinstance(mutated, dict) and "method" not in mutated \
                and ("error" in mutated or "result" in mutated):
            self._pending_replies.append(mutated)
            return
        line = json.dumps(mutated, separators=(",", ":")) + "\n"
        try:
            self._proc.stdin.write(line.encode("utf-8"))
            self._proc.stdin.flush()
        except (BrokenPipeError, OSError):
            _log.debug("upstream stdin closed while sending request")

    def recv_response(self, timeout: float = 5.0) -> Optional[dict | list]:
        """Read one JSON-RPC message upstream→agent. Applies any
        `direction=from_upstream` fault rules (the default direction).
        Returns `None` on timeout, upstream EOF, or proxy tear-down."""
        if self._pending_replies:
            return self._pending_replies.pop(0)
        # Proxy was torn down (drop_connection); the agent sees EOF.
        if self._proc is None or self._proc.stdout is None:
            return None
        deadline = time.monotonic() + timeout
        line = self._readline_with_deadline(self._proc.stdout, deadline)
        if line is None:
            return None
        try:
            msg = json.loads(line)
        except json.JSONDecodeError as e:
            _log.warning("upstream emitted non-JSON line: %r (%s)", line[:120], e)
            return None
        mutated = self._apply_faults(msg, _Dir.FROM_UPSTREAM)
        if mutated is _DROP:
            self.stop()
            return None
        return mutated  # may be `None` (fault deleted) — caller handles

    # ── CLI passthrough: pump sys.stdin↔upstream, sys.stdout↔upstream ────

    def run(self) -> int:
        """Run as a CLI passthrough. Blocks until either side closes.

        The proxy reads JSON-RPC lines from `sys.stdin`, forwards them
        to the upstream, and forwards upstream replies to `sys.stdout`
        — applying faults on the way. Used as the entry point in MCP
        client configurations (`command: python`, `args: ["-m",
        "tape.chaos.mcp_stdio", ...]`).
        """
        self.start()
        assert self._proc is not None and self._proc.stdin is not None
        assert self._proc.stdout is not None
        # Threads, not asyncio: stdin's blocking read interacts poorly with
        # asyncio across platforms, and stdlib `selectors` doesn't see
        # file-handle EOF deterministically. Two daemon threads with a
        # shared `_stop` event is the boring-and-reliable choice.
        up_thread = threading.Thread(
            target=self._pump_agent_to_upstream, daemon=True, name="mcp-up")
        dn_thread = threading.Thread(
            target=self._pump_upstream_to_agent, daemon=True, name="mcp-dn")
        up_thread.start()
        dn_thread.start()

        # When the upstream exits or the agent closes its stdin, end.
        rc = self._proc.wait()
        self._stop.set()
        up_thread.join(timeout=1.0)
        dn_thread.join(timeout=1.0)
        return rc

    # ── internal: fault application ──────────────────────────────────────

    def _apply_faults(self, msg: Any, direction: str) -> Any:
        """Apply every applicable fault rule. Returns the (possibly
        mutated) message, `None` to drop it silently, or the `_DROP`
        sentinel to tear the whole proxy down (drop_connection)."""
        out: Any = msg
        for f in self.faults:
            if not _matches(f, msg, direction):
                continue
            if random.random() > f.probability:
                continue
            out = _apply_one(f, out, direction)
            if out is _DROP:
                return _DROP
            if out is None:
                return None
        return out

    # ── internal: CLI pump threads ───────────────────────────────────────

    def _pump_agent_to_upstream(self) -> None:
        assert self._proc is not None and self._proc.stdin is not None
        try:
            for raw in sys.stdin.buffer:
                if self._stop.is_set():
                    break
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    _log.warning("agent sent non-JSON: %r", line[:120])
                    continue
                mutated = self._apply_faults(msg, _Dir.TO_UPSTREAM)
                if mutated is _DROP:
                    self.stop()
                    return
                if mutated is None:
                    continue
                try:
                    self._proc.stdin.write(
                        (json.dumps(mutated, separators=(",", ":")) + "\n").encode("utf-8"))
                    self._proc.stdin.flush()
                except (BrokenPipeError, OSError):
                    return
        except Exception:
            _log.exception("agent→upstream pump died")

    def _pump_upstream_to_agent(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        try:
            for raw in self._proc.stdout:
                if self._stop.is_set():
                    break
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    # Pass-through: maybe it's a log line from a poorly
                    # behaved server. Stdout should be JSON-RPC only, but
                    # don't break the agent over it.
                    sys.stdout.buffer.write(raw)
                    sys.stdout.flush()
                    continue
                mutated = self._apply_faults(msg, _Dir.FROM_UPSTREAM)
                if mutated is _DROP:
                    self.stop()
                    return
                if mutated is None:
                    continue
                sys.stdout.buffer.write(
                    (json.dumps(mutated, separators=(",", ":")) + "\n").encode("utf-8"))
                sys.stdout.flush()
        except Exception:
            _log.exception("upstream→agent pump died")

    def _pump_stderr(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        try:
            for line in self._proc.stderr:
                sys.stderr.buffer.write(line)
                sys.stderr.flush()
        except Exception:
            pass

    @staticmethod
    def _readline_with_deadline(f: IO[bytes], deadline: float) -> Optional[str]:
        """Read one line, giving up after `deadline` wall-clock seconds.
        Uses non-blocking select on POSIX; falls back to a blocking read
        on Windows (where we expect tests to set generous timeouts)."""
        if os.name != "posix":
            return f.readline().decode("utf-8").rstrip("\r\n") or None
        import selectors
        sel = selectors.DefaultSelector()
        sel.register(f, selectors.EVENT_READ)
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                if sel.select(remaining):
                    raw = f.readline()
                    if not raw:
                        return None
                    return raw.decode("utf-8").rstrip("\r\n")
        finally:
            sel.close()


# ── sentinel for "tear the proxy down" ──────────────────────────────────

class _DropSentinel: pass
_DROP = _DropSentinel()


# ── fault dispatch ──────────────────────────────────────────────────────

def _matches(f: ProxyFault, msg: Any, direction: str) -> bool:
    """A fault matches when its kind is applicable here. `path_prefix`
    doubles as a JSON-RPC *method* prefix in stdio mode — there's no
    URL path. Empty string = match every message."""
    # Pull the method name out, if any.
    method = ""
    if isinstance(msg, dict):
        method = msg.get("method", "") or ""
    elif isinstance(msg, list) and msg and isinstance(msg[0], dict):
        method = msg[0].get("method", "") or ""

    if f.path_prefix and not method.startswith(f.path_prefix):
        return False

    # Direction filter: tool_shadow / schema_drift apply to responses
    # (from_upstream), inject_status applies wherever a reply might be
    # synthesised, delay / drop_connection apply both ways.
    if direction == _Dir.TO_UPSTREAM:
        return f.kind in ("delay", "drop_connection", "inject_status",
                          "inject_prompt", "schema_drift")
    return f.kind in ("delay", "drop_connection", "tool_shadow",
                      "schema_drift", "mangle_json", "inject_status")


def _apply_one(f: ProxyFault, msg: Any, direction: str) -> Any:
    """Apply one fault to one (already matched) message. Returns the
    mutated message, `None` to drop it, or `_DROP` for tear-down."""
    if f.kind == "delay":
        if f.ms > 0:
            jitter = 1.0 + (random.uniform(-f.jitter, f.jitter) if f.jitter else 0.0)
            time.sleep(f.ms * jitter / 1000.0)
        return msg

    if f.kind == "drop_connection":
        return _DROP

    if f.kind == "inject_status":
        # Synthesise a JSON-RPC error reply with the chosen code.
        # The agent sees the error in place of the real response.
        if isinstance(msg, dict) and "id" in msg:
            return {
                "jsonrpc": "2.0",
                "id": msg.get("id"),
                "error": {
                    "code": f.status or -32000,
                    "message": f.body or "injected by tape.chaos.mcp_stdio",
                },
            }
        return msg

    if f.kind == "inject_prompt":
        # Append `suffix` to the first text-bearing field we find in
        # tools/call params. Useful for testing prompt-injection resistance.
        if isinstance(msg, dict) and msg.get("method") == "tools/call":
            args = msg.get("params", {}).get("arguments", {})
            for k, v in list(args.items()):
                if isinstance(v, str):
                    args[k] = v + f.suffix
                    break
        return msg

    if f.kind == "tool_shadow":
        # Inject `extra_tool` into the result of a tools/list response.
        if isinstance(msg, dict) and "result" in msg:
            tools = msg["result"].get("tools")
            if isinstance(tools, list) and f.extra_tool is not None:
                tools.append(dict(f.extra_tool))
        return msg

    if f.kind == "schema_drift":
        if callable(f.drift_fn):
            return f.drift_fn(msg)
        return msg

    if f.kind == "mangle_json":
        # `json_path` = dotted; walk and overwrite. Best-effort — missing
        # keys are no-ops (the rule simply doesn't fire on this message).
        if isinstance(msg, dict) and f.json_path:
            parts = f.json_path.split(".")
            cur: Any = msg
            for p in parts[:-1]:
                if not isinstance(cur, dict) or p not in cur:
                    return msg
                cur = cur[p]
            if isinstance(cur, dict):
                cur[parts[-1]] = f.replacement
        return msg

    return msg


# ── CLI entry point ─────────────────────────────────────────────────────

def _parse_kv(s: str) -> dict:
    """Parse `key=value,other=v2` → `{"key": "value", "other": "v2"}`."""
    out: dict[str, Any] = {}
    for kv in s.split(","):
        if "=" in kv:
            k, v = kv.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m tape.chaos.mcp_stdio",
        description="stdio MCP proxy — wraps an upstream MCP server, "
                    "applies tape.chaos fault rules to JSON-RPC traffic.",
    )
    p.add_argument("--upstream", required=True,
                   help="The real MCP server command to wrap, e.g. "
                        "'uvx mcp-server-time'. Quoted in one argument; "
                        "shlex-split internally.")
    p.add_argument("--delay-ms", type=int, default=0,
                   help="Inject this many ms of latency on each response.")
    p.add_argument("--tool-shadow", default="",
                   help="Inject a synthetic tool into tools/list. "
                        "Format: 'name=foo,description=does X'.")
    p.add_argument("--inject-error", default="",
                   help="Reply with a JSON-RPC error instead of forwarding. "
                        "Format: 'method=tools/call,code=-32603,message=down'.")
    p.add_argument("--drop-prob", type=float, default=0.0,
                   help="Probability of tearing the connection down per response.")
    args = p.parse_args(argv)

    faults: list[ProxyFault] = []
    if args.delay_ms > 0:
        faults.append(ProxyFault(kind="delay", ms=args.delay_ms))
    if args.tool_shadow:
        spec = _parse_kv(args.tool_shadow)
        faults.append(ProxyFault(kind="tool_shadow", extra_tool=spec))
    if args.inject_error:
        spec = _parse_kv(args.inject_error)
        faults.append(ProxyFault(
            kind="inject_status",
            path_prefix=spec.get("method", ""),
            status=int(spec.get("code", "-32000")),
            body=spec.get("message", "injected"),
        ))
    if args.drop_prob > 0:
        faults.append(ProxyFault(
            kind="drop_connection", probability=args.drop_prob))

    upstream_cmd = shlex.split(args.upstream)
    proxy = MCPStdioProxy(upstream_cmd=upstream_cmd, faults=faults)
    try:
        return proxy.run()
    finally:
        proxy.stop()


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["MCPStdioProxy", "main"]
