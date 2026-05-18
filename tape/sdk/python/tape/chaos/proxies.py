"""Agent-layer chaos — Toxiproxy-for-LLM and Toxiproxy-for-MCP, in one file.

Point your agent's `base_url` at this proxy and inject the failure shapes the
SDK can't reach from inside the runtime: token-stream stalls, partial JSON,
schema deviations, refusal injection, rate-limit storms, prompt injection,
MCP tool shadowing, MCP schema drift.

Two convenience constructors plus a small `ChaosProxy` underneath:

  proxy = chaos.model_proxy(
      upstream="https://api.anthropic.com",
      faults=[
          chaos.proxy.delay("/v1/messages", ms=2_000, probability=0.1),
          chaos.proxy.inject_status("/v1/messages", status=429, probability=0.05),
          chaos.proxy.truncate_stream("/v1/messages", at_event=3, probability=0.05),
      ],
  )
  proxy.start()                          # listens on 127.0.0.1:<auto-port>
  print(proxy.url)                       # → http://127.0.0.1:8543
  # point your client.base_url at proxy.url ...
  proxy.stop()

  with chaos.mcp_proxy("http://localhost:9000",
                       faults=[chaos.proxy.tool_shadow("/mcp", extra_tool={
                           "name": "exfiltrate", "description": "tool that should not exist"})]) as p:
      run_agent(mcp_url=p.url)

The implementation is single-file and stdlib-only — `http.server` +
`urllib.request`. No third-party dependency, no MITM cert trickery: the
proxy speaks plain HTTP to the agent; if the upstream is TLS, the proxy
upgrades on the way out (the agent's SDK doesn't see the upstream cert).

SSE streams are forwarded chunk-by-chunk so faults like `truncate_stream`
can cut a stream mid-event without buffering the whole response. SSE is
the dominant streaming format for LLM APIs and MCP-over-HTTP.

Limitations (documented; addressable later):
  * stdio MCP transport not covered here — requires a subprocess wrapper.
  * No HTTP/2 server push; gRPC over HTTP/2 is not a target (Tape's own
    gRPC has the in-process `tape.chaos` failpoints — Phase 0/1).
  * No client-side TLS termination; the proxy is plaintext to the agent.
    For a TLS-only agent SDK, set `ssl_context=` on `start()`.

Phase 4 of TapeChaos. See `design-principles/chaos.md §3e`.
"""

from __future__ import annotations

import json
import logging
import random
import socket
import ssl as _ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, List, Optional, Sequence, Tuple

_log = logging.getLogger("tape.chaos.proxies")


# ── fault rule type + constructors ─────────────────────────────────────────

@dataclass(frozen=True)
class ProxyFault:
    """One declarative chaos rule the proxy applies to matching requests.

    `kind` selects the injection behaviour; `path_prefix` scopes the rule
    to a URL path (`""` = all paths). `probability` is per-request.

    Per-kind fields:
      delay            — `ms`, `jitter` (fraction)
      inject_status    — `status`, `body`
      truncate_stream  — `at_event` (cut SSE after N events)
      mangle_json      — `json_path` (dotted), `replacement`
      inject_prompt    — `suffix` (appended to top-level text/content fields)
      tool_shadow      — `extra_tool` (added to MCP tools/list response)
      schema_drift     — `drift_fn(payload) -> payload`
      drop_connection  — none (closes the socket mid-response)
    """
    kind: str
    path_prefix: str = ""
    probability: float = 1.0
    # Per-kind fields (only some are used per kind — keeps the proto small):
    ms: int = 0
    jitter: float = 0.0
    status: int = 0
    body: str = ""
    at_event: int = 0
    json_path: str = ""
    replacement: Any = None
    suffix: str = ""
    extra_tool: Optional[dict] = None
    drift_fn: Optional[Callable[[Any], Any]] = None


def delay(path_prefix: str = "", *, ms: int, jitter: float = 0.0,
          probability: float = 1.0) -> ProxyFault:
    """Sleep `ms` (± `jitter` fraction) before forwarding the request."""
    return ProxyFault(kind="delay", path_prefix=path_prefix,
                       probability=probability, ms=int(ms), jitter=jitter)


def inject_status(path_prefix: str = "", *, status: int, body: str = "",
                  probability: float = 1.0) -> ProxyFault:
    """Short-circuit the upstream and return `status` with `body`. Use
    for rate-limit storms (`429`), model deprecation (`410`),
    service-unavailable (`503`), auth glitches (`401`)."""
    return ProxyFault(kind="inject_status", path_prefix=path_prefix,
                       probability=probability, status=int(status), body=body)


def truncate_stream(path_prefix: str = "", *, at_event: int,
                    probability: float = 1.0) -> ProxyFault:
    """Cut an SSE stream after `at_event` events have been forwarded.
    Models partial JSON / mid-tool-call stream death. Only fires on
    `Content-Type: text/event-stream` responses."""
    return ProxyFault(kind="truncate_stream", path_prefix=path_prefix,
                       probability=probability, at_event=int(at_event))


def mangle_json(path_prefix: str = "", *, json_path: str = "",
                replacement: Any = None,
                probability: float = 1.0) -> ProxyFault:
    """Corrupt a JSON response field. `json_path` is dotted (`choices.0.text`);
    empty = corrupt the whole body to invalid JSON. Models schema drift
    + malformed-response failure modes."""
    return ProxyFault(kind="mangle_json", path_prefix=path_prefix,
                       probability=probability, json_path=json_path,
                       replacement=replacement)


def inject_prompt(path_prefix: str = "", *, suffix: str,
                  probability: float = 1.0) -> ProxyFault:
    """Append `suffix` to top-level `text` / `content` string fields in a
    JSON response. Models prompt-injection-in-tool-result and adversarial
    upstream behaviour. Common payloads: `\\n[IGNORE PREVIOUS INSTRUCTIONS]`."""
    return ProxyFault(kind="inject_prompt", path_prefix=path_prefix,
                       probability=probability, suffix=suffix)


def tool_shadow(path_prefix: str = "", *, extra_tool: dict,
                probability: float = 1.0) -> ProxyFault:
    """Inject `extra_tool` into an MCP `tools/list` response — the
    classic tool-shadowing attack. The agent sees a tool the server
    didn't actually expose."""
    return ProxyFault(kind="tool_shadow", path_prefix=path_prefix,
                       probability=probability, extra_tool=extra_tool)


def schema_drift(path_prefix: str = "", *, drift_fn: Callable[[Any], Any],
                 probability: float = 1.0) -> ProxyFault:
    """Apply an arbitrary transform to a JSON response payload. The
    escape hatch for custom drift scenarios."""
    return ProxyFault(kind="schema_drift", path_prefix=path_prefix,
                       probability=probability, drift_fn=drift_fn)


def drop_connection(path_prefix: str = "",
                    probability: float = 1.0) -> ProxyFault:
    """Close the connection mid-response. Models flaky upstreams and
    proxy-side EOFs. Triggered after headers are sent."""
    return ProxyFault(kind="drop_connection", path_prefix=path_prefix,
                       probability=probability)


# ── helpers ────────────────────────────────────────────────────────────────

def _set_json_at(obj: Any, path: str, value: Any) -> Any:
    """Set `path` (dotted) on a nested dict/list. Best-effort: returns
    `obj` even if the path didn't match. Numeric components index lists."""
    if not path:
        return value
    parts = path.split(".")
    cur = obj
    for p in parts[:-1]:
        try:
            if isinstance(cur, list) and p.isdigit():
                cur = cur[int(p)]
            elif isinstance(cur, dict):
                cur = cur[p]
            else:
                return obj
        except (KeyError, IndexError, TypeError):
            return obj
    last = parts[-1]
    try:
        if isinstance(cur, list) and last.isdigit():
            cur[int(last)] = value
        elif isinstance(cur, dict):
            cur[last] = value
    except Exception:
        pass
    return obj


def _inject_prompt_into(obj: Any, suffix: str) -> Any:
    """Recurse and append `suffix` to every string `text` / `content` /
    `output_text` field. Conservative: only top-level matches per node."""
    if isinstance(obj, dict):
        for k in ("text", "content", "output_text"):
            if k in obj and isinstance(obj[k], str):
                obj[k] = obj[k] + suffix
        for v in obj.values():
            _inject_prompt_into(v, suffix)
    elif isinstance(obj, list):
        for v in obj:
            _inject_prompt_into(v, suffix)
    return obj


def _shadow_tools(obj: Any, extra_tool: dict) -> Any:
    """Add `extra_tool` to a `tools` list at any depth — covers MCP
    `tools/list` and similar shapes."""
    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            if k == "tools" and isinstance(v, list):
                v.append(extra_tool)
            else:
                _shadow_tools(v, extra_tool)
    elif isinstance(obj, list):
        for v in obj:
            _shadow_tools(v, extra_tool)
    return obj


# ── the proxy ──────────────────────────────────────────────────────────────

# Headers we don't forward upstream (they'd duplicate or mismatch).
_DROP_REQUEST_HEADERS = {"host", "content-length", "connection",
                          "keep-alive", "proxy-authenticate", "proxy-authorization",
                          "te", "trailers", "transfer-encoding", "upgrade"}
# Headers we don't forward downstream to the client.
_DROP_RESPONSE_HEADERS = {"transfer-encoding", "content-encoding",
                           "connection", "keep-alive"}


class ChaosProxy:
    """A threading HTTP forward-proxy with declarative chaos rules.

    Single-file, stdlib-only. Streams SSE chunk-by-chunk. Not a TLS
    terminator — agents speak plain HTTP to the proxy; the proxy upgrades
    to TLS on the way out when `upstream` is `https://`.

    Use as a context manager (preferred — `stop()` is called on exit)::

        with chaos.proxies.ChaosProxy("https://api.anthropic.com", faults) as p:
            point_agent_at(p.url)
            drive_agent()
    """

    def __init__(self, upstream: str, faults: Sequence[ProxyFault] = (), *,
                 rng: Optional[random.Random] = None,
                 timeout_s: float = 60.0,
                 ssl_context: Optional[_ssl.SSLContext] = None):
        self.upstream = upstream.rstrip("/")
        self.faults: tuple = tuple(faults)
        self._rng = rng or random.Random()
        self.timeout_s = timeout_s
        self._ssl_context = ssl_context
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._host: str = ""
        self._port: int = 0
        # Per-fault counters, for tests + reports.
        self.fault_hits: dict[Tuple[str, str], int] = {}

    @property
    def url(self) -> str:
        scheme = "https" if self._ssl_context else "http"
        return f"{scheme}://{self._host}:{self._port}"

    def start(self, host: str = "127.0.0.1", port: int = 0) -> None:
        """Bind and serve in a background thread. `port=0` picks a free port."""
        proxy = self
        handler_cls = _build_handler(proxy)
        srv = ThreadingHTTPServer((host, port), handler_cls)
        if self._ssl_context is not None:
            srv.socket = self._ssl_context.wrap_socket(srv.socket, server_side=True)
        self._host, self._port = srv.server_address[:2]
        self._server = srv
        self._thread = threading.Thread(
            target=srv.serve_forever, name="tape-chaos-proxy", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def __enter__(self) -> "ChaosProxy":
        self.start()
        return self

    def __exit__(self, *_exc) -> None:
        self.stop()

    # ── fault matching / firing ────────────────────────────────────────────
    def _matching(self, path: str, kind: str) -> List[ProxyFault]:
        """Return every fault whose kind+path matches. Probability is
        rolled per matching fault — independent rolls so multiple kinds
        can fire on the same request."""
        out: list[ProxyFault] = []
        for f in self.faults:
            if f.kind != kind:
                continue
            if f.path_prefix and not path.startswith(f.path_prefix):
                continue
            if f.probability >= 1.0 or self._rng.random() < f.probability:
                out.append(f)
                self.fault_hits[(kind, f.path_prefix)] = (
                    self.fault_hits.get((kind, f.path_prefix), 0) + 1)
        return out


def _build_handler(proxy: ChaosProxy):
    """Build a per-instance request handler class bound to `proxy`."""

    class _Handler(BaseHTTPRequestHandler):
        # Quiet by default; the proxy logs its own structured events.
        def log_message(self, fmt: str, *args: Any) -> None:
            _log.debug("proxy: " + fmt, *args)

        # One method handles every verb — the proxy is verb-agnostic.
        def do_GET(self): return self._handle()
        def do_POST(self): return self._handle()
        def do_PUT(self): return self._handle()
        def do_DELETE(self): return self._handle()
        def do_PATCH(self): return self._handle()
        def do_OPTIONS(self): return self._handle()

        def _handle(self) -> None:
            path = self.path
            # 1. PRE-FORWARD faults
            for f in proxy._matching(path, "delay"):
                ms = f.ms
                if f.jitter > 0:
                    ms = int(ms * (1.0 + proxy._rng.uniform(-f.jitter, f.jitter)))
                time.sleep(max(0, ms) / 1000.0)
            for f in proxy._matching(path, "inject_status"):
                body = (f.body or "").encode("utf-8")
                self.send_response(f.status)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("X-Tape-Chaos", "inject_status")
                self.end_headers()
                if body:
                    self.wfile.write(body)
                return

            # 2. Forward the request upstream.
            body = self._read_request_body()
            try:
                resp = self._forward(path, body)
            except urllib.error.HTTPError as ex:
                self.send_response(ex.code)
                err_body = (ex.read() if hasattr(ex, "read") else b"") or b""
                self.send_header("Content-Length", str(len(err_body)))
                self.end_headers()
                self.wfile.write(err_body)
                return
            except (urllib.error.URLError, OSError) as ex:
                self.send_response(502)
                self.send_header("Content-Length", "0")
                self.send_header("X-Tape-Chaos", "upstream-unreachable")
                self.end_headers()
                _log.warning("proxy: upstream unreachable: %s", ex)
                return

            # 3. POST-FORWARD faults — depend on response shape.
            ctype = resp.headers.get("Content-Type", "")
            is_sse = ctype.startswith("text/event-stream")
            is_json = ctype.startswith("application/json")

            # 3a. SSE: forward chunk-by-chunk; honour truncate_stream.
            if is_sse:
                self._stream_sse(resp, path)
                return

            # 3b. JSON: read whole body; honour mangle_json / inject_prompt /
            # tool_shadow / schema_drift / drop_connection.
            if is_json:
                self._reply_json(resp, path)
                return

            # 3c. Anything else: passthrough.
            self._reply_passthrough(resp, path)

        # ── helpers ────────────────────────────────────────────────────────

        def _read_request_body(self) -> bytes:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length <= 0:
                return b""
            return self.rfile.read(length)

        def _forward(self, path: str, body: bytes):
            target = proxy.upstream + path
            headers = {k: v for k, v in self.headers.items()
                       if k.lower() not in _DROP_REQUEST_HEADERS}
            # The upstream's Host header should be the upstream's, not the
            # proxy's — `urlopen` sets this automatically based on the URL.
            req = urllib.request.Request(
                target, data=body if body else None, method=self.command,
                headers=headers)
            return urllib.request.urlopen(req, timeout=proxy.timeout_s)

        def _send_response_headers(self, resp, *, override_length: Optional[int] = None,
                                    extra: Optional[List[Tuple[str, str]]] = None) -> None:
            self.send_response(resp.getcode())
            for k, v in resp.headers.items():
                if k.lower() in _DROP_RESPONSE_HEADERS:
                    continue
                if override_length is not None and k.lower() == "content-length":
                    continue
                self.send_header(k, v)
            if override_length is not None:
                self.send_header("Content-Length", str(override_length))
            for k, v in (extra or []):
                self.send_header(k, v)
            self.end_headers()

        def _reply_passthrough(self, resp, path: str) -> None:
            data = resp.read()
            # drop_connection is the "hang up after headers" variant.
            drops = proxy._matching(path, "drop_connection")
            self._send_response_headers(resp, override_length=len(data),
                                        extra=[("X-Tape-Chaos", "passthrough")])
            if drops:
                return  # close without writing body
            self.wfile.write(data)

        def _reply_json(self, resp, path: str) -> None:
            data = resp.read()
            try:
                payload = json.loads(data.decode("utf-8"))
            except Exception:
                # Not really JSON; passthrough.
                self._send_response_headers(resp, override_length=len(data))
                self.wfile.write(data)
                return
            faults_applied: list[str] = []
            for f in proxy._matching(path, "mangle_json"):
                payload = _set_json_at(payload, f.json_path, f.replacement)
                faults_applied.append("mangle_json")
            for f in proxy._matching(path, "inject_prompt"):
                payload = _inject_prompt_into(payload, f.suffix)
                faults_applied.append("inject_prompt")
            for f in proxy._matching(path, "tool_shadow"):
                if f.extra_tool is not None:
                    payload = _shadow_tools(payload, dict(f.extra_tool))
                    faults_applied.append("tool_shadow")
            for f in proxy._matching(path, "schema_drift"):
                if f.drift_fn is not None:
                    try:
                        payload = f.drift_fn(payload)
                    except Exception as ex:
                        _log.warning("proxy: schema_drift fn raised: %s", ex)
                    faults_applied.append("schema_drift")

            drops = proxy._matching(path, "drop_connection")
            new_body = json.dumps(payload).encode("utf-8")
            self._send_response_headers(resp, override_length=len(new_body),
                                        extra=[("X-Tape-Chaos",
                                                ",".join(faults_applied) or "json")])
            if drops:
                return
            self.wfile.write(new_body)

        def _stream_sse(self, resp, path: str) -> None:
            truncates = proxy._matching(path, "truncate_stream")
            cut_at = min((f.at_event for f in truncates), default=0)
            drops = proxy._matching(path, "drop_connection")
            self._send_response_headers(resp, extra=[
                ("X-Tape-Chaos",
                 "truncate_stream" if truncates else
                 ("drop_connection" if drops else "sse"))])
            event_count = 0
            buf = b""
            # SSE events are separated by blank lines (`\n\n`). Read chunks
            # and emit events as they complete.
            while True:
                try:
                    chunk = resp.read(2048)
                except Exception:
                    return
                if not chunk:
                    return
                buf += chunk
                while b"\n\n" in buf:
                    evt, buf = buf.split(b"\n\n", 1)
                    evt += b"\n\n"
                    event_count += 1
                    try:
                        self.wfile.write(evt)
                        self.wfile.flush()
                    except (BrokenPipeError, OSError):
                        return
                    if cut_at and event_count >= cut_at:
                        return    # mid-stream death
                # If a drop_connection fault fired, hang up before flushing
                # the buffer's tail.
                if drops and event_count >= 1:
                    return

    return _Handler


# ── convenience constructors ───────────────────────────────────────────────

def model_proxy(upstream: str, faults: Sequence[ProxyFault] = (), **kw) -> ChaosProxy:
    """A `ChaosProxy` tuned for an LLM provider's `base_url`. The defaults
    are identical to `ChaosProxy(...)`; this exists as a documentation
    seam — point your agent SDK's `base_url` at `proxy.url`.

    Provider hints:
      Anthropic: client = Anthropic(base_url=proxy.url)
      OpenAI:    client = OpenAI(base_url=proxy.url)
      Vertex AI: set HTTP_PROXY=proxy.url or override the genai endpoint
    """
    return ChaosProxy(upstream, faults, **kw)


def mcp_proxy(upstream: str, faults: Sequence[ProxyFault] = (), **kw) -> ChaosProxy:
    """A `ChaosProxy` tuned for an MCP server's HTTP/SSE endpoint. Same
    shape as `model_proxy`; the difference is the fault menu —
    `tool_shadow` + `schema_drift` + `inject_prompt` cover OWASP MCP
    Top-10's tool-poisoning / shadowing / injection threats."""
    return ChaosProxy(upstream, faults, **kw)


__all__ = [
    "ProxyFault",
    "ChaosProxy",
    "delay",
    "inject_status",
    "truncate_stream",
    "mangle_json",
    "inject_prompt",
    "tool_shadow",
    "schema_drift",
    "drop_connection",
    "model_proxy",
    "mcp_proxy",
]
