"""`tape doctor` — local + GCP checks. Each check yields a small dict the
caller can format; the runner prints the standard tick/cross format.
"""

from __future__ import annotations

import importlib
import os
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import typer

from ..config import TapeProject, find_project_root
from ..util import console, ok, warn, fail, info, which, section


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str = ""
    hint: str = ""


def _check(name: str):
    def deco(fn):
        fn._check_name = name
        return fn
    return deco


# ── local checks ────────────────────────────────────────────────────────────

@_check("Python ≥ 3.10")
def _check_python() -> CheckResult:
    py = sys.version_info
    if py >= (3, 10):
        return CheckResult("Python", True, f"{py.major}.{py.minor}.{py.micro}")
    return CheckResult("Python", False, f"{py.major}.{py.minor}.{py.micro}",
                       hint="install Python 3.10+ (3.12 recommended).")


@_check("ADK importable")
def _check_adk() -> CheckResult:
    try:
        import google.adk  # noqa: F401
        return CheckResult("ADK", True)
    except Exception as ex:
        return CheckResult("ADK", False, str(ex), hint="pip install google-adk>=1.30")


@_check("Tape SDK importable")
def _check_tape() -> CheckResult:
    try:
        import tape  # noqa: F401
        from tape.adk import TapePlugin, TapeSessionService, durable_app  # noqa: F401
        return CheckResult("Tape SDK", True)
    except Exception as ex:
        return CheckResult("Tape SDK", False, str(ex), hint="pip install tape-py")


@_check("Docker available")
def _check_docker() -> CheckResult:
    p = which("docker")
    if p:
        return CheckResult("Docker", True, p)
    return CheckResult("Docker", False, hint="install Docker if you want `tape dev` to use compose.")


@_check("Cargo available (for building the server)")
def _check_cargo() -> CheckResult:
    p = which("cargo")
    if p:
        return CheckResult("Cargo", True, p)
    return CheckResult("Cargo", False, hint="optional — only needed to build tape-server from source.")


def _check_tape_server(project: TapeProject) -> CheckResult:
    url = project.tape.url.replace("tape://", "").replace("tapes://", "").replace("grpc://", "")
    host, _, port = url.partition(":")
    port = port.split("/")[0] or ("443" if "tapes://" in project.tape.url else "7878")
    try:
        with socket.create_connection((host, int(port)), timeout=2.0):
            return CheckResult(f"Tape server {project.tape.url}", True)
    except Exception as ex:
        return CheckResult(f"Tape server {project.tape.url}", False, str(ex),
                           hint="`tape dev` to start one locally, or `tape deploy gcp` to ship it.")


def _check_env_var(name: str, required: bool) -> CheckResult:
    val = os.environ.get(name)
    if val:
        return CheckResult(name, True, f"set to {val[:20]}{'…' if len(val) > 20 else ''}")
    return CheckResult(name, not required, "unset",
                       hint=f"export {name}=... in your environment or .env")


# ── GCP checks ──────────────────────────────────────────────────────────────

def _check_gcloud_auth() -> CheckResult:
    if not which("gcloud"):
        return CheckResult("gcloud installed", False, hint="https://cloud.google.com/sdk/docs/install")
    return CheckResult("gcloud installed", True, which("gcloud") or "")


def _check_adc() -> CheckResult:
    try:
        import google.auth
        creds, project = google.auth.default()
        return CheckResult("Application Default Credentials", True,
                           f"project={project}, type={type(creds).__name__}")
    except Exception as ex:
        return CheckResult("Application Default Credentials", False, str(ex),
                           hint="gcloud auth application-default login")


def _check_gcp_apis(project_id: str, apis: list[str]) -> list[CheckResult]:
    if not project_id:
        return [CheckResult("GCP APIs", False, "no project_id set",
                            hint="set gcp.project_id in tape.yaml or GOOGLE_CLOUD_PROJECT")]
    try:
        from google.cloud import serviceusage_v1
        client = serviceusage_v1.ServiceUsageClient()
    except Exception as ex:
        return [CheckResult("GCP APIs", False, str(ex),
                            hint="pip install google-cloud-service-usage, or skip this check.")]
    out: list[CheckResult] = []
    for api in apis:
        try:
            name = f"projects/{project_id}/services/{api}"
            svc = client.get_service(name=name)
            enabled = str(svc.state) == "State.ENABLED" or "ENABLED" in str(svc.state)
            out.append(CheckResult(f"API {api}", enabled,
                                   hint=f"gcloud services enable {api}" if not enabled else ""))
        except Exception as ex:
            out.append(CheckResult(f"API {api}", False, str(ex),
                                   hint=f"gcloud services enable {api}"))
    return out


def _print(results: list[CheckResult]) -> int:
    n_fail = 0
    for r in results:
        if r.ok:
            ok(f"{r.name}" + (f"  [dim]({r.detail})[/dim]" if r.detail else ""))
        else:
            n_fail += 1
            fail(r.name + (f"  [dim]({r.detail})[/dim]" if r.detail else ""), hint=r.hint or None)
    return n_fail


# ── entry point ────────────────────────────────────────────────────────────

def run(
    local: bool = typer.Option(True, "--local/--no-local"),
    gcp: bool = typer.Option(False, "--gcp/--no-gcp"),
    agents_cli_aware: bool = typer.Option(False, "--agents-cli-aware",
        help="Also run agents-cli scaffold compatibility checks."),
):
    project: Optional[TapeProject] = None
    try:
        root = find_project_root()
        project = TapeProject.load(root / "tape.yaml")
        ok(f"Tape project detected — {root}")
    except FileNotFoundError:
        warn("no tape.yaml found; running standalone checks.")

    fails = 0
    if local:
        section("Local")
        local_results = [
            _check_python(),
            _check_adk(),
            _check_tape(),
            _check_docker(),
            _check_cargo(),
        ]
        if project:
            local_results.append(_check_tape_server(project))
            local_results.append(_check_env_var("TAPE_URL", required=False))
            if project.tape.store.kind != "sqlite":
                local_results.append(_check_env_var("TAPE_STORE", required=False))
        fails += _print(local_results)

    if gcp:
        section("GCP")
        gcp_results: list[CheckResult] = [_check_gcloud_auth(), _check_adc()]

        project_id = (project.gcp.project_id if project else "") or os.environ.get("GOOGLE_CLOUD_PROJECT", "")
        if project_id:
            ok(f"project_id = {project_id}")

        apis = ["run.googleapis.com", "artifactregistry.googleapis.com", "secretmanager.googleapis.com"]
        if project and project.tape.events.kind == "pubsub":
            apis.append("pubsub.googleapis.com")
        if project and project.tape.store.kind == "alloydb":
            apis.append("alloydb.googleapis.com")
        if project and project.tape.store.kind == "bigtable":
            apis.append("bigtable.googleapis.com")
        if project and project.tape.store.kind == "spanner":
            apis.append("spanner.googleapis.com")
        gcp_results.extend(_check_gcp_apis(project_id, apis))
        fails += _print(gcp_results)

    if project and agents_cli_aware:
        section("agents-cli compatibility (advisory)")
        adv = []
        if (Path.cwd() / "agents.yaml").exists():
            adv.append(CheckResult("agents.yaml present", True,
                                   "Tape will live alongside your agents-cli scaffold."))
        else:
            adv.append(CheckResult("agents.yaml present", False,
                                   "this is not an agents-cli project — that's fine."))
        fails += _print(adv)

    if project and project.tenancy.mode == "hard_multi_tenant":
        section("Tenancy")
        from tape.tenancy import TenancyConfig
        tc = TenancyConfig(mode=project.tenancy.mode, tenant_id=project.tenancy.tenant_id)  # type: ignore[arg-type]
        for w in tc.warn_if_hard_but_unenforced():
            warn(w)

    info("")
    if fails:
        fail(f"{fails} check(s) failed.")
        raise typer.Exit(1)
    ok("all checks passed.")
