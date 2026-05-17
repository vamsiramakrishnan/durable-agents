"""`tape deploy gcp ...` — build & deploy Tape server + reactors + agent."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import typer

from ..config import TapeProject, find_project_root
from ..util import console, ok, info, warn, fail, which, run_cmd, die

app = typer.Typer(name="deploy", help="Build & deploy services.")


@app.command("gcp", help="Deploy to GCP (cloud-run / gke / agent-runtime-adapter).")
def gcp(
    target: str = typer.Option("cloud-run", "--target",
        help="cloud-run | gke | agent-runtime-adapter"),
    store: Optional[str] = typer.Option(None, "--store"),
    reactors: Optional[str] = typer.Option(None, "--reactors",
        help="Comma-separated subset: recovery,reconciler,outbox,timers,compensation"),
    image_tag: str = typer.Option("0.1", "--image-tag"),
    skip_build: bool = typer.Option(False, "--skip-build"),
    out: str = typer.Option("deploy/gcp/release", "--out",
        help="Where to write the rendered service spec(s)."),
):
    root = find_project_root()
    project = TapeProject.load(root / "tape.yaml")
    if store:
        project.tape.store.kind = store  # type: ignore[assignment]
    if reactors:
        enabled = set(r.strip() for r in reactors.split(",") if r.strip())
        for r in ("recovery", "reconciler", "outbox", "timers", "compensation"):
            getattr(project.tape.reactors, r).enabled = r in enabled

    out_dir = (root / out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if target == "cloud-run":
        _deploy_cloud_run(root, project, out_dir, image_tag, skip_build)
    elif target == "gke":
        _deploy_gke(root, project, out_dir, image_tag, skip_build)
    elif target == "agent-runtime-adapter":
        _deploy_agent_runtime(root, project, out_dir)
    else:
        die(f"unknown target {target!r}.")


def _find_server_source_root(project_root: Path) -> Optional[Path]:
    """Find a sibling Rust server source if we're in the monorepo layout.

    Looks for `tape/server/Cargo.toml` walking up from the project root. A
    scaffolded standalone project will return None, in which case we consume
    a published `tape-server` image instead of building it.
    """
    cur = project_root.resolve()
    for p in [cur] + list(cur.parents):
        cargo = p / "tape" / "server" / "Cargo.toml"
        if cargo.exists():
            return p
    return None


def _deploy_cloud_run(root: Path, project: TapeProject, out_dir: Path,
                      image_tag: str, skip_build: bool) -> None:
    if not which("gcloud"):
        die("gcloud not installed.", )
    gcp = project.gcp
    if not gcp.project_id:
        die("gcp.project_id is unset.")

    server_source_root = _find_server_source_root(root)
    # Project-local image (the agent + tape-py) is what reactors run.
    reactor_image = (
        f"{gcp.region}-docker.pkg.dev/{gcp.project_id}/"
        f"{gcp.artifact_registry_repository}/{project.project.name}-reactor:{image_tag}"
    )
    # The server image is either the project's pinned image (typical for
    # standalone projects — they consume a published tape-server) OR a
    # project-built one when we can see the Rust source.
    if server_source_root:
        server_image = (
            f"{gcp.region}-docker.pkg.dev/{gcp.project_id}/"
            f"{gcp.artifact_registry_repository}/tape-server:{image_tag}"
        )
    else:
        server_image = project.tape.server.image
        info(f"using published tape-server image {server_image} "
             f"(no Rust source under <root>/../tape/server/Cargo.toml)")

    # 1. Build images.
    if not skip_build:
        if server_source_root:
            ok(f"building tape-server image from {server_source_root}")
            server_ctx = str(server_source_root / "tape")
            rc = run_cmd(["gcloud", "builds", "submit", server_ctx,
                          "--config=/dev/null",  # use Dockerfile autodiscovery
                          "--tag", server_image,
                          "--region", gcp.region],
                         cwd=root, check=False).returncode
            if rc != 0:
                warn("gcloud builds submit failed — falling back to local docker build.")
                run_cmd(["docker", "build", "-f", "server/Dockerfile",
                         "-t", server_image, "."], cwd=server_source_root / "tape")
                run_cmd(["docker", "push", server_image], cwd=server_source_root / "tape")
        ok(f"building reactor image {reactor_image}")
        run_cmd(["gcloud", "builds", "submit", str(root),
                 "--tag", reactor_image, "--region", gcp.region],
                cwd=root, check=False)

    # 2. Write a Cloud Run service spec for the server (only when we own its
    # build; otherwise the user is consuming a published image and shouldn't
    # be redeploying the server from this command).
    if server_source_root:
        server_yaml = _render_cloud_run_server(project, server_image)
        (out_dir / "tape-server.service.yaml").write_text(server_yaml)
        ok(f"wrote {out_dir / 'tape-server.service.yaml'}")

    # 3. Resolve the live server URL for the reactor specs.
    tape_server_url, url_resolved = _resolve_tape_server_url(project)
    if not url_resolved:
        warn(f"could not resolve tape-server URL from gcloud; emitting "
             f"`{tape_server_url}` placeholder in reactor specs.")

    for name in project.tape.reactors.enabled_names():
        spec = _render_cloud_run_reactor(project, reactor_image, name, tape_server_url)
        path = out_dir / f"tape-reactor-{name}.service.yaml"
        path.write_text(spec)
        ok(f"wrote {path}")

    info("")
    info("To apply:")
    if server_source_root:
        info(f"  gcloud run services replace {out_dir / 'tape-server.service.yaml'} "
             f"--region={gcp.region} --project={gcp.project_id}")
    for name in project.tape.reactors.enabled_names():
        path = out_dir / f"tape-reactor-{name}.service.yaml"
        if url_resolved:
            info(f"  gcloud run services replace {path} "
                 f"--region={gcp.region} --project={gcp.project_id}")
        else:
            info(f"  TAPE_SERVER_URL=tapes://<your-tape-server-host> envsubst < {path} | \\")
            info(f"    gcloud run services replace - --region={gcp.region} "
                 f"--project={gcp.project_id}")
    info("")
    info("Or run `tape deploy gcp` with --apply (not implemented; deliberately so you")
    info("see exactly which gcloud invocations will run).")


def _render_cloud_run_server(project: TapeProject, image: str) -> str:
    gcp = project.gcp
    sa = f"{gcp.service_account_prefix}-server@{gcp.project_id}.iam.gserviceaccount.com"
    spec = {
        "apiVersion": "serving.knative.dev/v1",
        "kind": "Service",
        "metadata": {
            "name": "tape-server",
            "annotations": {"run.googleapis.com/ingress": project.tape.server.ingress},
        },
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "run.googleapis.com/use-http2": "true",
                        "autoscaling.knative.dev/minScale": str(project.tape.server.min_instances),
                        "autoscaling.knative.dev/maxScale": str(project.tape.server.max_instances),
                    },
                },
                "spec": {
                    "serviceAccountName": sa,
                    "containers": [{
                        "name": "tape-server",
                        "image": image,
                        "ports": [{"name": "h2c", "containerPort": 7878}],
                        "env": _server_env(project),
                        "resources": {
                            "limits": {"cpu": project.tape.server.cpu,
                                       "memory": project.tape.server.memory},
                        },
                    }],
                },
            },
        },
    }
    import yaml
    return yaml.safe_dump(spec, sort_keys=False)


def _server_env(project: TapeProject) -> list[dict]:
    env: list[dict] = [
        {"name": "TAPE_LISTEN", "value": "0.0.0.0:7878"},
        {"name": "RUST_LOG", "value": "tape_server=info"},
    ]
    if project.tape.store.url_secret:
        env.append({
            "name": "TAPE_STORE",
            "valueFrom": {"secretKeyRef": {"name": project.tape.store.url_secret, "key": "latest"}},
        })
    elif project.tape.store.url:
        env.append({"name": "TAPE_STORE", "value": project.tape.store.url})
    return env


def _resolve_tape_server_url(project: TapeProject) -> tuple[str, bool]:
    """Return `(url, resolved)` for the Tape server.

    Tries `gcloud run services describe tape-server` first; falls back to a
    `${TAPE_SERVER_URL}` placeholder the caller substitutes via envsubst when
    the server isn't deployed yet. `resolved` says whether the URL is real.
    """
    if which("gcloud") and project.gcp.project_id:
        try:
            res = run_cmd(
                ["gcloud", "run", "services", "describe", "tape-server",
                 f"--region={project.gcp.region}",
                 f"--project={project.gcp.project_id}",
                 "--format=value(status.url)"],
                check=False, capture=True,
            )
            url = (res.stdout or "").strip()
            if url.startswith("https://"):
                # tapes:// is the IAM-aware TLS scheme the SDK uses to attach
                # ID tokens for Cloud Run.
                return "tapes://" + url[len("https://"):], True
        except Exception:
            pass
    return "${TAPE_SERVER_URL}", False


def _render_cloud_run_reactor(project: TapeProject, image: str, reactor_name: str,
                              tape_server_url: str) -> str:
    gcp = project.gcp
    sa = f"{gcp.service_account_prefix}-reactor@{gcp.project_id}.iam.gserviceaccount.com"
    cmd = ["python", "-m", "tape.reactors",
           "--runner-from", project.agent.runner_factory or "app.agent:build_runner",
           "--url", tape_server_url,
           "--only", reactor_name]
    spec = {
        "apiVersion": "serving.knative.dev/v1",
        "kind": "Service",
        "metadata": {"name": f"tape-reactor-{reactor_name}"},
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "autoscaling.knative.dev/minScale": str(getattr(project.tape.reactors, reactor_name).min_instances),
                        "autoscaling.knative.dev/maxScale": str(getattr(project.tape.reactors, reactor_name).max_instances),
                    },
                },
                "spec": {
                    "serviceAccountName": sa,
                    "containers": [{
                        "name": f"tape-reactor-{reactor_name}",
                        "image": image,
                        "command": cmd,
                        "env": [
                            {"name": "TAPE_URL", "value": tape_server_url},
                            {"name": "TAPE_REACTOR", "value": reactor_name},
                            {"name": "GOOGLE_CLOUD_PROJECT", "value": project.gcp.project_id},
                            {"name": "GOOGLE_CLOUD_LOCATION", "value": project.gcp.region},
                        ],
                    }],
                },
            },
        },
    }
    import yaml
    return yaml.safe_dump(spec, sort_keys=False)


def _deploy_gke(root: Path, project: TapeProject, out_dir: Path,
                image_tag: str, skip_build: bool) -> None:
    info("Rendering Helm values for the bundled tape chart.")
    chart_dir = Path(__file__).resolve().parent.parent.parent.parent / "deploy" / "gcp" / "k8s" / "chart" / "tape"
    if not chart_dir.exists():
        die(f"bundled chart not found at {chart_dir}.")
    gcp = project.gcp
    values = {
        "image": {
            "server": f"{gcp.region}-docker.pkg.dev/{gcp.project_id}/{gcp.artifact_registry_repository}/tape-server:{image_tag}",
            "reactor": f"{gcp.region}-docker.pkg.dev/{gcp.project_id}/{gcp.artifact_registry_repository}/tape-reactor:{image_tag}",
        },
        "store": {
            "kind": project.tape.store.kind,
            "urlSecret": project.tape.store.url_secret or "",
            "url": project.tape.store.url or "",
        },
        "reactors": {name: getattr(project.tape.reactors, name).enabled
                     for name in ("recovery", "reconciler", "outbox", "timers", "compensation")},
        "server": project.tape.server.model_dump(),
        "tenancy": project.tenancy.model_dump(),
    }
    import yaml
    (out_dir / "values.generated.yaml").write_text(yaml.safe_dump(values, sort_keys=False))
    ok(f"wrote {out_dir / 'values.generated.yaml'}")
    info("")
    info("To install (after pushing images):")
    info(f"  helm upgrade --install tape {chart_dir} \\")
    info(f"    --namespace tape --create-namespace \\")
    info(f"    -f {out_dir / 'values.generated.yaml'}")


def _deploy_agent_runtime(root: Path, project: TapeProject, out_dir: Path) -> None:
    template = """\"\"\"Adapter: re-drive a Vertex AI Agent Runtime deployment from a Tape reactor.

Replace AGENT_RESOURCE with `projects/.../locations/.../reasoningEngines/...`
and deploy this module's `main` as the reactor entrypoint.
\"\"\"

import os
import vertexai
from vertexai import agent_engines

import tape
import tape.reactors

# importing the agent module must register @tape.effect(status_check=...) hooks
import {entrypoint_module}  # noqa: F401

PROJECT  = os.environ["GOOGLE_CLOUD_PROJECT"]
LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-central1")
TAPE_URL = os.environ["TAPE_URL"]
AGENT_RESOURCE = os.environ["AGENT_ENGINE"]

vertexai.init(project=PROJECT, location=LOCATION)
_AGENT = agent_engines.get(AGENT_RESOURCE)


def agent_engine_redrive(run) -> None:
    for _ in _AGENT.stream_query(user_id=run.user_id, session_id=run.session_id,
                                 invocation_id=run.invocation_id, message=None):
        pass


def main() -> None:
    tape.reactors.run_reactors(redrive_fn=agent_engine_redrive, url=TAPE_URL)


if __name__ == "__main__":
    main()
"""
    entrypoint_mod = project.agent.entrypoint.split(":")[0]
    (out_dir / "agent_runtime_reactor.py").write_text(template.format(entrypoint_module=entrypoint_mod))
    ok(f"wrote {out_dir / 'agent_runtime_reactor.py'}")
    info("")
    info("Deploy it as a Cloud Run service (min-instances=1) with these env vars:")
    info("  GOOGLE_CLOUD_PROJECT, GOOGLE_CLOUD_LOCATION,")
    info("  TAPE_URL=tapes://<tape-server-url>,")
    info("  AGENT_ENGINE=projects/.../locations/.../reasoningEngines/RESOURCE_ID")
