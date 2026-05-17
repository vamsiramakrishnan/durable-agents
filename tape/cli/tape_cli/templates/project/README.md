# {{ name }}

A durable ADK agent on **Tape**.

## Local

```bash
pip install -e .
tape dev          # tape-server + reactors + agent
tape doctor       # diagnose your setup
```

## GCP

```bash
tape provision gcp --dry-run         # render Terraform; review it
tape provision gcp --apply           # apply
tape deploy gcp --target cloud-run   # render Cloud Run service specs
tape doctor --gcp                    # diagnose your cloud setup
```

## Layout

```
{{ name }}/
  tape.yaml                    project-level config
  pyproject.toml               your Python package
  app/
    __init__.py
    agent.py                   the ADK agent + tools + durable_app(...)
    tools.py                   tool bodies (effects + outbox tools)
    connectors.py              capability connector registrations
  deploy/gcp/                  GCP-specific overlays (Terraform, Cloud Run)
  docker-compose.yaml          local dev stack
  Dockerfile                   build a container image for the agent
```
