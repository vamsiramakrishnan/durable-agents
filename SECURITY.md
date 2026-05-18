# Security Policy

## Supported versions

Tape is pre-1.0 and ships from `main`. Security fixes land there and roll into
the next minor release. Older `0.x` tags are not patched.

| Version  | Supported          |
|----------|--------------------|
| `main`   | ✅                 |
| `0.x.y`  | only the latest    |

## Reporting a vulnerability

**Do not open a public issue.** Email the maintainer at the address listed on
the repo's GitHub profile, or use [GitHub's private vulnerability
reporting](https://github.com/vamsiramakrishnan/durable-agents/security/advisories/new).

Please include:

- A description of the issue and the impact (what an attacker can do).
- The smallest possible reproducer — code, config, the exact commit hash.
- Whether you've already shared the finding with anyone else.

You'll get an acknowledgement within **72 hours** and a triage decision within
**7 days**. If we accept the report, we'll work with you on a fix and a
coordinated disclosure window (typically 30 days).

## Scope

In scope:

- The Tape server (`tape/server/`) — gRPC handlers, store backends, the
  per-run lease, the budget admit/charge path.
- The SDKs (`tape/sdk/{python,typescript,go,java}/`) — including the outbox
  reactor's `non_idempotent` safety contract.
- The CLI (`tape/cli/`) — including any GCP-touching subcommand.
- The published binary and language packages (when those exist).

Out of scope:

- The treatise and design pages (`design-principles/`).
- Anything in `tape/examples/` — these are illustrative, not production.
- Dependency vulnerabilities without a Tape-side impact path (file an issue
  instead).

## Hardening guidance

A few defaults worth knowing if you're operating Tape:

- Use `tapes://` (TLS) for any cross-host link. Plaintext `tape://` is only
  appropriate for localhost or a strictly-internal VPC with mTLS at the mesh
  layer.
- The Bigtable backend requires explicit table creation (`server/src/store/bigtable.rs`
  documents the cell layout); a misconfigured GC policy can permit a stale
  read to override a fresh write.
- The Python CLI's `tape provision gcp` and `tape deploy gcp` shell out to
  `gcloud` / `terraform` with the active credentials. Audit the rendered
  Terraform plan before applying in production.
