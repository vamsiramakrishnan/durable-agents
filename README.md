# Durable Agents — Tape

[![sdk-tests](https://github.com/vamsiramakrishnan/durable-agents/actions/workflows/sdk-tests.yml/badge.svg)](https://github.com/vamsiramakrishnan/durable-agents/actions/workflows/sdk-tests.yml)
[![docs](https://github.com/vamsiramakrishnan/durable-agents/actions/workflows/docs.yml/badge.svg)](https://vamsiramakrishnan.github.io/durable-agents/)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/vamsiramakrishnan/durable-agents)

Tape is a durable execution runtime for agents that call external systems.

It records model decisions and effect state in an append-only journal. On resume, confirmed effects are not reissued. Ambiguous effects stop the run until the runtime can reconcile what happened.

Full documentation: <https://vamsiramakrishnan.github.io/durable-agents/>

## Quickstart

```bash
git clone https://github.com/vamsiramakrishnan/durable-agents
cd durable-agents
./setup.sh
make doctor
make demo
make demo-resume
```

`make demo-resume` kills the treasury example during an external effect, restarts it, and exercises the recovery path.

Run the same small scenario through every SDK:

```bash
make quickstart-all
```

The repository currently includes Python, TypeScript, Go, and Java SDKs.

When release artifacts are available, install the binary and Python CLI with:

```bash
curl -fsSL https://raw.githubusercontent.com/vamsiramakrishnan/durable-agents/main/install.sh | sh
```

Then:

```bash
tape init my-agent
tape dev
tape doctor
```

`make help` lists development tasks. [`SDK_PARITY.md`](SDK_PARITY.md) tracks SDK coverage. [`examples/`](examples/) contains the cross-language example. [`tape/README.md`](tape/README.md) describes the runtime itself.

## Execution model

An acting agent crosses a side-effect boundary whenever it calls an external system. A process crash can occur before the call, during the call, after the remote system commits it, or after the local process records the result.

Retrying the whole agent does not distinguish those cases.

Tape records enough state to make the distinction explicit:

- model decisions;
- intended external effects;
- confirmed effect results;
- unknown effect outcomes;
- gates and timers;
- budgets;
- compensations;
- replay state.

The journal is append-only. Projections derive the current view of the run:

```text
append-only execution journal
        │
        ├── decisions
        ├── effects
        ├── obligations
        ├── timers
        ├── gates
        ├── budgets
        └── reactive KV
```

On replay, Tape reuses recorded decisions and confirmed effects instead of issuing those effects again. An effect whose outcome is unknown remains `UNKNOWN` until a reconciliation step resolves it.

## Design documents

[`design-principles/agents-that-act-treatise.md`](design-principles/agents-that-act-treatise.md) explains the failure model that led to Tape.

[`design-principles/tape.md`](design-principles/tape.md) turns that argument into the runtime design: journal semantics, idempotent effects, `UNKNOWN`, action gates, budgets, compensation, replay, and recovery.

These documents are design rationale. The runtime behavior is defined by the implementation and tests under `tape/`.

## Runtime

`tape/` contains the server and SDKs.

The server is written in Rust and supports Postgres, SQLite, and Bigtable-backed storage. SDKs communicate with it over gRPC.

Google ADK integration uses extension points already exposed by ADK, including plugins, custom `SessionService` implementations, `LongRunningFunctionTool`, and `invocation_id`-based resume. ADK itself does not need to be patched.

The runtime provides:

- a journal for model and tool activity;
- durable gates;
- run-state budgets;
- compensation records;
- recovery after process loss;
- replay that reuses recorded decisions and confirmed effects.

Effect safety still depends on the effect adapter and reconciliation strategy. Tape can avoid repeating an effect it has recorded as complete. It cannot infer the outcome of an ambiguous remote call without evidence from the target system or an application-supplied reconciliation rule.

See [`tape/README.md`](tape/README.md) for the runtime quickstart.

## Documentation

| Task | Start here |
| --- | --- |
| First run | [Quickstart](https://vamsiramakrishnan.github.io/durable-agents/quickstart/) |
| Connect an ADK agent | [ADK on Tape](https://vamsiramakrishnan.github.io/durable-agents/adk/) |
| Understand the runtime | [Architecture](https://vamsiramakrishnan.github.io/durable-agents/architecture/) · [Journal](https://vamsiramakrishnan.github.io/durable-agents/concepts/journal/) · [Replay](https://vamsiramakrishnan.github.io/durable-agents/concepts/replay/) |
| Run on GCP | [Cloud Run topology](https://vamsiramakrishnan.github.io/durable-agents/gcp-cloud-run/) · [IAM](https://vamsiramakrishnan.github.io/durable-agents/deploy/iam/) |
| Compare approaches | [Runtime vs. framework](https://vamsiramakrishnan.github.io/durable-agents/runtime-vs-framework/) · [Alternatives](https://vamsiramakrishnan.github.io/durable-agents/concepts/alternatives/) |
| Read the design argument | [Treatise](https://vamsiramakrishnan.github.io/durable-agents/design/agents-that-act-treatise/) |

Preview the docs locally:

```bash
pip install -r docs/requirements.txt
pip install -e tape/sdk/python -e tape/cli
scripts/docs/gen_all.sh
mkdocs serve
```

The docs workflow publishes from `main` through GitHub Actions. Repository Pages settings must use GitHub Actions as the source.

## Repository orientation

- [`tape/`](tape/): runtime, protocol, storage backends, SDKs, and runtime docs
- [`design-principles/`](design-principles/): design rationale
- [`examples/`](examples/): equivalent scenarios across supported SDKs
- [`SDK_PARITY.md`](SDK_PARITY.md): API parity matrix
- [`CLAUDE.md`](CLAUDE.md): coding-harness orientation for this repository

## License

[Apache 2.0](LICENSE)
