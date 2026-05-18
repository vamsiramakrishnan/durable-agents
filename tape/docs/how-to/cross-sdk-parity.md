# Cross-SDK parity — one scenario, four languages

The wire protocol (`tape/proto/tape.proto`) is the contract. Anything
reachable by RPC is reachable from every SDK. The cross-SDK parity harness
at [`tape/tests/parity/`](https://github.com/vamsiramakrishnan/durable-agents/tree/main/tape/tests/parity)
drives **one shared scenario** through Python, TypeScript, Go, and Java
against the same Tape server and asserts the journal projection is identical
across languages.

That's the parity contract: not "every SDK has every Python module
verbatim" but "every SDK reaches the same journal state from the same
input."

## Run it

```bash
make sdk-parity                              # all four, against one tmp server
PYTHONPATH=tape/sdk/python pytest tape/tests/parity/ -v    # equivalent
```

Each language test cleanly `skip`s if its toolchain isn't installed — CI
runs them all, local dev may only have a subset. The harness picks a free
port, spawns `tape-server --store memory`, and tears it down after each
test.

## The scenario

[`tape/tests/parity/scenario.py`](https://github.com/vamsiramakrishnan/durable-agents/blob/main/tape/tests/parity/scenario.py)
builds a fresh run + decision + a single `PENDING+OUTBOX` effect with
`semantics=NON_IDEMPOTENT` and `connector="log"`. The shape:

```python
from tape.tests.parity.scenario import make_pending_outbox_effect
scenario = make_pending_outbox_effect(url, language_tag="<lang>")
# scenario.run_id, .idempotency_key, .business_key, .tool_name, .connector
```

Then [`test_outbox_parity.py`](https://github.com/vamsiramakrishnan/durable-agents/blob/main/tape/tests/parity/test_outbox_parity.py)
runs **one pass** of each language's
[outbox dispatcher](outbox-daemon.md) — `--once
--register-log-connector` — against the same server, and polls the effect
until it reaches `EFFECT_STATUS_CONFIRMED`.

## Adding a new scenario

If you add a new primitive (or a new failure mode), drop a sibling scenario
into `tape/tests/parity/` and run it through every language's CLI:

1. Define a `make_<thing>` factory in a new scenario module — keep it
   Python-only; the harness drives the languages-under-test via their
   existing CLIs.
2. Add a parameterized test that loops over `{python, typescript, go,
   java}`, runs each language's dispatcher / reactor / whatever CLI, and
   asserts the journal projection.
3. Update `.github/workflows/sdk-tests.yml`'s `parity` job if your new
   scenario needs extra setup (it usually doesn't).

## CI

`.github/workflows/sdk-tests.yml` runs `sdk-parity` after the per-SDK
jobs. The job pre-warms `npm install`, the Maven jar, and the Maven
runtime classpath, so cold-start latency stays out of the test loop.

A green `sdk-parity` job is the parity gate — see
[`SDK_PARITY.md`](https://github.com/vamsiramakrishnan/durable-agents/blob/main/SDK_PARITY.md)
for the live scorecard.

## What it doesn't prove

The harness drives the **wire protocol contract**. It doesn't prove:

- That an ADK runner end-to-end behaves identically across languages —
  there's no Java ADK example yet (see
  [`SDK_PARITY.md`](https://github.com/vamsiramakrishnan/durable-agents/blob/main/SDK_PARITY.md)
  G4 for the Java adapter status).
- That model replay (short-circuit a recorded LlmResponse on re-drive)
  works in every SDK — only Python ships that today.
- Performance or scaling characteristics — the harness is correctness-only.

Each of those needs its own scenario; the harness is the mould.
