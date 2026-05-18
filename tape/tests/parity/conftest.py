"""Cross-SDK parity harness — every SDK runs the same scenario.

The parent `tape/tests/conftest.py` provides the `tape_server` fixture (a real
Rust tape-server subprocess backed by an ephemeral SQLite file). Each test in
this directory drives the **same scenario** against that server through a
different language's outbox dispatcher CLI, then asserts the resulting journal
projection is identical across languages.

The scenario shape:

  1. The Python client creates a fresh run + decision + a PENDING+OUTBOX
     effect with `semantics=NON_IDEMPOTENT` and `connector="log"`.
  2. The language under test runs **one pass** of its outbox dispatcher with
     `--register-log-connector --once`.
  3. The Python client polls the effect and asserts `status == CONFIRMED`.

That covers G1 + the protocol-parity contract: every SDK reaches the same
journal state from the same input. The Python harness is the **driver and
observer** — the language under test does only the work the wire protocol
permits.
"""
from __future__ import annotations

# This file intentionally only documents the harness — the actual `tape_server`
# fixture comes from `tape/tests/conftest.py`, which pytest discovers
# automatically because it's the parent directory's conftest.
