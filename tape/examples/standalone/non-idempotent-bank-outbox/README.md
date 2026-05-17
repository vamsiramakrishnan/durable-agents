# non-idempotent-bank-outbox

The kill-test. An agent that wires money through a fake **non-idempotent**
upstream. Proves the outbox + reconciler + compensation choreography.

## Run

```bash
pip install -e .
tape dev
# in another shell:
python -c "from app.agent import build_runner; r=build_runner(); print('runner ready')"
```

## Scenarios

### A — crash before dispatch
Reactor re-drives the run; outbox dispatches exactly once. One wire in the ledger.

### B — crash after dispatch, before record
`observe()` returns CONFIRMED. No re-dispatch. One wire in the ledger.

### C — real duplicate (upstream processed twice)
```bash
TAPE_FAKE_BANK_DUPLICATE=1 tape dev
```
`observe()` returns DUPLICATE. Compensation runs `reverse_wire(duplicate_id)`.
Ledger settles at one wire.

### D — counterparty inconclusive
Edit `fake_bank.lookup` to return `{"count": -1}`. `observe()` maps that to
STUCK. The run becomes STUCK; a human gate is the only path forward.

Read `app/agent.py` — the entire agent code is the `@tape.outbox_tool`
declaration plus a 7-line instruction. The model can't accidentally double-fire
a wire because the body doesn't perform IO.
