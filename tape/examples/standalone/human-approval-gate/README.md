# human-approval-gate

A run that parks on a durable gate (`tape.gate_tool("cfo-approval")`) until
an approver sends a signal. Restart the agent process while parked — the run
stays WAITING. When the signal arrives, the recovery reactor re-drives the
run and the tool returns the resolution.

This example is intentionally minimal: see `app/agent.py` for the wiring.
