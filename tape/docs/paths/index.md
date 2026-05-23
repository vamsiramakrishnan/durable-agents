# Reading paths

The docs are dense — that's deliberate. The runtime has a small number of
primitives doing a lot of work, and each one needs both a how-to and a why.
If you read every page, you'll get every detail; if you read in the wrong
order, you'll bounce off.

Pick the path that matches what you're doing right now:

<div class="grid cards" markdown>

- :material-rocket-launch:{ .lg .middle } **[Beginner](beginner.md)** — *"I want to see what this is."*

    Quickstart → 60-second crash demo → replay diff → first real agent.
    About 45 minutes end-to-end, every step gives you something to look at.

- :material-server:{ .lg .middle } **[Operator](operator.md)** — *"I have to run this in production."*

    Stores → reactors → leases → triage → Cloud Run → monitoring. The
    "what is this thing doing right now and what do I do when it breaks"
    path.

- :material-atom:{ .lg .middle } **[Systems](systems.md)** — *"I want to understand the model."*

    The treatise's section IX (the primitives), the journal-vs-projections
    split, replay determinism, the event bus, what's different from
    Temporal / LangGraph / DBOS.

</div>

The three paths cross each other often — that's fine. They're just
"recommended next" trails through the same forest.
