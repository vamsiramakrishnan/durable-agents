# When the Orchestrator Isn't Code

*A treatise on agents that act*

---

## I. The Asymmetry

An agent is a chatbot that can spend your money. That is the whole story and the whole problem.

The chatbot's worst day is a wrong answer. The agent's worst day is the wrong charge. The first is fixed by clicking regenerate. The second is not.

A system that produces only text is graded on the text. A system that mutates state outside its own process — places an order, books a seat, submits a trade, dispatches a message — is graded on something else. It is graded on whether reality, after the run, looks the way the user wanted. Reality is not regeneratable.

That asymmetry is the architectural argument for durable execution. It is also why the discipline becomes less a 2020s machine learning problem than a 1990s distributed systems problem with a stochastic step in the middle. The hard part is not the model. The hard part is the part of the system that must keep promises across crashes, deploys, network partitions, and the model's own caprice.

The chatbot was forgiven for forgetting. The agent will not be.

---

## II. Three Commitments

Three concrete cases make the case better than any abstract argument. Read each as a sequence of mutations against the world, not as a happy-path API call.

### The shopping agent

A user says: find me these running shoes in size 11, ship to my home.

The request resolves into four commitments across three systems. The cart is held at the retailer. The payment is authorized on the user's card. The order is committed at the retailer. Inventory is decremented in a real warehouse three states away. Four promises. Three vendors. None of them know about each other.

Now ask what happens when the agent's process dies between the second commitment and the third. The card has a hold. The order does not exist. The user does not know.

On retry, an agent without durable execution runs from the top. It searches again. It picks again. It attempts checkout again. Whether the duplicates fire at the retailer or at the card network depends entirely on whose idempotency layer happens to be paying attention. The optimistic outcome is one charge, one order. The pessimistic outcome is two charges, one order, and an angry email at nine in the morning.

What durable execution offers here is a discipline, not a miracle. Each external call is journaled. Each carries an idempotency key derived deterministically from the workflow itself. Same workflow, same step, same key. The retailer dedupes. If the agent crashes after authorization but before the order, replay reconstructs state from the journal — the authorization recorded with its response, the order step the unfinished one, the engine retrying only what is unfinished. One charge, one order, regardless of how many times the host bounced.

The deeper point is that the merchant's idempotency layer was already there. Stripe's twenty-four-hour key cache, Shopify's cart tokens, the airline's PNR. None of them were built for agents. They were built for humans who submit forms twice when the page does not load, who press the back button at the wrong moment, who reload checkout pages out of impatience. Agents do not need new plumbing. They need to plug into old plumbing correctly. The patterns that survived a decade of microservice failure are the patterns agents need; nothing has to be reinvented.

A shopping agent is four promises in a row, each to a different system. Two charges and one shipment is worse than no shipment at all.

### The travel agent

Travel is shopping with a longer time horizon and more vendors, and it gets harder along both axes.

A round trip to Singapore touches an outbound flight, a return flight, often a hotel, sometimes a car, and a payment processor. Five external systems, none of which know about each other, none of which agree on what time it is, none of which will tell the agent anything they did not ask for.

The interesting failure is the partial commit. Outbound is booked. Return is rejected because the fare class disappeared in the fifteen seconds since the search. Now the user has half a trip and a charge. The agent must choose between three responses: cancel the outbound and refund (compensation), retry the return at the next available fare (forward recovery), or surface the failure and wait for instruction (escalation). Each is a real saga path. None of them are reachable without a runtime that remembers what was committed and what was not.

The time axis introduces a second problem. Travel approvals come from partners, expense systems, managers. The agent presents an itinerary and waits. The wait may be ten minutes or two days. During that wait, the process holding agent state cannot stay pinned in memory. The operator deploys. The host autoscales. The cluster reschedules. Without durable suspend, the agent loses its place; the human approval becomes orphaned; the user clicks *approve* and nothing happens, because nothing on the other side is listening anymore.

Travel also exposes the freshness problem in its sharpest form. The price seen at search time is not the price at booking time. Durable execution does not solve this. What it does is force the choice to be explicit. You design a re-fetch step immediately before commit. You journal its result. The workflow consumes the fresh price, not the stale search result. The runtime does not smuggle freshness in; it makes every step declare whether its data is snapshot or live.

The hard part of travel is not booking. It is unbooking. Any travel agent that can commit but cannot roll back is a travel agent that should not be authorized to commit.

### The agent acting on a financial mandate

The third case carries the highest cost of getting wrong.

Take a corporate treasury automation as the concrete version. The agent reads a daily cash position. It decides how much to sweep into short-dated instruments. It runs a pre-trade compliance check. It submits the order to a custodian. It records the fill. Five steps. The order succeeds at the custodian. The agent crashes before recording.

Two things break at once. The day's reconciliation is off by one trade. The next run of the agent reads the old balance and may submit a duplicate order. Both errors compound silently, because nothing in the agent's local state knows the world has already moved.

Durable execution journals the custodian's response. Replay reconstructs the fact that the order was submitted and the fill received, even if the agent's process never reached the recording step. Idempotency on the custodian side — institutional brokers generally accept a `clOrdID` for exactly this reason — closes the data loop. Together: one trade, one record, one reconciliation.

Multi-leg trades are where the discipline becomes philosophically interesting. An FX hedge is two trades that must both execute or neither execute. Without compensation primitives, a partial fill is an unhedged position the user did not ask for. With sagas, leg one is reversed if leg two fails. The agent's job is no longer *submit orders*. It is *submit orders, observe fills, compensate on partial failure, maintain the invariant the user actually wanted*. That is a saga, and a saga needs a runtime.

Audit is where the journal stops being an implementation detail and becomes the deliverable. Every decision the agent made, every input it considered, every effect it submitted, every response it received — recorded, ordered, replayable. A non-durable agent has logging. It does not have audit. The difference is whether a regulator can reconstruct exactly what happened, or whether the firm has to defend a partial story.

A trade that fires twice is a position you did not intend to take. The market does not care that your process restarted. Audit is not logging; it is the journal you cannot argue with.

### The thread

What unites the three is that the agent is not the protagonist. The protagonist is the world — the merchant's inventory, the airline's seat map, the custodian's order book. The agent is a small program negotiating commitments with larger systems that have their own opinions about what just happened. The runtime's job is to keep the agent's view of those commitments and the world's view of them in agreement across crashes, deploys, and time.

Most current agent frameworks treat this as somebody else's problem. The retry decorator is the tell. Every retry decorator is a place where the framework asked the user to solve a problem the runtime should have solved. Multiplied across every tool call in a multi-step agent, the user ends up reimplementing a poor version of Temporal inside their own application, integration by integration, discovering the same edge cases in the same order the workflow-engine community discovered them between 2014 and 2020.

---

## III. What the Old Texts Already Say

Distributed systems research has spent forty years saying things that turn out to apply to agent systems with the parameters adjusted.

Lamport's *Time, Clocks, and the Ordering of Events* (1978) established that there is no global clock. Events are ordered only by their causal relationships, and that order is partial, not total. Helland's *Life Beyond Distributed Transactions* extended this to the practical: ACID across services is not available; what you have are grants, agreements, and partial failures. His *Memories, Guesses, and Apologies* went further. Distributed systems live in a world of inexact information. The right primitive is often *I think this is true; if I am wrong, I will apologize.*

The end-to-end argument (Saltzer, Reed, Clark, 1984) holds that reliability must be verified at the endpoints. Intermediate components cannot guarantee what the endpoint cares about. A network can promise delivery; only the receiver can confirm it. The implication for agents is direct. The vendor's confirmation is the only commitment that counts. The agent's local belief that the order was placed is not the same thing as the order having been placed.

The saga paper (Garcia-Molina and Salem, 1987) introduced compensation as the way to maintain weakened consistency over long-running operations. If step C fails, run ¬B and ¬A. The literature on event sourcing built on this. Store events. Derive state by replay. The journal becomes the source of truth. The actor model — Hewitt, Agha, Erlang/OTP — handled message-driven concurrency through mailbox ordering and supervised processes that survive their own faults.

Every one of these primitives assumes that the orchestrator's behaviour can be characterised at design time. Sagas have known forward graphs and known compensation graphs. Event sourcing has deterministic state transitions. Idempotency keys cover known operations. The actor model has typed messages.

They were designed for a world where the orchestrator was code an engineer wrote, and the engineer's job was to handle network failure within the bounds of decisions the engineer had pre-specified.

This is the assumption the LLM-as-orchestrator pattern violates. And the violation is structural, not incidental.

---

## IV. The Assumption Breaks

The orchestrator's decisions are no longer pre-specified. They are sampled at runtime from a model whose policy was learned, not written.

Four consequences follow.

The forward path is not a graph the engineer can enumerate. It is a runtime trajectory the engineer can only constrain. Where the saga literature said *here is the sequence A→B→C and here is its compensation ¬C→¬B→¬A*, the agent literature has to say *the agent will choose actions from this space at runtime, and we will have to journal what it chose so we can compensate it*.

The compensation depends on what the LLM did. It cannot be pre-written, because the forward path was not pre-written. Often the only thing positioned to write the compensation is the LLM itself, given the partial state and the goal.

The idempotency unit is fuzzy. A single tool call is clear — the call has parameters; the parameters can be hashed; the hash is the key. An LLM-generated plan is less clear; the same prompt can produce different plans, and the plans are not naturally addressable. An agentic objective is very unclear; *book me a trip to Singapore* is not a key.

The verification gap widens. Classical patterns assumed there was an oracle for *did the operation succeed*. The bank's response said yes or no. For an agent task — *did the research report answer the question well*, *did the support response satisfy the customer*, *did the coding agent fix the bug* — there is no oracle. There is only a slower, more expensive judgment, often by another model, often by a human.

These are not problems classical patterns solve. They are problems classical patterns assume away.

---

## V. Six Places It Breaks

A short tour. Six classes of agent, six places the inherited toolkit runs out.

### The autonomous coding agent

The LLM reads a ticket, plans an implementation, edits files, runs tests, iterates, opens a pull request. The classical reading is a saga: plan, implement, test, PR; on failure, revert.

The pattern breaks in three places.

The plan is generated at runtime, so the saga's forward graph is not knowable when the workflow is defined. Compensation is partial: the code can be reverted, but the LLM has consumed context window and burned cost making decisions whose value cannot be undone. On long-running jobs — a coding agent that owns a feature across a sprint — the journal of *what the agent did* is large and partly irrelevant; replay does not reconstruct the same plan, because the plan was non-deterministic in the first place.

What the system actually needs is journaled decisions, not just side effects. The ability to fork and abandon plans cheaply. Budget enforcement. Resumption that uses the journal as memory rather than as a transcript to replay verbatim.

### The browser-use agent

The LLM looks at a screenshot, decides where to click, types text, navigates, fills forms.

Classical idempotency does not apply. *Click at (453, 287)* is not idempotent because the page state changes between attempts. The DOM is not a database with versioned writes; it is a stateful UI with implicit locks the LLM cannot see. Replay against the page produced two minutes later is interaction with a different page. Compensation is wildly contextual: *undo this booking* is not a database revert, it is another agent run that must discover the booking, navigate to the cancellation flow, and confirm.

The system needs visual state snapshots in the journal, action verification — *did the click do what was intended* — and the ability to detect when replay has diverged from the journaled trajectory.

### The SRE agent responding to a page

The LLM reads the alert, queries metrics, tails logs, runs probes, forms a hypothesis, applies a fix.

The action space is *anything you would run on a Linux box*. It is unbounded. Some actions are destructive — restart a service, scale down a fleet, drain a node — and compensation is partial. The system being diagnosed is changing while the agent reasons; events that occurred while the agent was thinking are not in its context. Multiple agents on the same incident can fight: one scales up, another scales down. Audit is not optional; regulators and incident reviewers will ask what the agent did, why, and with what authority.

The system needs action-level approval gates for destructive operations, fresh-state verification before each significant action, multi-agent coordination via shared environment state, and a journal that doubles as audit.

### The customer support agent in a multi-turn conversation

The LLM reads messages, classifies intent, queries account state, decides actions, responds.

The conversation accumulates commitments. *I will refund $50* said in turn 2 must be honoured in turn 5. State persists across long gaps; the customer may respond two days later. Compensation logic is awkward: how do you undo an empathetic response? The LLM might commit, in language, to something the system cannot deliver, and the gap between the language commitment and the system action is where the failure lives.

The system needs structured commitment tracking that the conversational layer renders as language but the durable layer enforces as data. Conversation suspend across days. A verification step before any commitment is rendered to the customer.

### The deep-research agent

Open-ended question. The agent plans search queries, reads results, synthesises, follows up, builds a report.

Branching is non-enumerable. What to follow up on depends on what was found. Costs accumulate — LLM calls, search calls; without budget enforcement the agent runs to exhaustion. Citation correctness is a verification problem the system cannot easily check. Replay re-does searches and may find different results, producing a different report.

The system needs budget as a first-class workflow construct, journaled provenance for every claim in the output, and the recognition that *replay* for this class of agent means *resume the search tree from where it stopped*, not *reproduce the same outputs*.

### The sales or outbound agent

The LLM researches a prospect, drafts an outreach, sends, follows up, books meetings.

Communication is reputation-bearing. Sending the wrong message is not an idempotency problem to retry through; it is a damage event. Compensation is impossible — you cannot unsend an email. The agent's plan adapts to responses or non-responses; pre-written drip sequences cannot adapt to the prospect's signals. Cross-agent coordination is critical; two agents working the same prospect is a serious failure.

The system needs lock-and-coordinate primitives at the prospect level, irreversible-action gates with human approval, and explicit *do not contact* state that lives outside any single workflow run.

### The pattern across the six

Classical microservice primitives handle the side-effect mechanics — idempotency on the API call, durability on the database write, compensation on the partial commit. They do not handle decision journaling. They do not handle action-space management. They do not handle multi-agent coordination. They do not handle fresh-state verification. They do not handle budget enforcement. They do not handle non-deterministic replay.

These are the gaps the new tools are trying to fill.

---

## VI. The "Idempotent API" Defence

The most reasonable objection to all of this, made well, sounds like this: Stripe is idempotent on charge. The airline API is idempotent on booking. The broker is idempotent on `clOrdID`. A retry of the workflow re-fires the same call with the same key; the merchant dedupes. We do not need a durable runtime. We need disciplined idempotency.

The objection is right and incomplete. There are five specific ways it falls short, and each is a reason the durable execution category exists.

*The keys must be derived from a stable workflow identity.* A fresh UUID per attempt is not idempotency. It is a guarantee of duplication. The workflow needs an identifier that survives the agent's restart, and the keys for each step must be deterministic functions of `(workflow_id, step_position)`. This is a workflow-level invariant. If the framework does not supply it, the developer has to invent it — and inventing it correctly is exactly the job of a workflow runtime.

*Idempotency at each integration does not produce idempotency at the workflow level.* A multi-step workflow that charges then books may, on a crash between the two, retry both steps. The charge dedupes. The booking is fresh, because the booking step had not yet fired so no key existed. The agent on retry has no way to know whether the original run booked and the response was lost, or never booked at all. The journal answers this. Idempotency at the layer below the journal does not.

*Compensation is not a retry.* When step 2 succeeded and step 3 fails, the system must run ¬2, which is a different operation than 2. Idempotent APIs help if the compensation needs to retry, but the existence and design of the compensation path is the workflow's responsibility. Frameworks that hand-wave compensation with *use try/except* are not handling it.

*Idempotency windows are bounded.* Stripe holds keys for twenty-four hours. Travel approvals, settlement cycles, multi-day human-in-the-loop flows routinely run longer. The key expires before the workflow completes. On retry the merchant treats the call as new. The runtime must either complete the operation within the window or detect expiry and choose a recovery strategy. That logic does not exist at the API layer.

*Decision and reasoning provenance is not in any API contract.* The agent decided to pick vendor A because of search snapshot X. It decided to allocate Y because of policy version Z. It decided to escalate because of error pattern W. None of this is an API call. None of it has an idempotency key. All of it is part of what makes the agent's behaviour auditable, replayable, defensible. The journal is where this lives. The API has no story for it.

Idempotency at the API is necessary. It is not sufficient. The gap between necessary and sufficient is exactly the work the new tools are doing.

---

## VII. What the New Layer Actually Does

Read the documentation for any current agent runtime — Temporal's agent framing, Restate's agent docs, the durability features being retrofitted into LangGraph and CrewAI — and what they are converging on is a layer above the classical patterns, not a replacement for them.

The capabilities they are trying to standardise are recognisable once stated.

Decisions are journaled, not just side effects. The LLM call is wrapped as a recorded activity whose output drives the next step, so on replay the decision is consistent with what the agent originally did, not re-sampled from a fresh distribution. Compensation is adaptive; the system can run a compensation path generated at runtime rather than pre-written, often by the LLM itself given the partial state. Action-space gates surface destructive or irreversible actions to a human and suspend until approved. Multi-agent coordination is mediated by durable shared state rather than direct messaging; when N agents share a problem, they coordinate through a journaled consistent view. Budget — token, dollar, time — is a first-class workflow construct, not a wrapper that fires after the fact. Verification at consumption means re-reading fresh state before any consequential action rather than trusting the journal. Replay semantics are tuned to the workload: a coding agent's replay is *resume from journal*; a browser agent's replay may be *re-verify the page*; a research agent's replay is *extend the search tree*.

None of this is solved by Postgres plus Kafka plus a saga library. None of it is a problem Postgres plus Kafka plus a saga library was designed for. The classical toolkit handles the plumbing. What sits above the plumbing is genuinely new work.

---

## VIII. On Substrate

Most of these systems are written in Python. Python is structurally hostile to most of what they require. The problems compound rather than cancel.

Determinism within workflow code is the first. Python permits non-deterministic constructs — `time`, `random`, hash randomisation, dict iteration in some contexts, threading — anywhere. Sandboxes mitigate but cannot enforce. In a world where the LLM is already a non-determinism source, having a second non-determinism source in the runtime is a recipe for replay drift.

Concurrency is the second. Python's three concurrency models — sync, asyncio, threading — compose poorly with each other and with durable execution. Most agent libraries mix all three. Workflow engines have to constrain user code to one model, fighting the language.

State serialisation is the third. `pickle` is fragile across Python versions and class definitions. The first deploy after a workflow goes long-running is the deploy where pickle breaks. Strongly typed alternatives like Pydantic help but are not pervasive.

Hot reload is the fourth, and it is not really possible. Long-running agents that need to be patched mid-flight are a Python anti-pattern. Erlang/BEAM was designed for this case. Python was not.

Sandbox enforceability is the fifth. Python's dynamism — `__import__`, `ctypes`, `exec`, monkey-patching — means the workflow sandbox is best-effort. Static-typed languages such as Go, Rust, and TypeScript with strict typing make the equivalent guarantees enforceable rather than documented.

Process model is the sixth. Python has no first-class actor model. Multi-agent systems on Python are bolted on with Ray, Dask, or custom infrastructure, none of which integrate with workflow journals natively.

The cumulative effect is that durable agent execution tools targeting Python ship with dialect restrictions — *don't use `time.sleep` in workflow code* — failure modes documented as developer rules rather than runtime guarantees, and state-serialisation edge cases that surface at the worst possible moment. The TypeScript and Rust equivalents do not have these. The engineering economics of building durable agent execution in Python are uphill in a way that does not show up in greenfield benchmarks but compounds in production over months.

The honest position is that the next generation of agent infrastructure will probably not be Python at the runtime layer, even if it remains Python at the application layer. Python will write the agent. Something else will run it.

---

## IX. The Landscape

The category has converged on a small set of architectural patterns implemented in different deployment models.

Temporal is the most mature, built by the team behind Cadence at Uber. Workflows are written as code in Go, Java, TypeScript, Python, .NET, or Ruby. The runtime is a self-hosted cluster or Temporal Cloud. Production users include Snap, Netflix, HashiCorp, Box, Datadog, and JPMorgan Chase, with announced agent integrations across the OpenAI Agents SDK and the Vercel AI SDK in 2025. Strong on long-running workflows and complex saga patterns. Heavier operational footprint than alternatives.

Restate is newer, from former Apache Flink and Meta engineers. Single-binary Rust runtime, optimised for low-latency durable execution and serverless or edge deployment. Strong agent positioning with integrations across the Vercel AI SDK, the OpenAI Agent SDK, and Google ADK. Production users include 21Bitcoin for trade orchestration, Coralogix for agentic observability fleets, and Deliveru for recruiting research agents. Lighter than Temporal, more opinionated about deployment.

DBOS takes the simplest operational model: durable execution as an in-process library backed by Postgres, with no new infrastructure beyond your existing database. Fits teams that already have Postgres at the centre and do not want a new clustered service.

AWS Step Functions, Azure Durable Functions, and Cloudflare Workflows are cloud-provider-native versions of the same pattern. Step Functions is the oldest and predates the modern category, using a JSON-based state language. Azure Durable Functions runs in-process to the Functions runtime. Cloudflare Workflows brought step-based durable execution to the edge in 2025, with multi-day execution and Python support.

Inngest and Hatchet are TypeScript- and Python-first hosted workflow runtimes targeting application teams that want durable execution without standing up Temporal. Lighter integration. Narrower scope. Simpler deployment.

LangGraph, CrewAI, and ADK sit in a different layer. They are graph runtimes or orchestration libraries with bolted-on durability. LangGraph's checkpointers are the most mature of the bolt-ons, but the granularity is at node boundaries rather than effect boundaries, and the patterns are documented as user discipline rather than runtime guarantees. The realistic move for any team building an agent system that needs the properties this essay has been describing is to put a durable execution engine *underneath* the agent framework rather than relying on the framework's own durability features. The integrations announced in 2025 across Temporal, Restate, and the major agent SDKs reflect this. The agent framework community is ceding the runtime layer to the durable execution category and focusing on the composition layer above it.

The honest read of the landscape is that the runtime layer is being built. The agent framework layer is, increasingly, an application of it.

---

## X. Floor and Ceiling

The deflationary argument — *we already have sagas* — is correct for the I/O layer and incomplete for everything above it.

The exactly-once writes, the compensation, the idempotency, the long-running workflow primitives that Garcia-Molina and Salem named in 1987 and that two decades of microservice engineering refined into something operational: that is the floor. It is solved. Anyone building an agent that ignores the floor will discover the same edge cases in the same order the workflow community discovered them between 2014 and 2020.

Decision journaling, adaptive compensation, action-space management, multi-agent coordination, fresh-state verification, budget enforcement, replay semantics tuned per agent class: that is the ceiling. It is being built. The saga literature does not solve it because the saga literature did not need to. The saga literature assumed the orchestrator was code. When the orchestrator is an LLM, the assumption no longer holds, and the work above the floor is genuinely new.

The people who say *we already have sagas* are right that the floor is solved. The people who say *we need new tools* are right that the ceiling is not. They are talking past each other because the language has not separated the floor from the ceiling. The agent durable execution category, when it survives the marketing layer, is the ceiling.

The chatbot was forgiven for forgetting. The agent will not be. An agent is a contract with the world. The runtime is what keeps the contract honest.
