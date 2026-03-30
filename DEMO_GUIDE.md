# Meltdown Demo Delivery Guide

This guide is for anyone presenting the Meltdown demo. It covers setup, the one-minute pitch on each technology, and step-by-step scripts for each demo scenario (~2–5 min each).

---

## Before You Start

**Requirements:**
- Temporal CLI running: `temporal server start-dev`
- `.env` with `GOOGLE_API_KEY` set (Gemini). Maps and CSE keys are optional — demo works without them.
- `./run.sh` (or `make run`) started, browser open at http://localhost:8080
- Temporal UI open at http://localhost:8233 (optional but great for showing workflow history)

**Pre-flight check:**
- Crews, orders, and map visible on the dashboard
- "Start Deliveries" button is active (not grayed out)
- If you see a stale state from a prior run, click **Reset** first

**Tip:** Do a dry run of each scenario before presenting. The cooler malfunction timing and crash recovery both have visual overlays that land better when you know exactly what to expect.

---

## The One-Minute Pitch

Use this framing at the start of the talk before any demo:

> "AI agents are increasingly being used to automate complex decisions — but in production, they break. The worker crashes. A tool call times out. The LLM call returns mid-reasoning and the state is gone. What we're showing today is what happens when you combine Google ADK — a framework for composing multi-agent AI — with Temporal — a durable execution engine — so that every agent action is retryable, replayable, and recoverable."

---

## What is Google ADK?

**The 30-second version:**

> "Google ADK is an open-source framework for building multi-agent AI systems. You compose agents — each with their own tools and model — into pipelines: run them sequentially, in parallel, or nested. In this demo, a Fleet Agent uses Google Maps to assess logistics, a Customer Agent uses live hotel search to understand order priorities, and a Resolver Agent synthesizes their output into an actionable recovery plan."

**Key points to land:**
- Agents are composable — `SequentialAgent`, `ParallelAgent`, nested agents
- Each agent can use tools (Maps, Search, custom functions)
- ADK manages the multi-turn reasoning loop — the developer just defines the agents and wires them together

---

## What is Temporal?

**The 30-second version:**

> "Temporal is a durable execution platform. You write your business logic as code — workflows and activities — and Temporal guarantees it runs to completion even if the service crashes, times out, or gets disconnected. Every step is recorded in an event log. If the worker dies mid-execution, Temporal replays the history deterministically, and your code resumes exactly where it left off."

**Key points to land:**
- Workflows are durable — crashes don't lose state
- Activities are retryable by default — transient failures self-heal
- Signals let you inject events into a running workflow (disconnect, disruption, change request)
- The Temporal UI shows the full event history for every workflow run — nothing is a black box

---

## The Integration: Why It Matters

> "Here's the key insight: in this demo, every LLM call goes through a `TemporalModel` wrapper — it becomes a Temporal activity. Every tool call (Maps, search, fleet status) is also a Temporal activity. That means if the worker crashes mid-agent-reasoning, Temporal doesn't re-call the LLM. It replays the result from the event log. The agent resumes exactly where it left off, with no extra cost and no lost context."

This is the "aha" moment. Return to it whenever you trigger a crash.

---

## Demo Scenarios

---

### Demo 1: Agent Disconnect — ADK + Temporal Working Together
**Time: 2–3 min | Best for: opening with why the integration matters**

**Setup:** Start deliveries. Let crews get moving, then disconnect an agent before the delivery plan coordination completes — or reset and trigger immediately after start.

**Steps:**
1. Click **Disconnect Agent** (Fleet Agent) shortly after deliveries begin
2. The Agent Reasoning panel shows Fleet Agent going offline
3. The other agents (Customer Agent, Resolver) continue reasoning and coordinate without it
4. The Resolver produces a plan based on available information — Temporal records it as an activity result
5. Click **Reconnect Agent** — Fleet Agent comes back online and is available for future events

**What to say:**
> "This is where ADK and Temporal each pull their weight. ADK handles the agent layer — when Fleet Agent goes offline, the Customer Agent and Resolver adapt. They don't crash, they degrade gracefully. Temporal handles the infrastructure layer — every reasoning step that did complete is recorded as an activity result. If the worker crashed right now, those results would be replayed. Two different resilience mechanisms, working at two different layers."

**Concepts to highlight:** ADK graceful degradation (agent layer) vs. Temporal durable execution (infrastructure layer) — this is the core of the integration story

---

### Demo 2: Crew Disconnect & Auto-Recovery
**Time: 2–3 min | Best for: showing Temporal activity retry**

**Setup:** Start deliveries. Wait until at least one crew is en route.

**Steps:**
1. In the Failure Modes panel, select a crew and click **Disconnect Crew**
2. That crew's status changes to `DISCONNECTED`, its dot stops moving
3. The other two crews keep delivering normally
4. Wait 10–15 seconds, then click **Reconnect Crew**
5. The crew's status shows a brief "recovering" state, then resumes

**What to say:**
> "When we disconnect the crew, the navigation activity starts failing and throwing errors. Temporal catches it and retries — with exponential backoff, no maximum attempt limit. The other crews are unaffected because each crew runs in its own child workflow. When we reconnect, the next retry succeeds, and the crew picks up exactly where it stopped."

**Temporal concept to highlight:** Activity retry policies, child workflow isolation

---

### Demo 3: Customer Change — Human-in-the-Loop
**Time: 2 min | Best for: showing signals and workflow waiting**

**Setup:** Start deliveries.

**Steps:**
1. In the Customer Changes panel, select an order and click **Submit Change Request**
2. The Agent Reasoning panel shows a `customer_request` event — the workflow is now paused, waiting
3. Open Temporal UI — show the workflow is "Running" but blocked on `wait_condition`
4. Click **Approve** (or **Reject**) — the workflow immediately unblocks and executes (or discards) the change

**What to say:**
> "The workflow is literally paused here — waiting for a human signal. There's no polling, no timeout hack, no database flag. Temporal persists the workflow state indefinitely. If I closed the server right now and restarted it, the workflow would still be waiting for this approval. That's what durable execution means."

**Temporal concept to highlight:** Signals, `wait_condition`, indefinite workflow suspension

---

### Demo 4: Full System Crash & Temporal Replay
**Time: 2–3 min | Best for: the most visceral Temporal moment**

**Setup:** Start deliveries. Let crews get partway to their destinations — the further along, the more dramatic the resume.

**Steps:**
1. Click **Crash Service** — a red overlay appears: "Service Crashed"
2. Point to the map: crews are frozen mid-route
3. Open the Temporal UI — show the workflow is still "Running" (not failed). *"The workflow state lives in Temporal, not in our service. The work isn't lost."*
4. Click **Restart Service** — blue "Replaying..." overlay appears for ~4 seconds
5. Crews resume from their exact positions

**What to say:**
> "Notice the crews didn't restart from the kitchen — they resumed mid-route. Temporal replayed the workflow event history. Our worker re-executed the same code path, but when it hit activities that already completed, Temporal returned the cached result. The agents didn't re-call Gemini. It just... continued."

**Temporal concept to highlight:** Deterministic replay, event sourcing

---

### Demo 5: Cooler Malfunction — Multi-Agent Recovery
**Time: 4–5 min | Best for: closing with the full ADK agent composition story**

**Setup:** Start deliveries. This is the most complex demo — give it space and let the agent reasoning panel breathe.

**Steps:**
1. Click **Trigger Cooler Malfunction** (defaults to AI-Crew 1 at nav step 5)
2. Watch the map — when the malfunction triggers, the affected crew's orders turn orange/at-risk
3. The Agent Reasoning panel starts populating — narrate as events appear:
   - Fleet Agent calls `tool_get_fleet_status` → assesses routes and capacity
   - Fleet Agent calls `tool_get_route_info` (Google Maps) → checks ETAs for backup crew
   - Customer Agent researches the hotel → checks for VIP events, context
   - Customer Agent assesses order priorities
   - Resolver Agent synthesizes both assessments into a recovery plan
4. A "Recommendation Pending" event appears — **Approve / Reject** button becomes active
5. Click **Approve** — orders are rerouted to a backup crew, the affected crew returns to base

**What to say (during agent reasoning):**
> "Fleet Agent and Customer Agent are running in parallel — that's ADK's ParallelAgent. Each tool call — Maps, fleet status, hotel search — is its own Temporal activity. Every one of those results is in the event log. If the worker crashed right now, the agents wouldn't re-call Gemini. Temporal would replay the results."

> "The Resolver sees both inputs and produces a structured recovery plan — not a blob of text, but a typed object with specific order IDs and crew assignments. The workflow reads it directly and acts on it."

**ADK concepts to highlight:** ParallelAgent, SequentialAgent (Hotel Researcher → Customer Agent), tool composition, structured output via session state

**Temporal concept to highlight:** Agent actions as durable activities, human-in-the-loop on `operator_decision` signal

---

## Handling Questions

**"How is this different from just using a queue?"**
> "A queue gives you one retry per message. Temporal gives you a complete execution model — retries, timeouts, timeouts-per-retry, backoff, heartbeating, child workflows, signals, queries. And it's all in code, not config."

**"What if Gemini returns something unexpected?"**
> "The agents use structured tool calls to submit their output — `tool_submit_recovery_plan` writes a typed object to ADK session state. The workflow reads that object. If the agent produces garbage or skips the tool call, the workflow gets `None` and falls back to a deterministic mock resolver. There's a clear contract."

**"Is this production-ready?"**
> "The pattern is production-ready — Temporal runs at Stripe, Netflix, Uber. ADK is Google's framework for building agents at scale. The integration shown here (`TemporalModel`, `activity_tool`, `GoogleAdkPlugin`) is the `temporalio[google-adk]` package, which is the official integration."

---

## Reset Between Demos

1. Click **Reset** on the dashboard
2. Verify all crews return to `IDLE`, orders return to `PENDING`
3. If any workflows are stuck, run: `temporal workflow list` and cancel manually
4. Refresh the browser before the next run
