<p align="center">
  <img src="https://github.com/google/adk-docs/raw/main/docs/assets/agent-development-kit.png" alt="Google Agent Development Kit" width="600">
</p>

# Meltdown — Ice Cream Delivery Fleet Demo

A conference demo showing **Google ADK** multi-agent reasoning with **Temporal** durable execution, visualized as an ice cream delivery fleet on the Las Vegas Strip.

Three AI-Crews deliver VIP ice cream orders to hotels on the Strip. When things go wrong — cooler malfunctions, service crashes, customer changes — AI agents coordinate the response while Temporal ensures nothing is lost.

## What It Demonstrates

| Scenario | What Happens | What It Shows |
|----------|-------------|---------------|
| **Crew Disconnect** | Take a single AI-Crew offline mid-delivery | Temporal retries activities indefinitely until reconnect — no work lost |
| **Agent Disconnect** | Take an individual ADK agent offline | Remaining agents adapt reasoning — resolver compensates for missing input |
| **Cooler Malfunction** | Trigger a cooler failure on an AI-Crew | Fleet Agent (with Maps routes) + Customer Agent (with hotel research) assess in parallel, Resolver synthesizes a recovery plan |
| **Customer Change** | Submit an address change or cancellation | Human-in-the-loop approval with agent reasoning |
| **Full System Crash** | Kill the entire worker mid-delivery, restart it | Temporal replays workflow history — AI-Crews resume from exact position |

## Architecture

```
┌──────────────────────────────────┐
│         Temporal Server          │
│   (workflow state + replay)      │
└──────────┬───────────────────────┘
           │
┌──────────▼───────────────────────┐
│       Temporal Worker            │
│  ┌─────────────────────────────┐ │
│  │  MeltdownDemoWorkflow       │ │
│  │    └─ CrewRouteWF x3     │ │
│  │        ├─ navigate_to()  ←──┼─┼── heartbeats every step
│  │        ├─ pickup_orders()   │ │
│  │        └─ deliver_order()   │ │
│  └─────────────────────────────┘ │
│  ┌─────────────────────────────┐ │
│  │  ADK Agents (via Temporal)  │ │
│  │  ParallelAgent:             │ │
│  │  ├─ Fleet Agent             │ │  tool_get_fleet_status
│  │  │   └─ Google Maps API  ←──┼─┼── tool_get_route_info (activity)
│  │  ├─ Customer Assessment     │ │  SequentialAgent:
│  │  │   ├─ Hotel Researcher ←──┼─┼── google_search (built-in)
│  │  │   └─ Customer Agent      │ │  tool_get_order_priorities
│  │  └─ Resolver Agent  ←──────┘ │  tool_submit_recovery_plan
│  │      model=TemporalModel()   │ │
│  │      tools=activity_tool()   │ │
│  └─────────────────────────────┘ │
└──────────────────────────────────┘
           │
┌──────────▼───────────────────────┐
│     FastAPI + WebSocket          │
│     └─ Frontend (Leaflet map)    │
└──────────────────────────────────┘
```

**Key integration**: ADK agents run inline in the workflow. Every LLM call goes through `TemporalModel` (activity), every tool call goes through `activity_tool` (activity). If the worker crashes mid-reasoning, Temporal replays the activities from history — the agent resumes without re-calling the LLM.

### MCP Toolsets

| Tool | Agent | Integration | Purpose |
|------|-------|-------------|---------|
| **Google Maps Directions** | Fleet Agent | Activity-backed (`activity_tool`) — replay-safe | Driving routes and ETAs for reroute assessment |
| **Google Search** | Hotel Researcher | Built-in ADK tool (`google_search`) | Live hotel event context (conferences, VIP bookings) |

The Fleet Agent's Maps tool is wrapped as a Temporal activity — if the worker crashes mid-call, the result is replayed from history. Google Search runs in a dedicated Hotel Researcher sub-agent (Gemini constraint: `google_search` cannot be combined with other tools in the same agent). The Hotel Researcher writes `hotel_context` to ADK session state, which the Customer Agent reads to enrich its assessment.

## Prerequisites

- Python 3.11+
- [Temporal CLI](https://docs.temporal.io/cli) (`brew install temporal`)
- Google Gemini API key (for ADK agents; falls back to mock mode without it)
- Google Maps API key (optional — falls back to mock route data)

Both API keys fall back gracefully: without `GOOGLE_API_KEY`, agents use deterministic mock reasoning. Without `GOOGLE_MAPS_API_KEY`, route checks use calculated distance/ETA estimates.

## Quick Start

### 1. Start Temporal dev server

```bash
temporal server start-dev
```

### 2. Install and run

```bash
pip install -e ".[dev]"
echo 'export GOOGLE_API_KEY="your-gemini-key"' > .env
echo 'export GOOGLE_MAPS_API_KEY="your-maps-key"' >> .env  # optional
./run.sh
```

### 3. Open the dashboard

Navigate to http://localhost:8080

## Demo Flow

1. **Start Deliveries** — 3 AI-Crews dispatch from Ice Cream Kitchen to MGM Grand, Caesars Palace, Mandalay Bay
2. **Crew Disconnect** — Select an AI-Crew → disconnect → activities retry until reconnect → seamless resume
3. **Agent Disconnect** — Take Fleet or Customer Agent offline → trigger disruption → remaining agents adapt reasoning
4. **Cooler Malfunction** — Trigger disruption → Fleet Agent checks Maps routes → Hotel Researcher gathers event context → agents reason in parallel → recovery plan → orders rerouted
5. **Customer Change** — Submit a change → agent evaluates → approve/reject → order updated
6. **Full System Crash** — Kill the service mid-flight → red overlay → restart → blue "Replaying..." overlay → AI-Crews resume

## Key Files

| File | What it does |
|------|-------------|
| `agent_fleet/models.py` | Dataclass models for all Temporal payloads |
| `agent_fleet/simulation.py` | In-memory fleet state (singleton shared by worker + server) |
| `agent_fleet/activities.py` | Temporal activities — navigation, delivery, Maps API, agent tools |
| `agent_fleet/workflows.py` | Temporal workflows — orchestration, signals, queries |
| `agent_fleet/agents.py` | ADK agent composition — Fleet, Hotel Researcher, Customer, Resolver |
| `agent_fleet/worker.py` | Temporal worker setup with `GoogleAdkPlugin` |
| `agent_fleet/server.py` | FastAPI server — APIs, WebSocket, frontend |
| `agent_fleet/locations.py` | Las Vegas Strip locations and AI-Crew assignments |
| `frontend/index.html` | Single-file SPA — Leaflet map, agent panels, overlays |

## Commands

```bash
make lint    # ruff check + format check
make fmt     # ruff format (write)
make test    # pytest
make run     # start the demo
```
