<p align="center">
  <img src="https://github.com/google/adk-docs/raw/main/docs/assets/agent-development-kit.png" alt="Google Agent Development Kit" width="600">
</p>

# Meltdown — Ice Cream Delivery Fleet Demo

A conference demo showing **Google ADK** multi-agent reasoning with **Temporal** durable execution, visualized as an ice cream delivery fleet on the Las Vegas Strip.

Orders auto-generate on a timer from Las Vegas Strip venues. AI agents reason about each order — evaluating crew positions, capacity, and priority — then assign it to the best crew. When things go wrong — crew disconnects, agent failures, customer changes — Temporal ensures nothing is lost.

## What It Demonstrates

| Scenario | What Happens | What It Shows |
|----------|-------------|---------------|
| **Agent Disconnect** | Take an agent offline mid-reasoning | ADK degrades gracefully — Resolver compensates with available data. Temporal records every step that completed. Two resilience layers. |
| **Crew Disconnect** | Take a single AI-Crew offline mid-delivery | Temporal retries the navigation activity indefinitely until reconnect — crew resumes exactly where it stopped |
| **Customer Change** | Submit an address change or cancellation | Human-in-the-loop: workflow pauses on `wait_condition`, resumes immediately on signal — no polling, no timeout |

## Architecture

```
┌──────────────────────────────────┐
│         Temporal Server          │
│   (workflow state + replay)      │
└──────────┬───────────────────────┘
           │
┌──────────▼────────────────────────────────────────────┐
│  Python process (3 workers, 1 FleetState singleton)   │
│                                                       │
│  meltdown-orchestration worker                        │
│  ├─ MeltdownDemoWorkflow                              │
│  │    ├─ generate_order()       timer-based, up to 20 │
│  │    ├─ reason_about_assignment() → AGENTS queue     │
│  │    └─ CrewRouteWorkflow x3   child workflows       │
│  └─ CrewRouteWorkflow                                 │
│       ├─ navigate_to()  → DELIVERY queue (heartbeats) │
│       ├─ pickup_orders() → DELIVERY queue             │
│       └─ deliver_order() → DELIVERY queue             │
│                                                       │
│  meltdown-delivery worker (max 20 concurrent)         │
│  └─ navigation, pickup, deliver, customer changes     │
│                                                       │
│  meltdown-agents worker (max 5 concurrent)            │
│  └─ ADK assignment pipeline (via TemporalModel)       │
│       ParallelAgent:                                  │
│       ├─ Fleet Agent    tool_get_fleet_status         │
│       │                 tool_get_route_info (Maps)    │
│       └─ Customer Agent tool_get_order_priorities     │
│                         tool_search_hotel_context     │
│       Assignment Resolver → tool_submit_assignment    │
│       model=TemporalModel(), tools=activity_tool()    │
└───────────────────────────────────────────────────────┘
           │
┌──────────▼───────────────────────┐
│     FastAPI + WebSocket          │
│     └─ Frontend (Leaflet map)    │
└──────────────────────────────────┘
```

**Key integration**: ADK agents run inside a Temporal activity (`reason_about_assignment`). Every LLM call goes through `TemporalModel` (becomes an `invoke_model` activity), every tool call goes through `activity_tool` (becomes a Temporal activity). If the worker restarts mid-reasoning, Temporal replays from history — the agent resumes without re-calling the LLM.

**3-queue separation**: LLM calls are slow (3–5s). Without separate queues, assignment requests could starve navigation activities and cause heartbeat timeouts. The agents queue caps at 5 concurrent; delivery at 20. All three workers share the `FleetState` singleton because they run in the same process.

### Activity-backed tools

| Tool | Agent | Purpose |
|------|-------|---------|
| **Google Maps Directions** (`tool_get_route_info`) | Fleet Agent | Driving routes and ETAs for crew selection |
| **Hotel Search** (`tool_search_hotel_context`) | Customer Agent | Live hotel event context — conferences, VIP bookings |

Both are wrapped with `activity_tool()` and routed to the agents queue. Results are recorded in Temporal history — if the worker restarts mid-call, they replay from the log. Hotel Search calls Google Custom Search API when `GOOGLE_CSE_ID` is set, otherwise returns curated mock data.

## Prerequisites

- Python 3.11+
- [Temporal CLI](https://docs.temporal.io/cli) (`brew install temporal`)
- Google Gemini API key (for ADK agents; falls back to mock mode without it)
- Google Maps API key (optional — falls back to mock route data)
- Google Custom Search Engine ID (optional — falls back to curated hotel data)

All API keys fall back gracefully: without `GOOGLE_API_KEY`, agents use deterministic mock reasoning. Without `GOOGLE_MAPS_API_KEY`, route checks use calculated distance/ETA estimates. Without `GOOGLE_CSE_ID`, hotel research uses curated Las Vegas hotel context.

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
echo 'export GOOGLE_CSE_ID="your-cse-id"' >> .env  # optional
./run.sh
```

### 3. Open the dashboard

Navigate to http://localhost:8080

## Demo Flow

1. **Start Deliveries** — Orders auto-generate every 15s. AI agents reason per-order (Fleet Agent checks positions/capacity, Customer Agent evaluates priority) and assign to the best crew. Crews continuously pick up from Frosty's Ice Cream and deliver.
2. **Agent Disconnect** — Take an agent offline → Resolver compensates with available data → reconnect → full reasoning resumes
3. **Crew Disconnect** — Select an AI-Crew → disconnect → activities retry until reconnect → seamless resume
4. **Customer Change** — Submit a change → workflow pauses waiting for approval → approve/reject → order updated or discarded

## Key Files

| File | What it does |
|------|-------------|
| `agent_fleet/models.py` | Dataclass models for all Temporal payloads |
| `agent_fleet/simulation.py` | In-memory fleet state (singleton shared by worker + server) |
| `agent_fleet/activities.py` | Temporal activities — navigation, delivery, Maps API, agent tools |
| `agent_fleet/workflows.py` | Temporal workflows — orchestration, signals, queries |
| `agent_fleet/agents.py` | ADK agent composition — Fleet, Customer, Assignment Resolver |
| `agent_fleet/queues.py` | Task queue name constants (orchestration / delivery / agents) |
| `agent_fleet/worker.py` | Three Temporal workers on three task queues, all in-process |
| `agent_fleet/server.py` | FastAPI server — APIs, WebSocket, frontend |
| `agent_fleet/locations.py` | Las Vegas Strip venue pool and random order generation |
| `frontend/index.html` | Single-file SPA — Leaflet map, agent panels, overlays |

## Commands

```bash
make lint    # ruff check + format check
make fmt     # ruff format (write)
make test    # pytest
make run     # start the demo
```
