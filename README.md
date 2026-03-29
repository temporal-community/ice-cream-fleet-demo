<p align="center">
  <img src="https://github.com/google/adk-docs/raw/main/docs/assets/agent-development-kit.png" alt="Google Agent Development Kit" width="600">
</p>

# Meltdown — Ice Cream Delivery Fleet Demo

A conference demo showing **Google ADK** multi-agent reasoning with **Temporal** durable execution, visualized as an ice cream delivery fleet on the Las Vegas Strip.

Three AI-Crews deliver VIP ice cream orders to hotels on the Strip. When things go wrong — cooler malfunctions, service crashes, customer changes — AI agents coordinate the response while Temporal ensures nothing is lost.

## What It Demonstrates

| Scenario | What Happens | What It Shows |
|----------|-------------|---------------|
| **Crash Recovery** | Kill the worker mid-delivery, restart it | Temporal replays workflow history — AI-Crews resume from exact position |
| **Cooler Malfunction** | Trigger a cooler failure on an AI-Crew | Fleet Agent + Customer Agent assess in parallel, Resolver synthesizes a recovery plan |
| **Customer Change** | Submit an address change or cancellation | Human-in-the-loop approval with agent reasoning |

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
│  │  ├─ FleetAgent      ──┐    │ │
│  │  ├─ CustomerAgent   ──┤    │ │  ParallelAgent
│  │  └─ ResolverAgent  ←──┘    │ │  SequentialAgent
│  │      model=TemporalModel()  │ │
│  │      tools=activity_tool()  │ │
│  └─────────────────────────────┘ │
└──────────────────────────────────┘
           │
┌──────────▼───────────────────────┐
│     FastAPI + WebSocket          │
│     └─ Frontend (Leaflet map)    │
└──────────────────────────────────┘
```

**Key integration**: ADK agents run inline in the workflow. Every LLM call goes through `TemporalModel` (activity), every tool call goes through `activity_tool` (activity). If the worker crashes mid-reasoning, Temporal replays the activities from history — the agent resumes without re-calling the LLM.

## Prerequisites

- Python 3.11+
- [Temporal CLI](https://docs.temporal.io/cli) (`brew install temporal`)
- Google Gemini API key (for ADK agents; falls back to mock mode without it)

## Quick Start

### 1. Start Temporal dev server

```bash
temporal server start-dev
```

### 2. Install and run

```bash
pip install -e ".[dev]"
echo 'export GOOGLE_API_KEY="your-key-here"' > .env
./run.sh
```

### 3. Open the dashboard

Navigate to http://localhost:8080

## Demo Flow

1. **Start Deliveries** — 3 AI-Crews dispatch from Ice Cream Kitchen to MGM Grand, Caesars Palace, Mandalay Bay
2. **Crash Recovery** — Kill the service mid-flight → red overlay → restart → blue "Replaying..." overlay → AI-Crews resume
3. **Cooler Malfunction** — Trigger disruption → agents reason in parallel → recovery plan → orders rerouted
4. **Customer Change** — Submit a change → agent evaluates → approve/reject → order updated

## Key Files

| File | What it does |
|------|-------------|
| `agent_fleet/models.py` | Dataclass models for all Temporal payloads |
| `agent_fleet/simulation.py` | In-memory fleet state (singleton shared by worker + server) |
| `agent_fleet/activities.py` | Temporal activities — retryable units of work |
| `agent_fleet/workflows.py` | Temporal workflows — orchestration + signal handling |
| `agent_fleet/agents.py` | ADK agent definitions (Fleet + Customer + Resolver) |
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
