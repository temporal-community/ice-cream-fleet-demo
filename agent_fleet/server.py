"""
FastAPI server for the Meltdown ice cream delivery demo.

Serves the frontend, exposes fleet state via WebSocket, and provides
API endpoints for demo control (start, crash, restart, trigger disruption,
customer change, approve/reject).

Run with:
    python -m agent_fleet.server
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from temporalio.client import Client
from temporalio.service import RPCError

from agent_fleet.locations import DELIVERY_DESTINATIONS, WAREHOUSE, WAREHOUSE_LABEL
from agent_fleet.models import (
    AgentDisconnectInput,
    ConditionUpdate,
    CrewDisconnectInput,
    CustomerChangeInput,
    DemoEventConfig,
    DisruptionSignalInput,
    MeltdownDemoInput,
    OperatorDecision,
)
from agent_fleet.simulation import fleet
from agent_fleet.worker import TASK_QUEUE, TEMPORAL_ADDRESS, create_worker
from agent_fleet.workflows import MeltdownDemoWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Runtime state ---

_escalation_enabled = False
_worker_task: asyncio.Task | None = None
_temporal_client: Client | None = None
_was_crashed = False
_disruption_watcher: asyncio.Task | None = None


# --- Worker lifecycle ---


async def _start_worker() -> None:
    global _worker_task, _temporal_client, _was_crashed
    if _worker_task and not _worker_task.done():
        logger.warning("Worker already running")
        return

    _temporal_client = await Client.connect(TEMPORAL_ADDRESS)
    worker = await create_worker(_temporal_client)

    # If restarting after a crash, enter replay phase
    if _was_crashed:
        _was_crashed = False
        await fleet.mark_worker_restart()

        async def _clear_recovery():
            try:
                await asyncio.sleep(4)
                await fleet.mark_recovery_complete()
            except Exception as e:
                logger.error(f"Recovery clear failed: {e}")

        asyncio.create_task(_clear_recovery())

    async def _run():
        try:
            await worker.run()
        except asyncio.CancelledError:
            logger.info("Worker task cancelled")
        except Exception as e:
            logger.error(f"Worker error: {e}")

    _worker_task = asyncio.create_task(_run())
    logger.info("Worker started")


async def _stop_worker(*, crash: bool = False) -> None:
    """Stop the worker. Set crash=True to enter replay phase on restart."""
    global _worker_task, _was_crashed
    if _worker_task and not _worker_task.done():
        _was_crashed = crash
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
        _worker_task = None
        logger.info("Worker stopped (simulating service crash)")
    else:
        logger.warning("No worker running to stop")


async def _cancel_running_workflows() -> None:
    """Best-effort cancel of known workflow IDs."""
    if _temporal_client is None:
        return
    # Cancel main workflow and all AI-Crew routes
    workflow_ids = ["meltdown-demo"]
    for i in range(1, 4):
        workflow_ids.append(f"route-ai-crew-{i}")
    for wf_id in workflow_ids:
        try:
            handle = _temporal_client.get_workflow_handle(wf_id)
            await handle.cancel()
        except Exception:
            pass


# --- App lifecycle ---


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _start_worker()
    yield
    await _stop_worker()


app = FastAPI(title="Meltdown Ice Cream Delivery", lifespan=lifespan)


# --- Demo control endpoints ---


@app.post("/api/start")
async def start_demo():
    """Start the Meltdown demo workflow."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}

    try:
        handle = await _temporal_client.start_workflow(
            MeltdownDemoWorkflow.run,
            MeltdownDemoInput(escalation_enabled=_escalation_enabled),
            id="meltdown-demo",
            task_queue=TASK_QUEUE,
        )
    except RPCError as e:
        if "already started" in str(e).lower():
            return {
                "error": "Demo already running. Reset first.",
                "status": "already_running",
            }
        raise

    return {
        "status": "started",
        "workflow_id": handle.id,
        "escalation_enabled": _escalation_enabled,
    }


@app.post("/api/crash-service")
async def crash_service():
    """Stop the Temporal worker to simulate a service crash."""
    await _stop_worker(crash=True)
    return {
        "status": "service_crashed",
        "message": "Service crashed! Ice cream deliveries frozen. Restart to recover.",
    }


@app.post("/api/restart-service")
async def restart_service():
    """Restart the Temporal worker. Workflows resume from where they left off."""
    await _start_worker()
    return {
        "status": "service_restarted",
        "message": "Service back online. Temporal replaying workflows — deliveries resuming.",
    }


@app.post("/api/reset")
async def reset_demo():
    """Cancel running workflows and reset simulation state."""
    await _cancel_running_workflows()
    fleet.reset()
    return {"status": "reset"}


# --- Per-crew disconnect/reconnect ---


class CrewDisconnectRequest(BaseModel):
    crew_id: str = "ai-crew-1"


@app.post("/api/disconnect-crew")
async def disconnect_crew(body: CrewDisconnectRequest):
    """Disconnect a single crew — its activities will fail and Temporal will retry."""
    await fleet.disconnect_crew(body.crew_id)

    # Signal the workflow so it knows
    if _temporal_client is not None:
        try:
            handle = _temporal_client.get_workflow_handle("meltdown-demo")
            await handle.signal(
                MeltdownDemoWorkflow.crew_disconnected,
                CrewDisconnectInput(crew_id=body.crew_id),
            )
        except Exception as e:
            logger.error(f"Failed to signal crew disconnect: {e}")

    return {
        "status": "crew_disconnected",
        "crew_id": body.crew_id,
        "message": f"AI-Crew {body.crew_id} disconnected. Other crews continue delivering.",
    }


@app.post("/api/reconnect-crew")
async def reconnect_crew(body: CrewDisconnectRequest):
    """Reconnect a crew — Temporal retries its activities and it resumes."""
    await fleet.reconnect_crew(body.crew_id)

    # Signal the workflow
    if _temporal_client is not None:
        try:
            handle = _temporal_client.get_workflow_handle("meltdown-demo")
            await handle.signal(
                MeltdownDemoWorkflow.crew_reconnected,
                CrewDisconnectInput(crew_id=body.crew_id),
            )
        except Exception as e:
            logger.error(f"Failed to signal crew reconnect: {e}")

    # Clear recovery phase after a delay (visual replay indicator)
    async def _clear_crew_recovery():
        try:
            await asyncio.sleep(3)
            await fleet.mark_crew_recovery_complete(body.crew_id)
        except Exception as e:
            logger.error(f"Crew recovery clear failed: {e}")

    asyncio.create_task(_clear_crew_recovery())

    return {
        "status": "crew_reconnected",
        "crew_id": body.crew_id,
        "message": (
            f"AI-Crew {body.crew_id} reconnecting. "
            f"Temporal replaying — crew will resume delivery."
        ),
    }


# --- Per-agent disconnect/reconnect ---


class AgentDisconnectRequest(BaseModel):
    agent_name: str = "fleet_agent"


@app.post("/api/disconnect-agent")
async def disconnect_agent(body: AgentDisconnectRequest):
    """Take a specific agent offline. Other agents compensate."""
    await fleet.disconnect_agent(body.agent_name)

    # Signal the workflow
    if _temporal_client is not None:
        try:
            handle = _temporal_client.get_workflow_handle("meltdown-demo")
            await handle.signal(
                MeltdownDemoWorkflow.agent_disconnected,
                AgentDisconnectInput(agent_name=body.agent_name),
            )
        except Exception as e:
            logger.error(f"Failed to signal agent disconnect: {e}")

    return {
        "status": "agent_disconnected",
        "agent_name": body.agent_name,
        "message": f"{body.agent_name} is offline. Other agents will compensate.",
    }


@app.post("/api/reconnect-agent")
async def reconnect_agent(body: AgentDisconnectRequest):
    """Bring a specific agent back online."""
    await fleet.reconnect_agent(body.agent_name)

    # Signal the workflow
    if _temporal_client is not None:
        try:
            handle = _temporal_client.get_workflow_handle("meltdown-demo")
            await handle.signal(
                MeltdownDemoWorkflow.agent_reconnected,
                AgentDisconnectInput(agent_name=body.agent_name),
            )
        except Exception as e:
            logger.error(f"Failed to signal agent reconnect: {e}")

    await fleet.publish_agent_event(
        body.agent_name,
        "reconnected",
        f"{body.agent_name} is back online and ready for reasoning.",
        summary=f"{body.agent_name} reconnected",
    )

    return {
        "status": "agent_reconnected",
        "agent_name": body.agent_name,
        "message": f"{body.agent_name} is back online.",
    }


# --- Disruption endpoint ---


class TriggerDisruptionRequest(BaseModel):
    crew_id: str = "ai-crew-1"
    at_nav_step: int = 5


@app.post("/api/trigger-disruption")
async def trigger_disruption(body: TriggerDisruptionRequest):
    """Arm a cooler malfunction and auto-signal the workflow when it fires."""
    config = DemoEventConfig(
        cooler_malfunction_at_nav_step=body.at_nav_step,
        cooler_malfunction_crew=body.crew_id,
        enabled=True,
    )
    await fleet.set_demo_events(config)

    # Start background watcher that signals the workflow when the
    # malfunction actually triggers (at the configured nav step)
    global _disruption_watcher
    if _disruption_watcher and not _disruption_watcher.done():
        logger.warning("Disruption watcher already running — skipping duplicate")
    else:
        _disruption_watcher = asyncio.create_task(_watch_for_disruption())

    return {
        "status": "disruption_armed",
        "crew_id": body.crew_id,
        "triggers_at_step": body.at_nav_step,
    }


async def _watch_for_disruption() -> None:
    """Poll simulation briefly until cooler malfunction fires, then signal workflow."""
    try:
        for _ in range(120):  # Up to 60 seconds
            await asyncio.sleep(0.5)
            result = await fleet.check_disruption()
            if result["disruption_detected"]:
                if _temporal_client is not None:
                    try:
                        handle = _temporal_client.get_workflow_handle("meltdown-demo")
                        await handle.signal(
                            MeltdownDemoWorkflow.disruption_detected,
                            DisruptionSignalInput(
                                crew_id=result["crew_id"],
                                cooler_temp_f=result["cooler_temp_f"],
                                affected_order_ids=result["affected_order_ids"],
                                description=result["description"],
                            ),
                        )
                        logger.info("Disruption signal sent to workflow")
                    except Exception as e:
                        logger.error(f"Failed to signal disruption: {e}")
                return
        logger.warning("Disruption watcher timed out — malfunction never triggered")
    except Exception as e:
        logger.error(f"Disruption watcher failed: {e}")


# --- Customer change endpoints ---


class CustomerChangeRequest(BaseModel):
    order_id: str
    change_type: str = "address_change"  # "address_change" or "cancel"
    new_details: str = ""
    new_lat: float | None = None
    new_lng: float | None = None


@app.post("/api/customer-change")
async def submit_customer_change(body: CustomerChangeRequest):
    """Submit a customer change request (triggers human-in-the-loop)."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}

    change = CustomerChangeInput(
        order_id=body.order_id,
        change_type=body.change_type,
        new_details=body.new_details,
        new_lat=body.new_lat,
        new_lng=body.new_lng,
    )

    try:
        handle = _temporal_client.get_workflow_handle("meltdown-demo")
        await handle.signal(MeltdownDemoWorkflow.customer_change, change)
    except RPCError as e:
        return {"error": f"Failed to signal workflow: {e}"}

    return {
        "status": "change_submitted",
        "order_id": body.order_id,
        "change_type": body.change_type,
    }


class ChangeDecisionRequest(BaseModel):
    approved: bool


@app.post("/api/approve-change")
async def approve_change(body: ChangeDecisionRequest):
    """Approve or reject a pending customer change."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}

    try:
        handle = _temporal_client.get_workflow_handle("meltdown-demo")
        await handle.signal(MeltdownDemoWorkflow.change_approved, body.approved)
    except RPCError as e:
        return {"error": f"Failed to signal workflow: {e}"}

    decision = "approved" if body.approved else "rejected"
    return {"status": f"change_{decision}"}


# --- Operator decision endpoint ---


class OperatorDecisionRequest(BaseModel):
    action: str  # "approve" or "reject"
    notes: str = ""


@app.post("/api/operator-decision")
async def submit_operator_decision(body: OperatorDecisionRequest):
    """Submit operator approval/rejection for a pending recommendation."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}
    try:
        handle = _temporal_client.get_workflow_handle("meltdown-demo")
        await handle.signal(
            MeltdownDemoWorkflow.operator_decision,
            OperatorDecision(action=body.action, notes=body.notes),
        )
    except RPCError as e:
        return {"error": f"Failed to signal workflow: {e}"}
    return {"status": f"decision_{body.action}"}


# --- Inject conditions endpoint ---


class InjectConditionsRequest(BaseModel):
    description: str = "Traffic delay reported"
    crew_id: str | None = None
    details: str = ""


@app.post("/api/inject-conditions")
async def inject_conditions(body: InjectConditionsRequest):
    """Inject new conditions to trigger agent re-evaluation."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}
    try:
        handle = _temporal_client.get_workflow_handle("meltdown-demo")
        await handle.signal(
            MeltdownDemoWorkflow.updated_conditions,
            ConditionUpdate(
                description=body.description,
                crew_id=body.crew_id,
                details=body.details,
            ),
        )
    except RPCError as e:
        return {"error": f"Failed to signal workflow: {e}"}
    return {"status": "conditions_injected"}


# --- Demo config endpoints ---


@app.post("/api/toggle-escalation")
async def toggle_escalation():
    """Toggle escalation mode (customer change / human-in-the-loop)."""
    global _escalation_enabled
    _escalation_enabled = not _escalation_enabled
    return {
        "status": "escalation_enabled" if _escalation_enabled else "escalation_disabled",
        "escalation_enabled": _escalation_enabled,
    }


class DemoEventConfigRequest(BaseModel):
    cooler_malfunction_at_nav_step: int | None = None
    cooler_malfunction_crew: str = "ai-crew-1"
    enabled: bool = False


@app.post("/api/demo-events")
async def configure_demo_events(config: DemoEventConfigRequest):
    """Configure demo event injection."""
    demo_config = DemoEventConfig(
        cooler_malfunction_at_nav_step=config.cooler_malfunction_at_nav_step,
        cooler_malfunction_crew=config.cooler_malfunction_crew,
        enabled=config.enabled,
    )
    await fleet.set_demo_events(demo_config)
    return {"status": "configured", "config": config.model_dump()}


# --- State query endpoints ---


@app.get("/api/state")
async def get_state():
    """Get current fleet state as JSON."""
    return await fleet.snapshot()


@app.get("/api/locations")
async def get_locations():
    """Return kitchen and hotel locations for the frontend map."""
    return {
        "warehouse": {
            "lat": WAREHOUSE.lat,
            "lng": WAREHOUSE.lng,
            "label": WAREHOUSE_LABEL,
        },
        "destinations": {
            oid: {
                "lat": info["coords"].lat,
                "lng": info["coords"].lng,
                "label": info["map_label"],
                "hotel": info["hotel"],
            }
            for oid, info in DELIVERY_DESTINATIONS.items()
        },
    }


# --- WebSocket for real-time state updates ---


@app.websocket("/ws")
async def websocket_state(ws: WebSocket):
    """Push fleet state to the frontend every 300ms."""
    await ws.accept()
    last_snapshot: str | None = None
    try:
        while True:
            data = json.dumps(await fleet.snapshot())
            if data != last_snapshot:
                await ws.send_text(data)
                last_snapshot = data
            await asyncio.sleep(0.3)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug(f"WebSocket closed: {e}")


# --- Frontend ---

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


@app.get("/")
async def index():
    return FileResponse(FRONTEND_DIR / "index.html")


app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="frontend-static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8080, log_level="info")
