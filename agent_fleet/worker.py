"""
Temporal worker entry point for the Meltdown demo.

Runs in the same process as the FastAPI server (started from server.py).
Can also be run standalone:
    python -m agent_fleet.worker
"""

from __future__ import annotations

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

try:
    from temporalio.contrib.google_adk_agents import GoogleAdkPlugin

    _ADK_AVAILABLE = True
except ImportError:
    GoogleAdkPlugin = None
    _ADK_AVAILABLE = False

from agent_fleet.activities import (
    check_for_disruption,
    deliver_order,
    execute_customer_change,
    execute_recovery,
    find_backup_crew,
    generate_order,
    get_fleet_status,
    get_order_priorities,
    get_route_polyline,
    navigate_to,
    pickup_orders,
    publish_agent_event,
    reason_about_assignment,
    register_assignment,
    resolve_disruption_mock,
    tool_get_fleet_status,
    tool_get_order_priorities,
    tool_get_route_info,
    tool_publish_agent_event,
    tool_search_hotel_context,
)
from agent_fleet.workflows import CrewRouteWorkflow, MeltdownDemoWorkflow

TASK_QUEUE = "meltdown-fleet"
TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
MOCK_MODE = not os.environ.get("GOOGLE_API_KEY") or not _ADK_AVAILABLE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def create_worker(client: Client) -> Worker:
    """Create a Temporal worker with all workflows and activities registered."""
    if MOCK_MODE:
        logger.info("MOCK MODE: running without Google ADK")

    kwargs = dict(
        task_queue=TASK_QUEUE,
        workflows=[
            MeltdownDemoWorkflow,
            CrewRouteWorkflow,
        ],
        activities=[
            generate_order,
            reason_about_assignment,
            navigate_to,
            pickup_orders,
            deliver_order,
            get_fleet_status,
            get_order_priorities,
            publish_agent_event,
            check_for_disruption,
            execute_recovery,
            execute_customer_change,
            find_backup_crew,
            get_route_polyline,
            register_assignment,
            resolve_disruption_mock,
            tool_get_fleet_status,
            tool_get_order_priorities,
            tool_get_route_info,
            tool_publish_agent_event,
            tool_search_hotel_context,
        ],
    )

    if not MOCK_MODE and GoogleAdkPlugin is not None:
        kwargs["plugins"] = [GoogleAdkPlugin()]

    return Worker(client, **kwargs)


async def run_worker() -> None:
    """Connect to Temporal and run the worker until interrupted."""
    logger.info(f"Connecting to Temporal at {TEMPORAL_ADDRESS}...")
    client = await Client.connect(TEMPORAL_ADDRESS)
    worker = await create_worker(client)
    logger.info(f"Worker started on task queue '{TASK_QUEUE}'")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(run_worker())
