"""
Temporal worker entry point for the Meltdown demo (live mode).

Runs in the same process as the FastAPI server (started from server.py).
Three workers on three task queues:
  - meltdown-workflows: workflows only (no activities, dedicated to replay)
  - meltdown-delivery: navigation, pickup, delivery, customer changes
  - meltdown-agents: LLM/ADK tool calls (rate-limited, max 5 concurrent)

Can also be run standalone:
    python -m agent_fleet.worker
"""

from __future__ import annotations

import asyncio
import logging

from temporalio.client import Client
from temporalio.contrib.google_adk_agents import GoogleAdkPlugin
from temporalio.worker import Worker

from agent_fleet.activities import (
    deliver_order,
    execute_customer_change,
    generate_order,
    get_fleet_status,
    get_order_priorities,
    get_route_polyline,
    navigate_to,
    pickup_orders,
    publish_agent_event,
    register_assignment,
    tool_get_fleet_status,
    tool_get_order_priorities,
    tool_get_route_info,
    tool_publish_agent_event,
    tool_search_hotel_context,
)
from agent_fleet.config import TEMPORAL_ADDRESS
from agent_fleet.queues import AGENTS_QUEUE, DELIVERY_QUEUE, WORKFLOWS_QUEUE
from agent_fleet.workflows import DriverRouteWorkflow, MeltdownDemoWorkflow, OrderGenerationWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Quiet Temporal SDK internals — "Timer started", replay chatter, etc.
logging.getLogger("temporalio.worker").setLevel(logging.WARNING)
logging.getLogger("temporalio.activity").setLevel(logging.WARNING)
logging.getLogger("temporalio.workflow").setLevel(logging.WARNING)


def create_workflow_worker(client: Client) -> Worker:
    """Workflow-only worker — no activities, dedicated to replay.

    GoogleAdkPlugin is needed here for sandbox passthroughs (google.adk,
    google.genai) and deterministic runtime (uuid, time) during replay.
    """
    return Worker(
        client,
        task_queue=WORKFLOWS_QUEUE,
        workflows=[MeltdownDemoWorkflow, DriverRouteWorkflow, OrderGenerationWorkflow],
        plugins=[GoogleAdkPlugin()],
    )


def create_delivery_worker(client: Client) -> Worker:
    """Navigation, pickup, delivery, order generation, and customer change activities."""
    return Worker(
        client,
        task_queue=DELIVERY_QUEUE,
        activities=[
            generate_order,
            navigate_to,
            pickup_orders,
            deliver_order,
            execute_customer_change,
            get_route_polyline,
            get_fleet_status,
            get_order_priorities,
            publish_agent_event,
        ],
        max_concurrent_activities=20,
    )


def create_agents_worker(client: Client) -> Worker:
    """ADK/LLM activities — rate-limited.

    GoogleAdkPlugin registers the invoke_model activity that TemporalModel
    routes LLM calls to.
    """
    return Worker(
        client,
        task_queue=AGENTS_QUEUE,
        activities=[
            register_assignment,
            tool_get_fleet_status,
            tool_get_order_priorities,
            tool_publish_agent_event,
            tool_get_route_info,
            tool_search_hotel_context,
        ],
        max_concurrent_activities=5,
        plugins=[GoogleAdkPlugin()],
    )


async def create_worker(client: Client) -> list[Worker]:
    """Create all three workers. Returns list for server.py to manage."""
    logger.info("Starting workers (LIVE MODE)")
    return [
        create_workflow_worker(client),
        create_delivery_worker(client),
        create_agents_worker(client),
    ]


async def run_worker() -> None:
    """Connect to Temporal and run all three workers until interrupted."""
    logger.info(f"Connecting to Temporal at {TEMPORAL_ADDRESS}...")
    from temporalio.contrib.pydantic import PydanticPayloadConverter
    from temporalio.converter import DataConverter

    client = await Client.connect(
        TEMPORAL_ADDRESS,
        data_converter=DataConverter(
            payload_converter_class=PydanticPayloadConverter,
        ),
    )
    workers = await create_worker(client)
    logger.info(f"Workers started on queues: {WORKFLOWS_QUEUE}, {DELIVERY_QUEUE}, {AGENTS_QUEUE}")
    await asyncio.gather(*[w.run() for w in workers])


if __name__ == "__main__":
    asyncio.run(run_worker())
