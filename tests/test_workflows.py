"""Integration tests for Temporal workflows using time-skipping test environment.

DriverRouteWorkflow tests use mock activities (no Gemini needed).
Full MeltdownDemoWorkflow tests require GOOGLE_API_KEY for live ADK.
"""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import timedelta

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agent_fleet.activities import (
    deliver_order,
    execute_customer_change,
    generate_order,
    get_fleet_status,
    get_order_priorities,
    navigate_to,
    pickup_orders,
    publish_agent_event,
    register_assignment,
)
from agent_fleet.locations import VENUES
from agent_fleet.mock.activities import (
    mock_get_route_polyline,
    mock_tool_get_route_info,
)
from agent_fleet.models import (
    CustomerChangeInput,
    DriverRouteInput,
    DriverRouteOrder,
    MeltdownDemoInput,
)
from agent_fleet.queues import AGENTS_QUEUE, DELIVERY_QUEUE, WORKFLOWS_QUEUE
from agent_fleet.simulation import fleet


@pytest.fixture
async def env():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


@asynccontextmanager
async def run_delivery_workers(env: WorkflowEnvironment):
    """Start workers for DriverRouteWorkflow tests (no ADK needed)."""
    from agent_fleet.workflows import DriverRouteWorkflow, OrderGenerationWorkflow

    workflow_worker = Worker(
        env.client,
        task_queue=WORKFLOWS_QUEUE,
        workflows=[DriverRouteWorkflow, OrderGenerationWorkflow],
    )
    delivery_worker = Worker(
        env.client,
        task_queue=DELIVERY_QUEUE,
        activities=[
            generate_order,
            navigate_to,
            pickup_orders,
            deliver_order,
            execute_customer_change,
            mock_get_route_polyline,
            get_fleet_status,
            get_order_priorities,
            publish_agent_event,
        ],
    )

    async with workflow_worker, delivery_worker:
        yield


async def test_driver_route_completes_with_signal(env: WorkflowEnvironment):
    """DriverRouteWorkflow receives an order via signal, delivers it, then stops."""
    from agent_fleet.workflows import DriverRouteWorkflow

    venue = VENUES[0]  # MGM Grand

    await fleet.register_order(
        order_id="order-1",
        hotel=venue["hotel"],
        label=f"{venue['hotel']} test delivery",
        priority="standard",
        servings=40,
        delivery_coords=venue["coords"],
        deadline_minutes=30,
    )
    await fleet.assign_order_to_driver("ai-driver-1", "order-1")

    async with run_delivery_workers(env):
        handle = await env.client.start_workflow(
            DriverRouteWorkflow.run,
            DriverRouteInput(driver_id="ai-driver-1"),
            id="test-route-ai-driver-1",
            task_queue=WORKFLOWS_QUEUE,
        )

        await handle.signal(
            DriverRouteWorkflow.add_order,
            DriverRouteOrder(
                order_id="order-1",
                hotel=venue["hotel"],
                delivery_lat=venue["coords"].lat,
                delivery_lng=venue["coords"].lng,
            ),
        )

        await asyncio.sleep(2)
        await handle.signal(DriverRouteWorkflow.stop)

        result = await handle.result()
        assert "ai-driver-1" in result
        assert "1 deliveries" in result or "completed" in result.lower()


@pytest.mark.skipif(
    not os.environ.get("GOOGLE_API_KEY"),
    reason="Requires GOOGLE_API_KEY for live ADK agents",
)
async def test_meltdown_demo_completes(env: WorkflowEnvironment):
    """Full demo with live ADK agents — only runs when API key is set."""
    from temporalio.contrib.google_adk_agents import GoogleAdkPlugin
    from temporalio.contrib.pydantic import PydanticPayloadConverter
    from temporalio.converter import DataConverter

    from agent_fleet.workflows import (
        DriverRouteWorkflow,
        MeltdownDemoWorkflow,
        OrderGenerationWorkflow,
    )

    # Live mode needs PydanticPayloadConverter for LlmResponse serialization
    live_client = await env.client.connect(
        env.client.service_client.config.target_host,
        data_converter=DataConverter(
            payload_converter_class=PydanticPayloadConverter,
        ),
    )

    workflow_worker = Worker(
        live_client,
        task_queue=WORKFLOWS_QUEUE,
        workflows=[MeltdownDemoWorkflow, DriverRouteWorkflow, OrderGenerationWorkflow],
        plugins=[GoogleAdkPlugin()],
    )
    delivery_worker = Worker(
        live_client,
        task_queue=DELIVERY_QUEUE,
        activities=[
            generate_order,
            navigate_to,
            pickup_orders,
            deliver_order,
            execute_customer_change,
            get_fleet_status,
            get_order_priorities,
            publish_agent_event,
        ],
        max_concurrent_activities=20,
    )
    agents_worker = Worker(
        live_client,
        task_queue=AGENTS_QUEUE,
        activities=[
            register_assignment,
            mock_tool_get_route_info,
        ],
        max_concurrent_activities=5,
        plugins=[GoogleAdkPlugin()],
    )

    async with workflow_worker, delivery_worker, agents_worker:
        result = await live_client.execute_workflow(
            MeltdownDemoWorkflow.run,
            MeltdownDemoInput(escalation_enabled=False, max_orders=2),
            id="meltdown-demo",
            task_queue=WORKFLOWS_QUEUE,
            execution_timeout=timedelta(minutes=10),
        )
        assert "complete" in result.lower()


@pytest.mark.skipif(
    not os.environ.get("GOOGLE_API_KEY"),
    reason="Requires GOOGLE_API_KEY for live ADK agents",
)
async def test_meltdown_demo_handles_customer_change(env: WorkflowEnvironment):
    """Customer change with live ADK — only runs when API key is set."""
    from temporalio.contrib.google_adk_agents import GoogleAdkPlugin
    from temporalio.contrib.pydantic import PydanticPayloadConverter
    from temporalio.converter import DataConverter

    from agent_fleet.workflows import (
        DriverRouteWorkflow,
        MeltdownDemoWorkflow,
        OrderGenerationWorkflow,
    )

    live_client = await env.client.connect(
        env.client.service_client.config.target_host,
        data_converter=DataConverter(
            payload_converter_class=PydanticPayloadConverter,
        ),
    )

    workflow_worker = Worker(
        live_client,
        task_queue=WORKFLOWS_QUEUE,
        workflows=[MeltdownDemoWorkflow, DriverRouteWorkflow, OrderGenerationWorkflow],
        plugins=[GoogleAdkPlugin()],
    )
    delivery_worker = Worker(
        live_client,
        task_queue=DELIVERY_QUEUE,
        activities=[
            generate_order,
            navigate_to,
            pickup_orders,
            deliver_order,
            execute_customer_change,
            get_fleet_status,
            get_order_priorities,
            publish_agent_event,
        ],
    )
    agents_worker = Worker(
        live_client,
        task_queue=AGENTS_QUEUE,
        activities=[
            register_assignment,
            mock_tool_get_route_info,
        ],
        max_concurrent_activities=5,
        plugins=[GoogleAdkPlugin()],
    )

    async with workflow_worker, delivery_worker, agents_worker:
        handle = await live_client.start_workflow(
            MeltdownDemoWorkflow.run,
            MeltdownDemoInput(escalation_enabled=False, max_orders=4),
            id="meltdown-demo",
            task_queue=WORKFLOWS_QUEUE,
            execution_timeout=timedelta(minutes=10),
        )

        await asyncio.sleep(5)

        await handle.signal(
            MeltdownDemoWorkflow.customer_change,
            CustomerChangeInput(
                order_id="order-1",
                change_type="address_change",
                new_details="Move to alternate loading bay",
                new_lat=36.1111,
                new_lng=-115.1666,
            ),
        )

        await handle.signal(MeltdownDemoWorkflow.change_approved, True)

        result = await handle.result()
        assert "complete" in result.lower()
