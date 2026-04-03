"""Integration tests for Temporal workflows using time-skipping test environment."""

import asyncio
from datetime import timedelta

import pytest
from temporalio.testing import WorkflowEnvironment

from agent_fleet.locations import VENUES
from agent_fleet.models import (
    CrewRouteInput,
    CrewRouteOrder,
    CustomerChangeInput,
    MeltdownDemoInput,
)
from agent_fleet.simulation import fleet

TASK_QUEUE = "test-meltdown"


@pytest.fixture
async def env():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


async def _start_worker(env: WorkflowEnvironment):
    """Create a test worker connected to the time-skipping environment."""
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
        reason_about_assignment,
        register_assignment,
        sync_crew_disconnect,
        sync_crew_recovery_complete,
    )
    from agent_fleet.workflows import CrewRouteWorkflow, MeltdownDemoWorkflow

    return Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[MeltdownDemoWorkflow, CrewRouteWorkflow],
        activities=[
            generate_order,
            reason_about_assignment,
            register_assignment,
            navigate_to,
            pickup_orders,
            deliver_order,
            get_fleet_status,
            get_order_priorities,
            publish_agent_event,
            execute_customer_change,
            get_route_polyline,
            sync_crew_disconnect,
            sync_crew_recovery_complete,
        ],
    )


async def test_crew_route_completes_with_signal(env: WorkflowEnvironment):
    """CrewRouteWorkflow receives an order via signal, delivers it, then stops."""
    from agent_fleet.workflows import CrewRouteWorkflow

    venue = VENUES[0]  # MGM Grand

    # Register the order in fleet state so activities can update UI projection
    await fleet.register_order(
        order_id="order-1",
        hotel=venue["hotel"],
        label=f"{venue['hotel']} test delivery",
        priority="standard",
        servings=40,
        delivery_coords=venue["coords"],
        deadline_minutes=30,
    )
    await fleet.assign_order_to_crew("ai-crew-1", "order-1")

    worker = await _start_worker(env)
    async with worker:
        handle = await env.client.start_workflow(
            CrewRouteWorkflow.run,
            CrewRouteInput(crew_id="ai-crew-1"),
            id="test-route-ai-crew-1",
            task_queue=TASK_QUEUE,
        )

        # Signal the order
        await handle.signal(
            CrewRouteWorkflow.add_order,
            CrewRouteOrder(
                order_id="order-1",
                hotel=venue["hotel"],
                delivery_lat=venue["coords"].lat,
                delivery_lng=venue["coords"].lng,
            ),
        )

        # Let it process, then stop
        await asyncio.sleep(2)
        await handle.signal(CrewRouteWorkflow.stop)

        result = await handle.result()
        assert "ai-crew-1" in result
        assert "1 deliveries" in result or "completed" in result.lower()


async def test_meltdown_demo_completes(env: WorkflowEnvironment):
    """Full demo workflow generates orders, assigns crews, and completes."""
    from agent_fleet.workflows import MeltdownDemoWorkflow

    worker = await _start_worker(env)
    async with worker:
        result = await env.client.execute_workflow(
            MeltdownDemoWorkflow.run,
            MeltdownDemoInput(escalation_enabled=False, max_orders=2),
            id="test-meltdown-demo",
            task_queue=TASK_QUEUE,
            execution_timeout=timedelta(minutes=10),
        )
        assert "complete" in result.lower()


async def test_meltdown_demo_handles_customer_change(env: WorkflowEnvironment):
    from agent_fleet.workflows import MeltdownDemoWorkflow

    worker = await _start_worker(env)
    async with worker:
        handle = await env.client.start_workflow(
            MeltdownDemoWorkflow.run,
            MeltdownDemoInput(escalation_enabled=False, max_orders=2),
            id="test-meltdown-demo-customer-changes",
            task_queue=TASK_QUEUE,
            execution_timeout=timedelta(minutes=10),
        )

        # Wait for orders to be generated
        await asyncio.sleep(3)

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
