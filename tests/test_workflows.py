"""Integration tests for Temporal workflows using time-skipping test environment."""

import pytest
from temporalio.testing import WorkflowEnvironment

from agent_fleet.locations import DELIVERY_DESTINATIONS
from agent_fleet.models import (
    CrewRouteInput,
    CrewRouteOrder,
    CustomerChangeInput,
    DisruptionSignalInput,
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
        assign_orders,
        check_for_disruption,
        coordinate_delivery_plan,
        deliver_order,
        execute_customer_change,
        execute_recovery,
        find_backup_crew,
        get_fleet_status,
        get_order_priorities,
        navigate_to,
        pickup_orders,
        publish_agent_event,
        resolve_disruption_mock,
        tool_get_fleet_status,
        tool_get_order_priorities,
        tool_publish_agent_event,
    )
    from agent_fleet.workflows import CrewRouteWorkflow, MeltdownDemoWorkflow

    return Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[MeltdownDemoWorkflow, CrewRouteWorkflow],
        activities=[
            assign_orders,
            coordinate_delivery_plan,
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
            resolve_disruption_mock,
            tool_get_fleet_status,
            tool_get_order_priorities,
            tool_publish_agent_event,
        ],
    )


async def test_crew_route_completes(env: WorkflowEnvironment):
    from agent_fleet.workflows import CrewRouteWorkflow

    worker = await _start_worker(env)
    async with worker:
        result = await env.client.execute_workflow(
            CrewRouteWorkflow.run,
            CrewRouteInput(
                crew_id="ai-crew-1",
                orders=[
                    CrewRouteOrder(
                        order_id="order-1",
                        hotel=DELIVERY_DESTINATIONS["order-1"]["hotel"],
                        delivery_lat=DELIVERY_DESTINATIONS["order-1"]["coords"].lat,
                        delivery_lng=DELIVERY_DESTINATIONS["order-1"]["coords"].lng,
                    ),
                ],
            ),
            id="test-route-ai-crew-1",
            task_queue=TASK_QUEUE,
        )
        assert "ai-crew-1" in result
        assert "completed" in result.lower() or "1 deliveries" in result


async def test_meltdown_demo_completes(env: WorkflowEnvironment):
    from agent_fleet.workflows import MeltdownDemoWorkflow

    worker = await _start_worker(env)
    async with worker:
        result = await env.client.execute_workflow(
            MeltdownDemoWorkflow.run,
            MeltdownDemoInput(escalation_enabled=False),
            id="test-meltdown-demo",
            task_queue=TASK_QUEUE,
        )
        assert "complete" in result.lower()


async def test_crew_route_returns_to_base_when_signaled(env: WorkflowEnvironment):
    from agent_fleet.workflows import CrewRouteWorkflow

    worker = await _start_worker(env)
    async with worker:
        handle = await env.client.start_workflow(
            CrewRouteWorkflow.run,
            CrewRouteInput(
                crew_id="ai-crew-1",
                orders=[
                    CrewRouteOrder(
                        order_id="order-1",
                        hotel=DELIVERY_DESTINATIONS["order-1"]["hotel"],
                        delivery_lat=DELIVERY_DESTINATIONS["order-1"]["coords"].lat,
                        delivery_lng=DELIVERY_DESTINATIONS["order-1"]["coords"].lng,
                    ),
                ],
            ),
            id="test-route-return-to-base",
            task_queue=TASK_QUEUE,
        )

        await handle.signal(CrewRouteWorkflow.return_to_base)
        result = await handle.result()

        assert "returned to base" in result.lower()


async def test_crew_route_delivers_extra_order_added_by_signal(env: WorkflowEnvironment):
    from agent_fleet.workflows import CrewRouteWorkflow

    worker = await _start_worker(env)
    async with worker:
        handle = await env.client.start_workflow(
            CrewRouteWorkflow.run,
            CrewRouteInput(
                crew_id="ai-crew-2",
                orders=[
                    CrewRouteOrder(
                        order_id="order-2",
                        hotel=DELIVERY_DESTINATIONS["order-2"]["hotel"],
                        delivery_lat=DELIVERY_DESTINATIONS["order-2"]["coords"].lat,
                        delivery_lng=DELIVERY_DESTINATIONS["order-2"]["coords"].lng,
                    ),
                ],
            ),
            id="test-route-extra-order",
            task_queue=TASK_QUEUE,
        )

        await handle.signal(
            CrewRouteWorkflow.add_order,
            CrewRouteOrder(
                order_id="order-3",
                hotel=DELIVERY_DESTINATIONS["order-3"]["hotel"],
                delivery_lat=DELIVERY_DESTINATIONS["order-3"]["coords"].lat,
                delivery_lng=DELIVERY_DESTINATIONS["order-3"]["coords"].lng,
            ),
        )
        result = await handle.result()

        assert "2 deliveries" in result
        order = await fleet.get_order("order-3")
        assert order.status.value == "delivered"


async def test_meltdown_demo_queues_customer_change_approvals_fifo(env: WorkflowEnvironment):
    from agent_fleet.workflows import MeltdownDemoWorkflow

    worker = await _start_worker(env)
    async with worker:
        handle = await env.client.start_workflow(
            MeltdownDemoWorkflow.run,
            MeltdownDemoInput(escalation_enabled=False),
            id="test-meltdown-demo-customer-changes",
            task_queue=TASK_QUEUE,
        )

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
        await handle.signal(
            MeltdownDemoWorkflow.customer_change,
            CustomerChangeInput(
                order_id="order-2",
                change_type="address_change",
                new_details="Move to service entrance",
                new_lat=36.2222,
                new_lng=-115.2777,
            ),
        )

        await handle.signal(MeltdownDemoWorkflow.change_approved, True)
        await handle.signal(MeltdownDemoWorkflow.change_approved, False)

        result = await handle.result()
        assert "complete" in result.lower()

        order_1 = await fleet.get_order("order-1")
        order_2 = await fleet.get_order("order-2")
        assert order_1.delivery_coords.lat == pytest.approx(36.1111)
        assert order_1.delivery_coords.lng == pytest.approx(-115.1666)
        assert order_2.delivery_coords.lat == pytest.approx(
            DELIVERY_DESTINATIONS["order-2"]["coords"].lat
        )
        assert order_2.delivery_coords.lng == pytest.approx(
            DELIVERY_DESTINATIONS["order-2"]["coords"].lng
        )


async def test_meltdown_demo_handles_disruption_signal_and_reroutes(env: WorkflowEnvironment):
    from agent_fleet.workflows import MeltdownDemoWorkflow

    worker = await _start_worker(env)
    async with worker:
        handle = await env.client.start_workflow(
            MeltdownDemoWorkflow.run,
            MeltdownDemoInput(escalation_enabled=False),
            id="test-meltdown-demo-disruption",
            task_queue=TASK_QUEUE,
        )

        await handle.signal(
            MeltdownDemoWorkflow.disruption_detected,
            DisruptionSignalInput(
                crew_id="ai-crew-1",
                cooler_temp_f=45.0,
                affected_order_ids=["order-1"],
                description="Cooler malfunction detected on ai-crew-1",
            ),
        )

        result = await handle.result()
        assert "complete" in result.lower()

        disrupted_crew = await fleet.get_crew("ai-crew-1")
        rerouted_order = await fleet.get_order("order-1")
        assert disrupted_crew.cooler_status.value == "failed"
        assert rerouted_order.assigned_crew_id == "ai-crew-2"
        assert rerouted_order.status.value == "delivered"
