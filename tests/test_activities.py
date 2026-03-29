"""Unit tests for Temporal activities using ActivityEnvironment."""

import pytest
from temporalio.testing import ActivityEnvironment

from agent_fleet.activities import (
    assign_orders,
    deliver_order,
    find_backup_crew,
    navigate_to,
    pickup_orders,
)
from agent_fleet.models import (
    AssignOrdersInput,
    CoolerStatus,
    CrewStatus,
    DeliverInput,
    FindBackupCrewInput,
    NavigateInput,
    OrderStatus,
    PickupInput,
)
from agent_fleet.simulation import fleet


@pytest.fixture
def env():
    return ActivityEnvironment()


async def test_assign_orders(env: ActivityEnvironment):
    result = await env.run(assign_orders, AssignOrdersInput())
    assert "ai-crew-1" in result.assignments
    assert "ai-crew-2" in result.assignments
    assert "ai-crew-3" in result.assignments

    # Verify fleet state was updated
    c1 = await fleet.get_crew("ai-crew-1")
    assert c1.status == CrewStatus.EN_ROUTE_PICKUP
    assert "order-1" in c1.current_orders


async def test_navigate_to_interpolates_position(env: ActivityEnvironment):
    inp = NavigateInput(
        crew_id="ai-crew-1",
        order_id="order-1",
        target_lat=36.10,
        target_lng=-115.17,
        leg="pickup",
        steps=4,
    )
    result = await env.run(navigate_to, inp)
    assert result.arrived is True
    assert result.final_lat == pytest.approx(36.10)
    assert result.final_lng == pytest.approx(-115.17)

    # Crew should be at target position
    lat, lng = await fleet.get_crew_position("ai-crew-1")
    assert lat == pytest.approx(36.10)
    assert lng == pytest.approx(-115.17)


async def test_pickup_orders_sets_status(env: ActivityEnvironment):
    # First assign orders so they exist in fleet state
    await env.run(assign_orders, AssignOrdersInput())

    result = await env.run(
        pickup_orders,
        PickupInput(crew_id="ai-crew-1", order_ids=["order-1"]),
    )
    assert result.success is True

    c = await fleet.get_crew("ai-crew-1")
    assert c.status == CrewStatus.PICKING_UP

    o = await fleet.get_order("order-1")
    assert o.status == OrderStatus.PICKED_UP


async def test_deliver_order_sets_status(env: ActivityEnvironment):
    await env.run(assign_orders, AssignOrdersInput())
    await env.run(
        pickup_orders,
        PickupInput(crew_id="ai-crew-1", order_ids=["order-1"]),
    )

    result = await env.run(
        deliver_order,
        DeliverInput(crew_id="ai-crew-1", order_id="order-1"),
    )
    assert result.success is True

    o = await fleet.get_order("order-1")
    assert o.status == OrderStatus.DELIVERED

    c = await fleet.get_crew("ai-crew-1")
    assert "order-1" not in c.current_orders
    assert c.status == CrewStatus.IDLE


async def test_find_backup_crew_selects_closest(env: ActivityEnvironment):
    # ai-crew-1 fails, ai-crew-2 and ai-crew-3 are available
    result = await env.run(
        find_backup_crew,
        FindBackupCrewInput(failed_crew_id="ai-crew-1", order_count=1),
    )
    assert result.crew_id is not None
    assert result.crew_id != "ai-crew-1"


async def test_find_backup_crew_excludes_malfunction(env: ActivityEnvironment):
    # Make ai-crew-2 have a cooler malfunction
    await fleet.set_cooler_status("ai-crew-2", CoolerStatus.MALFUNCTION)

    result = await env.run(
        find_backup_crew,
        FindBackupCrewInput(failed_crew_id="ai-crew-1", order_count=1),
    )
    # Should not select ai-crew-2
    assert result.crew_id != "ai-crew-2"
