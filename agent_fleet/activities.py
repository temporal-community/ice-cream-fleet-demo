"""
Temporal activities for the Meltdown ice cream delivery demo.

Each activity is a discrete, retryable unit of work. Activities handle:
- AI-Driver navigation with heartbeats
- Order pickup/delivery
- Fleet status queries (for LLM agents)
- Customer change execution
"""

from __future__ import annotations

import asyncio
import math

import httpx
from temporalio import activity

from agent_fleet.config import GOOGLE_API_KEY, GOOGLE_CSE_ID, GOOGLE_MAPS_API_KEY
from agent_fleet.locations import generate_random_order
from agent_fleet.models import (
    DeliverInput,
    DeliverOutput,
    DriverStatus,
    ExecuteCustomerChangeInput,
    ExecuteCustomerChangeOutput,
    GenerateOrderInput,
    GenerateOrderOutput,
    GetFleetStatusInput,
    GetFleetStatusOutput,
    GetOrderPrioritiesInput,
    GetOrderPrioritiesOutput,
    LegType,
    NavigateInput,
    NavigateOutput,
    OrderStatus,
    PickupInput,
    PickupOutput,
    PublishAgentEventInput,
    PublishAgentEventOutput,
    ReasonAboutAssignmentInput,
    ReasonAboutAssignmentOutput,
)
from agent_fleet.simulation import fleet

# --- Polyline decoding and route fetching ---


def decode_polyline(encoded: str) -> list[tuple[float, float]]:
    """Decode a Google Maps encoded polyline string into (lat, lng) tuples."""
    points = []
    index = 0
    lat = 0
    lng = 0

    while index < len(encoded):
        # Decode latitude
        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat

        # Decode longitude
        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlng = ~(result >> 1) if (result & 1) else (result >> 1)
        lng += dlng

        points.append((lat / 1e5, lng / 1e5))

    return points


@activity.defn
async def get_route_polyline(
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
) -> list[dict[str, float]]:
    """Fetch route waypoints from Google Maps Directions API (decoded polyline).

    Returns a list of {"lat": float, "lng": float} waypoints.
    Failures propagate to Temporal's retry mechanism — no silent fallback.
    In mock mode, the worker registers a mock version of this activity instead.
    """
    origin = f"{origin_lat},{origin_lng}"
    destination = f"{dest_lat},{dest_lng}"
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": origin,
        "destination": destination,
        "key": GOOGLE_MAPS_API_KEY,
        "mode": "driving",
    }

    async with httpx.AsyncClient(timeout=8.0) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

    if data.get("status") != "OK" or not data.get("routes"):
        raise RuntimeError(f"Maps Directions API returned status: {data.get('status')}")

    encoded = data["routes"][0]["overview_polyline"]["points"]
    decoded = decode_polyline(encoded)
    activity.logger.info(f"[NAV] Google Maps polyline: {len(decoded)} points")
    return [{"lat": lat, "lng": lng} for lat, lng in decoded]


# --- Flat-signature tool activities (called by ADK agents via activity_tool) ---


@activity.defn
async def tool_get_fleet_status() -> str:
    """Check current fleet state: AI-Driver positions, cooler conditions, orders."""
    return await fleet.get_fleet_summary()


@activity.defn
async def tool_get_order_priorities() -> str:
    """Check order priority details: VIP vs standard, deadlines, servings."""
    return await fleet.get_order_priorities_summary()


@activity.defn
async def tool_publish_agent_event(
    agent_name: str, event_type: str, content: str, summary: str = ""
) -> str:
    """Publish a reasoning event to the operator UI panel."""
    await fleet.publish_agent_event(agent_name, event_type, content, summary=summary)
    return "Event published."


@activity.defn
async def tool_get_route_info(
    origin_lat: float,
    origin_lng: float,
    destination_lat: float,
    destination_lng: float,
    destination_name: str = "",
) -> str:
    """Get driving route info between two points using Google Maps Directions API.

    Returns distance, duration, and step-by-step directions.
    Use this to assess reroute feasibility and ETAs for AI-Driver dispatching.
    Failures propagate to Temporal's retry mechanism.

    Args:
        origin_lat: Starting latitude
        origin_lng: Starting longitude
        destination_lat: Destination latitude
        destination_lng: Destination longitude
        destination_name: Human-readable name of the destination (e.g. "MGM Grand")
    """
    import re

    origin = f"{origin_lat},{origin_lng}"
    destination = f"{destination_lat},{destination_lng}"
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": origin,
        "destination": destination,
        "key": GOOGLE_MAPS_API_KEY,
        "mode": "driving",
    }

    async with httpx.AsyncClient(timeout=8.0) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

    if data.get("status") != "OK" or not data.get("routes"):
        raise RuntimeError(f"Maps Directions API returned status: {data.get('status')}")

    route = data["routes"][0]
    leg = route["legs"][0]
    distance = leg["distance"]["text"]
    duration = leg["duration"]["text"]
    eta_minutes = max(1, leg["duration"]["value"] // 60)

    steps = []
    for i, step in enumerate(leg["steps"][:5], 1):
        instruction = step["html_instructions"]
        instruction = re.sub(r"<[^>]+>", " ", instruction).strip()
        steps.append(f"  {i}. {instruction} ({step['distance']['text']})")

    dest_label = destination_name or f"({destination_lat:.4f}, {destination_lng:.4f})"
    steps_text = "\n".join(steps)
    return (
        f"Route to {dest_label}:\n"
        f"  Distance: {distance}\n"
        f"  ETA: {duration}\n"
        f"  ETA_MINUTES: {eta_minutes}\n"
        f"  Key directions:\n{steps_text}"
    )


@activity.defn
async def tool_search_hotel_context(hotel_name: str) -> str:
    """Search for live context about a Las Vegas hotel — current events, VIP bookings, reputation.

    Use this to understand delivery urgency for a specific hotel destination.
    Failures propagate to Temporal's retry mechanism.

    Args:
        hotel_name: Name of the hotel (e.g. "MGM Grand", "Caesars Palace", "Mandalay Bay")
    """
    query = f"{hotel_name} Las Vegas current events today"
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": GOOGLE_API_KEY,
        "cx": GOOGLE_CSE_ID,
        "q": query,
        "num": 3,
    }

    async with httpx.AsyncClient(timeout=8.0) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

    items = data.get("items", [])
    if not items:
        return f"No search results found for {hotel_name}."

    results = []
    for item in items[:3]:
        title = item.get("title", "")
        snippet = item.get("snippet", "")
        results.append(f"- {title}: {snippet}")
    return f"Live search results for {hotel_name}:\n" + "\n".join(results)


# --- Core delivery activities ---


@activity.defn
async def generate_order(inp: GenerateOrderInput) -> GenerateOrderOutput:
    """Generate a random order from the venue pool and register it in fleet state."""
    order_data = generate_random_order(inp.order_number)

    await fleet.register_order(
        order_id=order_data["order_id"],
        hotel=order_data["hotel"],
        label=order_data["label"],
        priority=order_data["priority"],
        servings=order_data["servings"],
        delivery_coords=order_data["coords"],
        deadline_minutes=order_data["deadline_minutes"],
    )

    activity.logger.info(f"Generated {order_data['order_id']}: {order_data['label']}")
    return GenerateOrderOutput(
        order_id=order_data["order_id"],
        hotel=order_data["hotel"],
        label=order_data["label"],
        priority=order_data["priority"],
        servings=order_data["servings"],
        delivery_lat=order_data["coords"].lat,
        delivery_lng=order_data["coords"].lng,
        deadline_minutes=order_data["deadline_minutes"],
        event=order_data["event"],
    )


@activity.defn
async def reason_about_assignment(
    inp: ReasonAboutAssignmentInput,
) -> ReasonAboutAssignmentOutput:
    """Run ADK agent pipeline to assign an order to the best driver."""
    from google.adk.runners import Runner
    from google.adk.sessions import InMemorySessionService
    from google.genai.types import Content, Part

    from agent_fleet.agents import create_order_assignment_agent

    agent = create_order_assignment_agent()

    session_service = InMemorySessionService()
    runner = Runner(
        agent=agent,
        app_name="meltdown_demo",
        session_service=session_service,
    )

    session = await session_service.create_session(
        app_name="meltdown_demo",
        user_id="activity",
    )

    prompt = (
        f"NEW ORDER — assign to the best driver:\n"
        f"Order ID: {inp.order_id}\n"
        f"Hotel: {inp.hotel}\n"
        f"Event: {inp.event}\n"
        f"Priority: {inp.priority}\n"
        f"Servings: {inp.servings}\n"
        f"Deadline: {inp.deadline_minutes} minutes\n"
        f"Coordinates: ({inp.delivery_lat}, {inp.delivery_lng})\n\n"
        f"Assess fleet capacity and customer priority, then the resolver "
        f"MUST call tool_submit_assignment with the driver_id and reasoning."
    )

    activity.logger.info(f"Running ADK assignment for {inp.order_id}")

    events_count = 0
    async for event in runner.run_async(
        user_id="activity",
        session_id=session.id,
        new_message=Content(parts=[Part(text=prompt)]),
    ):
        events_count += 1

    updated_session = await session_service.get_session(
        app_name="meltdown_demo",
        user_id="activity",
        session_id=session.id,
    )

    assignment_dict = (updated_session.state or {}).get("assignment")
    if not assignment_dict:
        raise RuntimeError("ADK assignment resolver did not submit an assignment")

    driver_id = assignment_dict["driver_id"]
    reasoning = assignment_dict.get("reasoning_summary", "ADK assignment")

    activity.logger.info(
        f"ADK assignment complete: {events_count} events, {inp.order_id} → {driver_id}"
    )

    # Register in fleet state (UI projection)
    await fleet.assign_order_to_driver(driver_id, inp.order_id)

    return ReasonAboutAssignmentOutput(
        driver_id=driver_id,
        reasoning_summary=reasoning,
    )


@activity.defn
async def register_assignment(driver_id: str, order_id: str) -> str:
    """Register an ADK-decided assignment in fleet state (UI projection)."""
    await fleet.assign_order_to_driver(driver_id, order_id)
    return f"Assigned {order_id} to {driver_id}"


@activity.defn
async def navigate_to(inp: NavigateInput) -> NavigateOutput:
    """
    Simulate AI-Driver navigation by interpolating position over N steps.

    Heartbeats on each step. Disconnect handling is two-layer:
    - inp.is_driver_disconnected: pre-flight check (set by workflow)
    - Cancellation scope: mid-flight disconnect delivers CancelledError
      on the next heartbeat() call (driven by workflow signal handler)
    """
    # Pre-flight disconnect check — workflow passes current state as input
    if inp.is_driver_disconnected:
        raise RuntimeError(
            f"AI-Driver {inp.driver_id} is disconnected — activity will retry on reconnect"
        )

    leg = inp.leg if isinstance(inp.leg, str) else str(inp.leg)
    status = (
        DriverStatus.EN_ROUTE_PICKUP
        if leg == LegType.PICKUP.value
        else DriverStatus.EN_ROUTE_DELIVERY
    )
    await fleet.set_driver_status(inp.driver_id, status)
    await fleet.update_order_status(
        inp.order_id,
        OrderStatus.IN_TRANSIT,
        f"En route to {leg}",
    )

    # Start position from workflow state (not FleetState)
    start_lat = inp.start_lat if inp.start_lat is not None else inp.target_lat
    start_lng = inp.start_lng if inp.start_lng is not None else inp.target_lng

    # Build the path to interpolate along
    if inp.waypoints and len(inp.waypoints) >= 2:
        # Follow waypoint path from Google Maps polyline (or mock corridor)
        path = [(wp["lat"], wp["lng"]) for wp in inp.waypoints]
    else:
        # Straight line fallback
        path = [(start_lat, start_lng), (inp.target_lat, inp.target_lng)]

    # Calculate cumulative distances along the path for proportional interpolation
    segment_dists = []
    for i in range(1, len(path)):
        d = math.sqrt((path[i][0] - path[i - 1][0]) ** 2 + (path[i][1] - path[i - 1][1]) ** 2)
        segment_dists.append(d)
    total_dist = sum(segment_dists) or 1e-9

    for step in range(1, inp.steps + 1):
        # Heartbeat — if workflow cancelled this activity's scope (disconnect signal),
        # this call raises CancelledError, which propagates up to the workflow.
        activity.heartbeat(f"step {step}/{inp.steps}")

        # Find position along the polyline path at this fraction
        fraction = step / inp.steps
        target_dist = fraction * total_dist

        # Walk along segments to find the interpolation point
        accumulated = 0.0
        new_lat, new_lng = path[-1]  # default to end
        for i, seg_d in enumerate(segment_dists):
            if accumulated + seg_d >= target_dist:
                # Interpolate within this segment
                remaining = target_dist - accumulated
                seg_frac = remaining / seg_d if seg_d > 0 else 1.0
                new_lat = path[i][0] + (path[i + 1][0] - path[i][0]) * seg_frac
                new_lng = path[i][1] + (path[i + 1][1] - path[i][1]) * seg_frac
                break
            accumulated += seg_d

        # UI projection write — position update for frontend WebSocket
        await fleet.update_driver_position(inp.driver_id, new_lat, new_lng)

        # Simulate drive time per step
        await asyncio.sleep(0.4)

    activity.logger.info(
        f"{inp.driver_id} arrived at {leg} ({inp.target_lat:.4f}, {inp.target_lng:.4f})"
    )
    return NavigateOutput(
        driver_id=inp.driver_id,
        arrived=True,
        final_lat=inp.target_lat,
        final_lng=inp.target_lng,
    )


@activity.defn
async def pickup_orders(inp: PickupInput) -> PickupOutput:
    """Simulate picking up ice cream orders at the kitchen."""
    if inp.is_driver_disconnected:
        raise RuntimeError(f"AI-Driver {inp.driver_id} is disconnected")
    await fleet.set_driver_status(inp.driver_id, DriverStatus.PICKING_UP)
    for oid in inp.order_ids:
        await fleet.update_order_status(oid, OrderStatus.PICKED_UP, "Picked up")

    await asyncio.sleep(1.5)

    activity.logger.info(f"{inp.driver_id} picked up orders {inp.order_ids}")
    return PickupOutput(driver_id=inp.driver_id, success=True)


@activity.defn
async def deliver_order(inp: DeliverInput) -> DeliverOutput:
    """Simulate delivering an ice cream order at a hotel."""
    if inp.is_driver_disconnected:
        raise RuntimeError(f"AI-Driver {inp.driver_id} is disconnected")
    await fleet.set_driver_status(inp.driver_id, DriverStatus.DELIVERING)
    await fleet.update_order_status(inp.order_id, OrderStatus.IN_TRANSIT, "Delivering")

    await asyncio.sleep(1.5)

    # UI projection — mark order delivered and update driver status
    remaining_count = await fleet.complete_order_delivery(inp.driver_id, inp.order_id)
    if remaining_count == 0:
        await fleet.set_driver_status(inp.driver_id, DriverStatus.IDLE)

    activity.logger.info(f"{inp.driver_id} delivered {inp.order_id}")
    return DeliverOutput(driver_id=inp.driver_id, order_id=inp.order_id, success=True)


# --- Agent tool activities (called by ADK agents via activity_tool) ---


@activity.defn
async def get_fleet_status(inp: GetFleetStatusInput) -> GetFleetStatusOutput:
    """Return fleet status summary for Fleet Agent consumption."""
    summary = await fleet.get_fleet_summary()
    return GetFleetStatusOutput(summary=summary)


@activity.defn
async def get_order_priorities(
    inp: GetOrderPrioritiesInput,
) -> GetOrderPrioritiesOutput:
    """Return order priority details for Customer Agent consumption."""
    summary = await fleet.get_order_priorities_summary()
    return GetOrderPrioritiesOutput(summary=summary)


@activity.defn
async def publish_agent_event(
    inp: PublishAgentEventInput,
) -> PublishAgentEventOutput:
    """Publish an agent reasoning event to the UI panel."""
    await fleet.publish_agent_event(
        inp.agent_name, inp.event_type, inp.content, summary=inp.summary
    )
    return PublishAgentEventOutput(success=True)


# --- Customer change activities ---


@activity.defn
async def execute_customer_change(
    inp: ExecuteCustomerChangeInput,
) -> ExecuteCustomerChangeOutput:
    """Execute a customer-initiated change (address update or cancellation)."""
    if inp.change_type == "cancel":
        await fleet.cancel_order(inp.order_id)
        activity.logger.info(f"Order {inp.order_id} cancelled")
    elif (
        inp.change_type == "address_change" and inp.new_lat is not None and inp.new_lng is not None
    ):
        await fleet.update_order_delivery(inp.order_id, inp.new_lat, inp.new_lng)
        activity.logger.info(
            f"Order {inp.order_id} delivery updated to ({inp.new_lat:.4f}, {inp.new_lng:.4f})"
        )

    return ExecuteCustomerChangeOutput(success=True)
