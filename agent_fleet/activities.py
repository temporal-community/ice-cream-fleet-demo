"""
Temporal activities for the Meltdown ice cream delivery demo.

Each activity is a discrete, retryable unit of work. Activities handle:
- AI-Crew navigation with heartbeats (crash recovery demo)
- Order pickup/delivery
- Fleet status queries (for LLM agents)
- Disruption detection and recovery
- Customer change execution
"""

from __future__ import annotations

import asyncio
import math
import os

import httpx
from temporalio import activity

from agent_fleet.locations import CREW_ASSIGNMENTS, DELIVERY_DESTINATIONS
from agent_fleet.models import (
    AssignOrdersInput,
    AssignOrdersOutput,
    CheckDisruptionInput,
    CheckDisruptionOutput,
    CoolerStatus,
    CoordinateDeliveryInput,
    CoordinateDeliveryOutput,
    CrewStatus,
    DeliverInput,
    DeliverOutput,
    ExecuteCustomerChangeInput,
    ExecuteCustomerChangeOutput,
    ExecuteRecoveryInput,
    ExecuteRecoveryOutput,
    FindBackupCrewInput,
    FindBackupCrewOutput,
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
    RecoveryPlan,
    RunDisruptionResolverInput,
    RunDisruptionResolverOutput,
)
from agent_fleet.simulation import fleet

# --- Polyline decoding and route fetching ---

# Known locations for mock waypoint generation (Las Vegas Strip)
_STRIP_POINTS = {
    "warehouse": (36.1280, -115.1520),
    "caesars": (36.1162, -115.1745),
    "mgm": (36.1024, -115.1696),
    "mandalay": (36.0919, -115.1761),
}

# Intermediate points along Las Vegas Blvd from warehouse heading south
# Las Vegas Blvd S — anchor points verified against map tiles + interpolated
# The strip runs SSE from Venetian, bends at Flamingo, then curves SW to Mandalay
# Coordinates placed ON the road centerline as shown on CartoDB/Stadia tiles
_STRIP_CORRIDOR = [
    # Venetian / Palazzo — LV Blvd here is at ~-115.1710
    (36.12200, -115.17100),
    (36.12150, -115.17105),
    (36.12100, -115.17110),
    (36.12050, -115.17120),
    (36.12000, -115.17130),
    # LINQ / Harrah's
    (36.11950, -115.17150),
    (36.11900, -115.17170),
    (36.11850, -115.17200),
    (36.11800, -115.17220),
    # Flamingo intersection — road at ~-115.1726
    (36.11750, -115.17240),
    (36.11700, -115.17260),
    # Caesars Palace (marker: 36.1162, -115.1745)
    (36.11670, -115.17300),
    (36.11650, -115.17350),
    (36.11620, -115.17450),  # Caesars marker
    (36.11580, -115.17460),
    (36.11540, -115.17470),
    # Bellagio — road at ~-115.1742
    (36.11500, -115.17420),
    (36.11450, -115.17410),
    (36.11400, -115.17400),
    (36.11350, -115.17390),
    (36.11300, -115.17380),
    (36.11250, -115.17370),
    (36.11200, -115.17360),
    # Cosmopolitan — Milk Bar (marker: 36.1094, -115.1735)
    (36.11150, -115.17355),
    (36.11100, -115.17350),
    (36.11050, -115.17350),
    (36.11000, -115.17350),
    (36.10940, -115.17350),  # Milk Bar
    # CityCenter / Aria — road bends slightly east
    (36.10880, -115.17340),
    (36.10830, -115.17330),
    (36.10780, -115.17310),
    (36.10730, -115.17300),
    (36.10680, -115.17290),
    (36.10630, -115.17280),
    # Park MGM
    (36.10580, -115.17270),
    (36.10530, -115.17265),
    (36.10480, -115.17260),
    (36.10430, -115.17255),
    # Tropicana / MGM Grand (marker: 36.1024, -115.1725)
    (36.10380, -115.17250),
    (36.10330, -115.17250),
    (36.10280, -115.17250),
    (36.10240, -115.17250),  # MGM Grand marker
    (36.10200, -115.17250),
    (36.10150, -115.17260),
    (36.10100, -115.17270),
    (36.10050, -115.17290),
    # South of MGM — road curves southwest
    (36.10000, -115.17310),
    (36.09950, -115.17330),
    (36.09900, -115.17360),
    (36.09850, -115.17390),
    (36.09800, -115.17420),
    # Excalibur / Luxor
    (36.09750, -115.17450),
    (36.09700, -115.17480),
    (36.09650, -115.17500),
    (36.09600, -115.17520),
    (36.09550, -115.17540),
    (36.09500, -115.17560),
    # Mandalay Bay approach (marker: 36.0919, -115.1761)
    (36.09450, -115.17570),
    (36.09400, -115.17580),
    (36.09350, -115.17590),
    (36.09300, -115.17600),
    (36.09250, -115.17610),
    (36.09190, -115.17610),  # Mandalay Bay marker
]


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


def _mock_route_waypoints(
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
) -> list[dict[str, float]]:
    """Generate mock waypoints that follow the Las Vegas Strip corridor.

    Finds the closest corridor points to origin and destination, then returns
    the slice of the corridor between them (plus origin/dest endpoints).
    """

    def _closest_corridor_idx(lat: float, lng: float) -> int:
        best_idx = 0
        best_dist = float("inf")
        for i, (clat, clng) in enumerate(_STRIP_CORRIDOR):
            d = math.sqrt((lat - clat) ** 2 + (lng - clng) ** 2)
            if d < best_dist:
                best_dist = d
                best_idx = i
        return best_idx

    start_idx = _closest_corridor_idx(origin_lat, origin_lng)
    end_idx = _closest_corridor_idx(dest_lat, dest_lng)

    # Build waypoints: origin -> corridor slice -> destination
    waypoints = [{"lat": origin_lat, "lng": origin_lng}]

    if start_idx <= end_idx:
        corridor_slice = _STRIP_CORRIDOR[start_idx : end_idx + 1]
    else:
        corridor_slice = list(reversed(_STRIP_CORRIDOR[end_idx : start_idx + 1]))

    for clat, clng in corridor_slice:
        # Skip if too close to origin (already added)
        if len(waypoints) == 1:
            d = math.sqrt((clat - origin_lat) ** 2 + (clng - origin_lng) ** 2)
            if d < 0.0005:
                continue
        waypoints.append({"lat": clat, "lng": clng})

    # Add final destination if not already close to last waypoint
    last = waypoints[-1]
    d = math.sqrt((dest_lat - last["lat"]) ** 2 + (dest_lng - last["lng"]) ** 2)
    if d > 0.0005:
        waypoints.append({"lat": dest_lat, "lng": dest_lng})

    return waypoints


@activity.defn(name="get_route_polyline")
async def get_route_polyline(
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
) -> list[dict[str, float]]:
    """Fetch route waypoints from Google Maps Directions API (decoded polyline).

    Returns a list of {"lat": float, "lng": float} waypoints.
    Falls back to mock corridor waypoints if no API key is set.
    """
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY") or os.environ.get("GOOGLE_API_KEY")

    if not api_key:
        return _mock_route_waypoints(origin_lat, origin_lng, dest_lat, dest_lng)

    origin = f"{origin_lat},{origin_lng}"
    destination = f"{dest_lat},{dest_lng}"
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": origin,
        "destination": destination,
        "key": api_key,
        "mode": "driving",
    }

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        if data.get("status") != "OK" or not data.get("routes"):
            activity.logger.warning(
                f"Maps API status for polyline: {data.get('status')}, using mock"
            )
            return _mock_route_waypoints(origin_lat, origin_lng, dest_lat, dest_lng)

        # Decode the overview polyline
        encoded = data["routes"][0]["overview_polyline"]["points"]
        decoded = decode_polyline(encoded)
        return [{"lat": lat, "lng": lng} for lat, lng in decoded]

    except Exception as e:
        activity.logger.warning(f"Maps polyline API error, using mock: {e}")
        return _mock_route_waypoints(origin_lat, origin_lng, dest_lat, dest_lng)


# --- Flat-signature tool activities (called by ADK agents via activity_tool) ---


@activity.defn(name="tool_get_fleet_status")
async def tool_get_fleet_status() -> str:
    """Check current fleet state: AI-Crew positions, cooler conditions, orders."""
    return await fleet.get_fleet_summary()


@activity.defn(name="tool_get_order_priorities")
async def tool_get_order_priorities() -> str:
    """Check order priority details: VIP vs standard, deadlines, servings."""
    return await fleet.get_order_priorities_summary()


@activity.defn(name="tool_publish_agent_event")
async def tool_publish_agent_event(
    agent_name: str, event_type: str, content: str, summary: str = ""
) -> str:
    """Publish a reasoning event to the operator UI panel."""
    await fleet.publish_agent_event(agent_name, event_type, content, summary=summary)
    return "Event published."


@activity.defn(name="tool_get_route_info")
async def tool_get_route_info(
    origin_lat: float,
    origin_lng: float,
    destination_lat: float,
    destination_lng: float,
    destination_name: str = "",
) -> str:
    """Get driving route info between two points using Google Maps Directions API.

    Returns distance, duration, and step-by-step directions.
    Use this to assess reroute feasibility and ETAs for AI-Crew dispatching.

    Args:
        origin_lat: Starting latitude
        origin_lng: Starting longitude
        destination_lat: Destination latitude
        destination_lng: Destination longitude
        destination_name: Human-readable name of the destination (e.g. "MGM Grand")
    """
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY") or os.environ.get("GOOGLE_API_KEY")

    if not api_key:
        # Mock fallback — deterministic response for demo
        return _mock_route_info(
            origin_lat, origin_lng, destination_lat, destination_lng, destination_name
        )

    origin = f"{origin_lat},{origin_lng}"
    destination = f"{destination_lat},{destination_lng}"
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": origin,
        "destination": destination,
        "key": api_key,
        "mode": "driving",
    }

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        if data.get("status") != "OK" or not data.get("routes"):
            activity.logger.warning(f"Maps API status: {data.get('status')}")
            return _mock_route_info(
                origin_lat, origin_lng, destination_lat, destination_lng, destination_name
            )

        route = data["routes"][0]
        leg = route["legs"][0]
        distance = leg["distance"]["text"]
        duration = leg["duration"]["text"]
        # Duration in minutes for structured agent reasoning
        eta_minutes = max(1, leg["duration"]["value"] // 60)

        steps = []
        for i, step in enumerate(leg["steps"][:5], 1):
            instruction = step["html_instructions"]
            # Strip HTML tags for clean text
            import re

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

    except Exception as e:
        activity.logger.warning(f"Maps API error, using mock: {e}")
        return _mock_route_info(
            origin_lat, origin_lng, destination_lat, destination_lng, destination_name
        )


def _mock_route_info(
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
    dest_name: str,
) -> str:
    """Deterministic mock route info for demo without Maps API key."""
    dlat = dest_lat - origin_lat
    dlng = dest_lng - origin_lng
    # Rough distance in miles (Las Vegas scale)
    dist_miles = math.sqrt(dlat**2 + dlng**2) * 69.0
    eta_minutes = max(3, int(dist_miles * 3.5))

    dest_label = dest_name or f"({dest_lat:.4f}, {dest_lng:.4f})"
    return (
        f"Route to {dest_label}:\n"
        f"  Distance: {dist_miles:.1f} mi\n"
        f"  ETA: {eta_minutes} mins\n"
        f"  ETA_MINUTES: {eta_minutes}\n"
        f"  Key directions:\n"
        f"    1. Head south on Las Vegas Blvd (0.5 mi)\n"
        f"    2. Continue on Las Vegas Blvd S ({max(0.1, dist_miles - 0.5):.1f} mi)\n"
        f"    3. Arrive at {dest_label}"
    )


@activity.defn(name="tool_search_hotel_context")
async def tool_search_hotel_context(hotel_name: str) -> str:
    """Search for live context about a Las Vegas hotel — current events, VIP bookings, reputation.

    Use this to understand delivery urgency for a specific hotel destination.

    Args:
        hotel_name: Name of the hotel (e.g. "MGM Grand", "Caesars Palace", "Mandalay Bay")
    """
    return await _search_hotel_context(hotel_name)


async def _search_hotel_context(hotel_name: str) -> str:
    """Search for hotel context — tries Google Search API, falls back to mock.

    This is the shared implementation used by both the tool_search_hotel_context
    activity and the mock resolver. Separated so it can be called from within
    another activity (activities can't call other activities via Temporal).
    """
    api_key = os.environ.get("GOOGLE_API_KEY")
    search_engine_id = os.environ.get("GOOGLE_CSE_ID")

    if api_key and search_engine_id:
        try:
            query = f"{hotel_name} Las Vegas current events today"
            url = "https://www.googleapis.com/customsearch/v1"
            params = {
                "key": api_key,
                "cx": search_engine_id,
                "q": query,
                "num": 3,
            }
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()

            items = data.get("items", [])
            if items:
                results = []
                for item in items[:3]:
                    title = item.get("title", "")
                    snippet = item.get("snippet", "")
                    results.append(f"- {title}: {snippet}")
                return f"Live search results for {hotel_name}:\n" + "\n".join(results)
        except Exception:
            pass

    return _mock_hotel_context(hotel_name)


def _mock_hotel_context(hotel_name: str) -> str:
    """Deterministic mock hotel context for demo without Search API."""
    contexts = {
        "MGM Grand": (
            f"- {hotel_name}: Currently hosting Wet Republic pool party series. "
            f"High guest volume with VIP catering expectations.\n"
            f"- {hotel_name}: Grand Garden Arena has a major event tonight — "
            f"hotel is at peak occupancy with elevated service standards.\n"
            f"- {hotel_name}: Known for premium poolside dining — late catering "
            f"deliveries have resulted in vendor penalties in the past."
        ),
        "Caesars Palace": (
            f"- {hotel_name}: Banquet halls booked for a corporate gala tonight. "
            f"Caesars is known for premium event standards.\n"
            f"- {hotel_name}: The Forum Shops are running a VIP shopping event — "
            f"hotel staff are stretched thin, on-time delivery is critical.\n"
            f"- {hotel_name}: Colosseum show tonight means 4,000+ guests on property. "
            f"Late delivery would damage vendor relationship."
        ),
        "Mandalay Bay": (
            f"- {hotel_name}: Tech conference in session at the Convention Center. "
            f"Conference catering is time-sensitive — dessert course is scheduled.\n"
            f"- {hotel_name}: Shark Reef and pool areas at capacity — resort is in "
            f"peak weekend mode with premium service expectations.\n"
            f"- {hotel_name}: Convention Center hosts 10,000+ attendees — "
            f"catering delays would be visible to a large audience."
        ),
    }
    # Fuzzy match hotel name
    for key, context in contexts.items():
        if key.lower() in hotel_name.lower() or hotel_name.lower() in key.lower():
            return f"Hotel intelligence for {hotel_name}:\n{context}"
    return f"No specific intelligence available for {hotel_name}."


# --- Core delivery activities ---


@activity.defn(name="assign_orders")
async def assign_orders(inp: AssignOrdersInput) -> AssignOrdersOutput:
    """Assign orders to AI-Crews using deterministic mapping."""
    for crew_id, order_ids in CREW_ASSIGNMENTS.items():
        await fleet.assign_orders_to_crew(crew_id, order_ids)
        activity.logger.info(f"Assigned {order_ids} to {crew_id}")
    return AssignOrdersOutput(assignments=dict(CREW_ASSIGNMENTS))


@activity.defn(name="coordinate_delivery_plan")
async def coordinate_delivery_plan(inp: CoordinateDeliveryInput) -> CoordinateDeliveryOutput:
    """
    Multi-agent coordination for initial delivery planning.

    Fleet Agent assesses routes and capacity, Customer Agent evaluates
    priorities and deadlines, Resolver synthesizes the final dispatch plan.
    """
    await fleet.get_fleet_summary()
    await fleet.get_order_priorities_summary()

    # --- Fleet Agent: route and capacity analysis ---
    await fleet.publish_agent_event(
        "fleet_agent",
        "tool_call",
        "Calling tool_get_fleet_status to assess AI-Crew positions, "
        "cooler conditions, and route capacity...",
        summary="Checking fleet status...",
    )
    await asyncio.sleep(0.5)

    route_lines = []
    for crew_id, order_ids in inp.assignments.items():
        for oid in order_ids:
            dest = DELIVERY_DESTINATIONS.get(oid, {})
            hotel = dest.get("hotel", oid)
            route_lines.append(f"  {crew_id} -> {hotel}")
    routes_text = "\n".join(route_lines)

    await fleet.publish_agent_event(
        "fleet_agent",
        "assessment",
        f"FLEET ASSESSMENT — 3 AI-Crews ready at Ice Cream Kitchen, "
        f"all coolers nominal at 0F.\n\n"
        f"Proposed routes:\n{routes_text}\n\n"
        f"All AI-Crews have capacity for their assigned orders. "
        f"Routes are non-overlapping — each AI-Crew takes a direct path "
        f"from the kitchen to their hotel. No conflicts detected.\n\n"
        f"RECOMMENDATION: Clear to dispatch. Monitoring cooler temps "
        f"and ETAs throughout delivery.",
        summary="All AI-Crews ready, routes clear — recommending dispatch",
    )
    await asyncio.sleep(0.4)

    # --- Customer Agent: priority and deadline analysis ---
    await fleet.publish_agent_event(
        "customer_agent",
        "tool_call",
        "Calling tool_get_order_priorities to review VIP status, "
        "servings, and delivery deadlines...",
        summary="Checking order priorities...",
    )
    await asyncio.sleep(0.5)

    order_lines = []
    total_servings = 0
    for oid in DELIVERY_DESTINATIONS:
        dest = DELIVERY_DESTINATIONS[oid]
        total_servings += dest["servings"]
        urgency = "TIGHT" if dest["deadline_minutes"] <= 30 else "comfortable"
        order_lines.append(
            f"  {oid} -> {dest['hotel']}: {dest['priority'].upper()}, "
            f"{dest['servings']} servings, {dest['deadline_minutes']}min deadline [{urgency}]"
        )
    orders_text = "\n".join(order_lines)

    await fleet.publish_agent_event(
        "customer_agent",
        "assessment",
        f"CUSTOMER IMPACT — {total_servings} total servings across "
        f"{len(DELIVERY_DESTINATIONS)} VIP orders:\n\n"
        f"{orders_text}\n\n"
        f"All orders are VIP-tier with 30-35 minute deadlines. "
        f"These are high-profile hotel events — pool parties, banquets, "
        f"and conferences. On-time delivery is critical.\n\n"
        f"RECOMMENDATION: Dispatch immediately. Caesars and Mandalay Bay "
        f"have the tightest deadlines (30min) — prioritize if any delays occur.",
        summary="All VIP orders with tight deadlines — dispatch immediately",
    )
    await asyncio.sleep(0.4)

    # --- Resolver: synthesize dispatch decision ---
    await fleet.publish_agent_event(
        "resolver",
        "synthesis",
        f"Fleet Agent confirms all AI-Crews ready with nominal coolers. "
        f"Customer Agent confirms all {len(DELIVERY_DESTINATIONS)} orders are VIP "
        f"with tight deadlines — {total_servings} servings at stake.\n\n"
        f"Both agents agree on immediate dispatch. No conflicts to resolve.",
        summary="Both agents agree — dispatching immediately",
    )
    await asyncio.sleep(0.3)

    await fleet.publish_agent_event(
        "resolver",
        "plan",
        f"DISPATCH PLAN CONFIRMED:\n"
        f"{routes_text}\n\n"
        f"All AI-Crews dispatching simultaneously from Ice Cream Kitchen. "
        f"Fleet Agent monitoring cooler temps and ETAs. "
        f"Customer Agent tracking deadline compliance.\n\n"
        f"Agents standing by for disruption response if needed.",
        summary="Dispatch plan confirmed — all AI-Crews rolling out",
    )

    activity.logger.info("Multi-agent delivery coordination complete")
    return CoordinateDeliveryOutput(success=True)


@activity.defn(name="navigate_to")
async def navigate_to(inp: NavigateInput) -> NavigateOutput:
    """
    Simulate AI-Crew navigation by interpolating position over N steps.

    Heartbeats on each step. If the worker is killed mid-navigation,
    the heartbeat timeout fires, Temporal marks it failed, and retries
    on the next worker — resuming the mission.
    """
    if not await fleet.crew_exists(inp.crew_id):
        raise ValueError(f"Unknown AI-Crew: {inp.crew_id}")

    if fleet.is_recovering():
        activity.logger.info(f"[REPLAY] Resuming navigation for {inp.crew_id}")

    # Per-crew disconnect: if this crew is disconnected, fail the activity.
    # Temporal will keep retrying until the crew is reconnected.
    if await fleet.is_crew_disconnected(inp.crew_id):
        raise RuntimeError(
            f"AI-Crew {inp.crew_id} is disconnected — activity will retry on reconnect"
        )

    leg = inp.leg if isinstance(inp.leg, str) else str(inp.leg)
    status = (
        CrewStatus.EN_ROUTE_PICKUP
        if leg == LegType.PICKUP.value
        else CrewStatus.EN_ROUTE_DELIVERY
    )
    await fleet.set_crew_status(inp.crew_id, status)
    await fleet.update_order_status(
        inp.order_id,
        OrderStatus.IN_TRANSIT,
        f"AI-Crew {inp.crew_id} navigating to {leg} point",
    )

    start_lat, start_lng = await fleet.get_crew_position(inp.crew_id)

    # Build the path to interpolate along
    if inp.waypoints and len(inp.waypoints) >= 2:
        # Follow waypoint path from Google Maps polyline (or mock corridor)
        path = [(wp["lat"], wp["lng"]) for wp in inp.waypoints]
    else:
        # Straight line fallback (backwards compat)
        path = [(start_lat, start_lng), (inp.target_lat, inp.target_lng)]

    # Calculate cumulative distances along the path for proportional interpolation
    segment_dists = []
    for i in range(1, len(path)):
        d = math.sqrt(
            (path[i][0] - path[i - 1][0]) ** 2
            + (path[i][1] - path[i - 1][1]) ** 2
        )
        segment_dists.append(d)
    total_dist = sum(segment_dists) or 1e-9

    for step in range(1, inp.steps + 1):
        activity.heartbeat(f"step {step}/{inp.steps}")

        # Check disconnect mid-navigation too
        if await fleet.is_crew_disconnected(inp.crew_id):
            activity.logger.warning(f"{inp.crew_id} disconnected at step {step}/{inp.steps}")
            raise RuntimeError(
                f"AI-Crew {inp.crew_id} disconnected mid-navigation at step {step}"
            )

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

        await fleet.update_crew_position(inp.crew_id, new_lat, new_lng)

        # Track nav steps for demo event triggers (cooler malfunction)
        await fleet.increment_nav_step(inp.crew_id)

        # Simulate drive time per step
        await asyncio.sleep(0.4)

    activity.logger.info(
        f"{inp.crew_id} arrived at {leg} ({inp.target_lat:.4f}, {inp.target_lng:.4f})"
    )
    return NavigateOutput(
        crew_id=inp.crew_id,
        arrived=True,
        final_lat=inp.target_lat,
        final_lng=inp.target_lng,
    )


@activity.defn(name="pickup_orders")
async def pickup_orders(inp: PickupInput) -> PickupOutput:
    """Simulate picking up ice cream orders at the kitchen."""
    if await fleet.is_crew_disconnected(inp.crew_id):
        raise RuntimeError(f"AI-Crew {inp.crew_id} is disconnected")
    await fleet.set_crew_status(inp.crew_id, CrewStatus.PICKING_UP)
    for oid in inp.order_ids:
        await fleet.update_order_status(oid, OrderStatus.PICKED_UP, "Ice cream loaded into cooler")

    await asyncio.sleep(1.5)

    activity.logger.info(f"{inp.crew_id} picked up orders {inp.order_ids}")
    return PickupOutput(crew_id=inp.crew_id, success=True)


@activity.defn(name="deliver_order")
async def deliver_order(inp: DeliverInput) -> DeliverOutput:
    """Simulate delivering an ice cream order at a hotel."""
    if await fleet.is_crew_disconnected(inp.crew_id):
        raise RuntimeError(f"AI-Crew {inp.crew_id} is disconnected")
    await fleet.set_crew_status(inp.crew_id, CrewStatus.DELIVERING)
    await fleet.update_order_status(inp.order_id, OrderStatus.IN_TRANSIT, "Delivering to hotel")

    await asyncio.sleep(1.5)

    remaining_count = await fleet.complete_order_delivery(inp.crew_id, inp.order_id)
    if remaining_count == 0:
        await fleet.set_crew_status(inp.crew_id, CrewStatus.IDLE)

    activity.logger.info(f"{inp.crew_id} delivered {inp.order_id}")
    return DeliverOutput(crew_id=inp.crew_id, order_id=inp.order_id, success=True)


# --- Agent tool activities (called by ADK agents via activity_tool) ---


@activity.defn(name="get_fleet_status")
async def get_fleet_status(inp: GetFleetStatusInput) -> GetFleetStatusOutput:
    """Return fleet status summary for Fleet Agent consumption."""
    summary = await fleet.get_fleet_summary()
    return GetFleetStatusOutput(summary=summary)


@activity.defn(name="get_order_priorities")
async def get_order_priorities(
    inp: GetOrderPrioritiesInput,
) -> GetOrderPrioritiesOutput:
    """Return order priority details for Customer Agent consumption."""
    summary = await fleet.get_order_priorities_summary()
    return GetOrderPrioritiesOutput(summary=summary)


@activity.defn(name="publish_agent_event")
async def publish_agent_event(
    inp: PublishAgentEventInput,
) -> PublishAgentEventOutput:
    """Publish an agent reasoning event to the UI panel."""
    await fleet.publish_agent_event(
        inp.agent_name, inp.event_type, inp.content, summary=inp.summary
    )
    return PublishAgentEventOutput(success=True)


# --- Disruption activities ---


@activity.defn(name="check_for_disruption")
async def check_for_disruption(
    inp: CheckDisruptionInput,
) -> CheckDisruptionOutput:
    """Check if any AI-Crew has a cooler malfunction."""
    result = await fleet.check_disruption()
    return CheckDisruptionOutput(
        disruption_detected=result["disruption_detected"],
        crew_id=result.get("crew_id"),
        cooler_temp_f=result.get("cooler_temp_f", 0.0),
        affected_order_ids=result.get("affected_order_ids", []),
        description=result.get("description", ""),
    )


@activity.defn(name="find_backup_crew")
async def find_backup_crew(
    inp: FindBackupCrewInput,
) -> FindBackupCrewOutput:
    """Find the best available AI-Crew to absorb rerouted orders."""
    crew_id, reason = await fleet.find_backup_crew(inp.failed_crew_id, inp.order_count)
    activity.logger.info(f"Backup AI-Crew selection: {reason}")
    return FindBackupCrewOutput(crew_id=crew_id, reason=reason)


@activity.defn(name="execute_recovery")
async def execute_recovery(inp: ExecuteRecoveryInput) -> ExecuteRecoveryOutput:
    """Execute disruption recovery: reroute orders and return failed AI-Crew."""
    # Reroute affected orders to backup AI-Crew
    await fleet.reroute_orders(
        inp.return_crew_id, inp.reroute_to_crew_id, inp.reroute_order_ids
    )

    # Mark the failed AI-Crew as returning
    await fleet.set_crew_status(inp.return_crew_id, CrewStatus.RETURNING)
    await fleet.set_cooler_status(inp.return_crew_id, CoolerStatus.FAILED)

    # Publish notifications as agent events
    for notification in inp.notifications:
        await fleet.publish_agent_event(
            "resolver", "notification", notification, summary="Recovery notification sent"
        )

    activity.logger.info(
        f"Recovery executed: {inp.reroute_order_ids} rerouted to "
        f"{inp.reroute_to_crew_id}, {inp.return_crew_id} returning"
    )
    return ExecuteRecoveryOutput(success=True)


# --- Customer change activities ---


@activity.defn(name="execute_customer_change")
async def execute_customer_change(
    inp: ExecuteCustomerChangeInput,
) -> ExecuteCustomerChangeOutput:
    """Execute a customer-initiated change (address update or cancellation)."""
    if inp.change_type == "cancel":
        await fleet.cancel_order(inp.order_id)
        activity.logger.info(f"Order {inp.order_id} cancelled")
    elif (
        inp.change_type == "address_change"
        and inp.new_lat is not None
        and inp.new_lng is not None
    ):
        await fleet.update_order_delivery(inp.order_id, inp.new_lat, inp.new_lng)
        activity.logger.info(
            f"Order {inp.order_id} delivery updated to ({inp.new_lat:.4f}, {inp.new_lng:.4f})"
        )

    return ExecuteCustomerChangeOutput(success=True)


@activity.defn(name="resolve_disruption_mock")
async def resolve_disruption_mock(
    inp: RunDisruptionResolverInput,
) -> RunDisruptionResolverOutput:
    """
    Deterministic mock resolver — same interface as the ADK resolver.

    Finds the best backup AI-Crew, publishes canned agent reasoning events,
    and returns a structured RecoveryPlan. Used in mock mode or as a fallback
    when the ADK resolver fails.
    """
    # Find the best available backup AI-Crew
    crew_id, reason = await fleet.find_backup_crew(
        inp.disruption_crew_id, len(inp.affected_order_ids)
    )

    events_published = 0

    if crew_id is None:
        # No backup available — publish detailed emergency events
        await fleet.publish_agent_event(
            "fleet_agent",
            "assessment",
            f"COOLER FAILURE on {inp.disruption_crew_id} — "
            f"temperature at {inp.cooler_temp_f}F and rising.\n\n"
            f"Scanned all AI-Crews for reroute candidates: {reason}.\n"
            f"No viable backup available — all routes are at capacity or "
            f"have their own cooler issues.",
            summary=f"Cooler failure on {inp.disruption_crew_id} — no backup available",
        )
        events_published += 1

        await fleet.publish_agent_event(
            "customer_agent",
            "assessment",
            f"{len(inp.affected_order_ids)} VIP orders at risk with no reroute "
            f"option. Hotels must be notified immediately of delay. "
            f"Orders will need to be repacked at the kitchen.",
            summary=f"{len(inp.affected_order_ids)} VIP orders at risk, no reroute option",
        )
        events_published += 1

        await fleet.publish_agent_event(
            "resolver",
            "plan",
            f"EMERGENCY PLAN (no backup AI-Crew available):\n"
            f"1. {inp.disruption_crew_id} returns to kitchen immediately\n"
            f"2. Orders {inp.affected_order_ids} returned to base for repack\n"
            f"3. All affected hotel coordinators notified of delay",
            summary="Emergency return-to-kitchen plan activated",
        )
        events_published += 1

        activity.logger.info(
            f"Mock resolver: no backup available for {inp.disruption_crew_id}"
        )
        return RunDisruptionResolverOutput(
            recovery_plan=None,
            agent_events_published=events_published,
            error=f"No backup AI-Crew available: {reason}",
        )

    # --- Gather state for agent reasoning ---
    await fleet.get_fleet_summary()
    backup_crew = await fleet.get_crew(crew_id)
    failed_crew = await fleet.get_crew(inp.disruption_crew_id)

    backup_orders = backup_crew.current_orders if backup_crew else []
    backup_capacity = (backup_crew.capacity - len(backup_orders)) if backup_crew else 0
    failed_temp = failed_crew.cooler_temp_f if failed_crew else inp.cooler_temp_f

    # Check per-agent health — skip offline agents
    fleet_online = await fleet.is_agent_online("fleet_agent")
    customer_online = await fleet.is_agent_online("customer_agent")
    resolver_online = await fleet.is_agent_online("resolver")

    fleet_assessment = ""
    customer_assessment = ""

    # --- Fleet Agent: operational assessment ---
    if fleet_online:
        await fleet.publish_agent_event(
            "fleet_agent",
            "tool_call",
            "Calling tool_get_fleet_status to check AI-Crew positions, "
            "cooler conditions, and available capacity...",
            summary="Checking fleet status...",
        )
        events_published += 1
        await asyncio.sleep(0.4)

        # Check Maps route ETAs from backup crew to each affected delivery location
        backup_lat = backup_crew.position.lat if backup_crew else 36.1280
        backup_lng = backup_crew.position.lng if backup_crew else -115.1520

        eta_lines = []
        for oid in inp.affected_order_ids:
            order = await fleet.get_order(oid)
            if order:
                route_info_text = _mock_route_info(
                    backup_lat,
                    backup_lng,
                    order.delivery_coords.lat,
                    order.delivery_coords.lng,
                    order.hotel,
                )
                # Extract ETA_MINUTES from the structured field
                eta_min = "?"
                for line in route_info_text.split("\n"):
                    if "ETA_MINUTES:" in line:
                        eta_min = line.split("ETA_MINUTES:")[1].strip()
                        break
                eta_lines.append(f"  - {crew_id} -> {order.hotel} ({oid}): ~{eta_min} min")

        # Also get a summary route from backup to the failed crew's area
        route_info = _mock_route_info(
            backup_lat,
            backup_lng,
            failed_crew.position.lat if failed_crew else 36.1024,
            failed_crew.position.lng if failed_crew else -115.1696,
            inp.disruption_crew_id,
        )
        eta_comparison = "\n".join(eta_lines) if eta_lines else "  (no delivery ETAs available)"

        await fleet.publish_agent_event(
            "fleet_agent",
            "tool_call",
            f"Calling tool_get_route_info to check driving routes from "
            f"{crew_id} to affected delivery locations...\n\n"
            f"Route to failed crew area:\n{route_info}\n\n"
            f"Delivery ETAs from {crew_id}:\n{eta_comparison}",
            summary=f"Checking routes from {crew_id} — ETAs calculated",
        )
        events_published += 1
        await asyncio.sleep(0.3)

        fleet_assessment = (
            f"COOLER FAILURE on {inp.disruption_crew_id} — "
            f"temperature has reached {failed_temp:.0f}F and climbing fast. "
            f"Ice cream integrity is compromised.\n\n"
            f"Fleet scan results:\n"
            f"- {crew_id} is the best reroute candidate — "
            f"cooler is nominal, {backup_capacity} capacity slots open, "
            f"and closest to the affected route.\n"
            f"- Route ETAs from {crew_id} to delivery locations:\n{eta_comparison}\n"
            f"- All ETAs are within deadline windows — reroute is feasible.\n"
            f"- Other AI-Crews either have cooler issues or lack capacity "
            f"for {len(inp.affected_order_ids)} additional orders.\n\n"
            f"RECOMMENDATION: Immediately reroute all {len(inp.affected_order_ids)} "
            f"orders to {crew_id} and return {inp.disruption_crew_id} to kitchen."
        )
        await fleet.publish_agent_event(
            "fleet_agent",
            "assessment",
            fleet_assessment,
            summary=f"{crew_id} is closest — recommending reroute",
        )
        events_published += 1
        await asyncio.sleep(0.3)
    else:
        await fleet.publish_agent_event(
            "fleet_agent",
            "offline",
            f"Fleet Agent is OFFLINE — unable to provide operational assessment. "
            f"Other agents will compensate.",
            summary="Fleet Agent offline",
        )
        events_published += 1
        await asyncio.sleep(0.3)

    # --- Customer Agent: customer impact assessment ---
    order_details = []
    total_servings = 0
    for oid in inp.affected_order_ids:
        order = await fleet.get_order(oid)
        if order:
            order_details.append(order)
            total_servings += order.servings

    if customer_online:
        # Hotel research — calls Google Search API if available, otherwise mock
        hotel_names = [o.hotel for o in order_details if o]
        hotel_context_lines = []
        for hotel in hotel_names:
            context = await _search_hotel_context(hotel)
            hotel_context_lines.append(context)
        hotel_context = "\n".join(hotel_context_lines)

        if hotel_context:
            await fleet.publish_agent_event(
                "customer_agent",
                "tool_call",
                f"Hotel Researcher (search) gathered live context:\n\n"
                f"{hotel_context}",
                summary="Hotel research complete — live event context gathered",
            )
            events_published += 1
            await asyncio.sleep(0.3)

        await fleet.publish_agent_event(
            "customer_agent",
            "tool_call",
            "Calling tool_get_order_priorities to check VIP status, "
            "deadlines, and servings at risk...",
            summary="Checking order priorities...",
        )
        events_published += 1
        await asyncio.sleep(0.4)

        order_lines = []
        for o in order_details:
            urgency = "URGENT" if o.deadline_minutes <= 30 else "moderate"
            order_lines.append(
                f"- {o.order_id} -> {o.hotel}: {o.priority.value.upper()} priority, "
                f"{o.servings} servings, {o.deadline_minutes}min deadline [{urgency}]"
            )
        orders_text = "\n".join(order_lines) if order_lines else "No order details available"

        customer_assessment = (
            f"CUSTOMER IMPACT — {total_servings} total servings at risk "
            f"across {len(inp.affected_order_ids)} orders:\n\n"
            f"{orders_text}\n\n"
        )
        if hotel_context:
            customer_assessment += f"HOTEL INTELLIGENCE:\n{hotel_context}\n\n"
        customer_assessment += (
            f"All affected orders are VIP-tier with tight deadlines. "
            f"These are high-profile hotel events — live event context confirms "
            f"active VIP gatherings. A melted delivery would be a reputation disaster.\n\n"
            f"RECOMMENDATION: Prioritize fastest possible reroute to {crew_id}. "
            f"Proactively notify hotel event coordinators of the slight delay. "
            f"VIP orders must be delivered first on the backup AI-Crew's route."
        )
        await fleet.publish_agent_event(
            "customer_agent",
            "assessment",
            customer_assessment,
            summary=f"{total_servings} servings at risk — reroute to {crew_id} ASAP",
        )
        events_published += 1
        await asyncio.sleep(0.3)
    else:
        await fleet.publish_agent_event(
            "customer_agent",
            "offline",
            f"Customer Agent is OFFLINE — unable to assess customer impact. "
            f"Other agents will compensate.",
            summary="Customer Agent offline",
        )
        events_published += 1
        await asyncio.sleep(0.3)

    # --- Resolver: synthesize and decide ---
    if resolver_online:
        # Adapt synthesis based on which agents contributed
        if fleet_online and customer_online:
            synthesis = (
                f"Fleet Agent recommends {crew_id} (best capacity + proximity). "
                f"Customer Agent confirms all {len(inp.affected_order_ids)} orders are "
                f"VIP with tight deadlines — {total_servings} servings at stake.\n\n"
                f"Both agents agree on immediate reroute. No conflict to resolve."
            )
            synthesis_summary = "Both agents agree — rerouting immediately"
        elif fleet_online:
            synthesis = (
                f"Customer Agent is OFFLINE. Operating with Fleet Agent input only.\n\n"
                f"Fleet Agent recommends {crew_id} as best reroute candidate. "
                f"Without customer impact data, defaulting to fastest reroute "
                f"to protect {len(inp.affected_order_ids)} orders from melting."
            )
            synthesis_summary = "Customer Agent offline — using fleet data only"
        elif customer_online:
            synthesis = (
                f"Fleet Agent is OFFLINE. Operating with Customer Agent input only.\n\n"
                f"Customer Agent reports {total_servings} VIP servings at risk. "
                f"Using backup crew selection ({crew_id}) based on proximity data. "
                f"Proceeding with reroute despite missing operational assessment."
            )
            synthesis_summary = "Fleet Agent offline — using customer data only"
        else:
            synthesis = (
                f"BOTH Fleet Agent and Customer Agent are OFFLINE.\n\n"
                f"Resolver operating autonomously with available system data. "
                f"Backup crew {crew_id} selected by proximity + capacity algorithm. "
                f"{len(inp.affected_order_ids)} orders will be rerouted immediately."
            )
            synthesis_summary = "Both agents offline — resolver acting autonomously"

        await fleet.publish_agent_event(
            "resolver",
            "synthesis",
            synthesis,
            summary=synthesis_summary,
        )
        events_published += 1
        await asyncio.sleep(0.3)

        await fleet.publish_agent_event(
            "resolver",
            "plan",
            f"RECOVERY PLAN FINALIZED:\n"
            f"1. Reroute {inp.affected_order_ids} -> {crew_id} (immediate)\n"
            f"2. {inp.disruption_crew_id} -> return to kitchen (cooler failed)\n"
            f"3. Notify hotel coordinators: slight delay, VIP orders prioritized\n"
            f"4. {crew_id} delivers VIP orders first by deadline urgency\n\n"
            f"Calling tool_submit_recovery_plan...",
            summary=f"Rerouting orders to {crew_id}",
        )
        events_published += 1
    else:
        # Resolver offline — publish emergency fallback
        await fleet.publish_agent_event(
            "resolver",
            "offline",
            f"Resolver Agent is OFFLINE — executing automatic failsafe.\n"
            f"System will reroute {inp.affected_order_ids} to {crew_id} "
            f"based on proximity algorithm without agent synthesis.",
            summary="Resolver offline — automatic failsafe",
        )
        events_published += 1

    plan = RecoveryPlan(
        reroute_to_crew_id=crew_id,
        reroute_order_ids=inp.affected_order_ids,
        return_crew_id=inp.disruption_crew_id,
        notifications=[
            f"Hotel notification: slight delay for orders {inp.affected_order_ids}",
            f"Orders rerouted to {crew_id}, ETA updated",
        ],
    )

    activity.logger.info(
        f"Mock resolver: rerouting {inp.affected_order_ids} "
        f"from {inp.disruption_crew_id} to {crew_id}"
    )
    return RunDisruptionResolverOutput(
        recovery_plan=plan,
        agent_events_published=events_published,
    )
