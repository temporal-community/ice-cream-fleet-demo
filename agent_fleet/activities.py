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

    for step in range(1, inp.steps + 1):
        activity.heartbeat(f"step {step}/{inp.steps}")

        # Check disconnect mid-navigation too
        if await fleet.is_crew_disconnected(inp.crew_id):
            activity.logger.warning(f"{inp.crew_id} disconnected at step {step}/{inp.steps}")
            raise RuntimeError(
                f"AI-Crew {inp.crew_id} disconnected mid-navigation at step {step}"
            )

        fraction = step / inp.steps
        new_lat = start_lat + (inp.target_lat - start_lat) * fraction
        new_lng = start_lng + (inp.target_lng - start_lng) * fraction

        await fleet.update_crew_position(inp.crew_id, new_lat, new_lng)

        # Track nav steps for demo event triggers (cooler malfunction)
        await fleet.increment_nav_step(inp.crew_id)

        # Simulate flight time — 0.8s per step
        await asyncio.sleep(0.8)

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

        fleet_assessment = (
            f"COOLER FAILURE on {inp.disruption_crew_id} — "
            f"temperature has reached {failed_temp:.0f}F and climbing fast. "
            f"Ice cream integrity is compromised.\n\n"
            f"Fleet scan results:\n"
            f"- {crew_id} is the best reroute candidate — "
            f"cooler is nominal, {backup_capacity} capacity slots open, "
            f"and closest to the affected route.\n"
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
            f"All affected orders are VIP-tier with tight deadlines. "
            f"These are high-profile hotel events — MGM pool parties, "
            f"Caesars banquets. A melted delivery would be a reputation disaster.\n\n"
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
