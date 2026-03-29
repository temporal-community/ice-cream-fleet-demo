"""
Temporal workflows for the Meltdown ice cream delivery demo.

MeltdownDemoWorkflow — main orchestrator. Starts crew routes then
processes disruption and customer-change signals as they arrive (no fixed
beat phases). Signals are handled between delivery steps concurrently
with route execution.

CrewRouteWorkflow — per-crew delivery route (child workflow).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    import os

    from agent_fleet.activities import (
        assign_orders,
        coordinate_delivery_plan,
        deliver_order,
        execute_customer_change,
        execute_recovery,
        navigate_to,
        pickup_orders,
        publish_agent_event,
        resolve_disruption_mock,
    )
    from agent_fleet.locations import (
        CREW_ASSIGNMENTS,
        DELIVERY_DESTINATIONS,
        WAREHOUSE,
    )
    from agent_fleet.models import (
        AgentDisconnectInput,
        AssignOrdersInput,
        ConditionUpdate,
        CoordinateDeliveryInput,
        CrewDisconnectInput,
        CrewRouteInput,
        CrewRouteOrder,
        CustomerChangeInput,
        DeliverInput,
        DisruptionSignalInput,
        ExecuteCustomerChangeInput,
        MeltdownDemoInput,
        NavigateInput,
        OperatorDecision,
        PickupInput,
        PublishAgentEventInput,
        RecoveryPlan,
        RunDisruptionResolverInput,
        RunDisruptionResolverOutput,
    )

    _MOCK_MODE = not os.environ.get("GOOGLE_API_KEY")

    try:
        from google.adk.runners import Runner
        from google.adk.sessions import InMemorySessionService
        from google.genai.types import Content, Part

        from agent_fleet.agents import create_disruption_resolver

        _ADK_IMPORTS_OK = True
    except ImportError:
        _ADK_IMPORTS_OK = False

FAST_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
)
NAV_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=10,
)


# --- Per-crew route workflow ---


@workflow.defn
class CrewRouteWorkflow:
    """
    Executes a single AI-Crew's delivery route:
    navigate to kitchen -> pick up -> for each order: navigate to hotel -> deliver.

    Supports signals:
    - return_to_base: abort and return crew to kitchen
    - add_order: add a rerouted order to this crew's route
    """

    def __init__(self) -> None:
        self._return_to_base = False
        self._extra_orders: list[CrewRouteOrder] = []

    @workflow.signal
    async def return_to_base(self) -> None:
        self._return_to_base = True

    @workflow.signal
    async def add_order(self, order: CrewRouteOrder) -> None:
        self._extra_orders.append(order)

    @workflow.run
    async def run(self, inp: CrewRouteInput) -> str:
        crew_id = inp.crew_id
        orders = list(inp.orders)
        order_ids = [o.order_id for o in orders]

        # Step 1: Navigate to kitchen (pickup point)
        await workflow.execute_activity(
            navigate_to,
            NavigateInput(
                crew_id=crew_id,
                order_id=order_ids[0],
                target_lat=WAREHOUSE.lat,
                target_lng=WAREHOUSE.lng,
                leg="pickup",
                steps=2,
            ),
            start_to_close_timeout=timedelta(seconds=120),
            heartbeat_timeout=timedelta(seconds=15),
            retry_policy=NAV_RETRY,
        )

        if self._return_to_base:
            return f"AI-Crew {crew_id} returned to base (aborted)"

        # Step 2: Pick up all orders
        await workflow.execute_activity(
            pickup_orders,
            PickupInput(crew_id=crew_id, order_ids=order_ids),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=FAST_RETRY,
        )

        # Step 3: Deliver each order
        all_orders = list(orders)
        delivered = []

        while all_orders:
            if self._return_to_base:
                await workflow.execute_activity(
                    navigate_to,
                    NavigateInput(
                        crew_id=crew_id,
                        order_id=all_orders[0].order_id,
                        target_lat=WAREHOUSE.lat,
                        target_lng=WAREHOUSE.lng,
                        leg="pickup",
                        steps=5,
                    ),
                    start_to_close_timeout=timedelta(seconds=120),
                    heartbeat_timeout=timedelta(seconds=15),
                    retry_policy=NAV_RETRY,
                )
                return (
                    f"AI-Crew {crew_id} returned to base after delivering "
                    f"{len(delivered)} orders"
                )

            order = all_orders.pop(0)

            # Navigate to hotel
            await workflow.execute_activity(
                navigate_to,
                NavigateInput(
                    crew_id=crew_id,
                    order_id=order.order_id,
                    target_lat=order.delivery_lat,
                    target_lng=order.delivery_lng,
                    leg="delivery",
                    steps=10,
                ),
                start_to_close_timeout=timedelta(seconds=120),
                heartbeat_timeout=timedelta(seconds=15),
                retry_policy=NAV_RETRY,
            )

            # Deliver
            await workflow.execute_activity(
                deliver_order,
                DeliverInput(crew_id=crew_id, order_id=order.order_id),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FAST_RETRY,
            )
            delivered.append(order.order_id)

            # Check for extra orders (rerouted to this crew)
            if self._extra_orders:
                all_orders.extend(self._extra_orders)
                self._extra_orders.clear()

        return f"AI-Crew {crew_id} completed {len(delivered)} deliveries: {delivered}"


# --- Main demo orchestrator ---


@workflow.defn
class MeltdownDemoWorkflow:
    """
    Orchestrates the Meltdown demo with concurrent signal handling.

    Starts crew routes, then processes disruption and customer-change
    signals as they arrive — no fixed beat phases or timeout windows.
    A background signal loop runs concurrently with route execution,
    reacting to events whenever they're signaled.
    """

    def __init__(self) -> None:
        self._disruption: DisruptionSignalInput | None = None
        self._disruption_handled: bool = False
        self._pending_changes: list[CustomerChangeInput] = []
        self._pending_approvals: list[bool] = []
        self._routes_done: bool = False
        self._operator_decision: OperatorDecision | None = None
        self._pending_updates: list[ConditionUpdate] = []
        self._current_recommendation: RecoveryPlan | None = None
        self._disconnected_crews: set[str] = set()
        self._disconnected_agents: set[str] = set()

    # --- Signals ---

    @workflow.signal
    async def disruption_detected(self, disruption: DisruptionSignalInput) -> None:
        """Server detected a cooler malfunction."""
        self._disruption = disruption

    @workflow.signal
    async def customer_change(self, change: CustomerChangeInput) -> None:
        """Customer wants to change an order. Queued for processing."""
        self._pending_changes.append(change)

    @workflow.signal
    async def change_approved(self, approved: bool) -> None:
        """Human operator approved/rejected the pending customer change."""
        self._pending_approvals.append(approved)

    @workflow.signal
    async def operator_decision(self, decision: OperatorDecision) -> None:
        self._operator_decision = decision

    @workflow.signal
    async def updated_conditions(self, update: ConditionUpdate) -> None:
        self._pending_updates.append(update)

    @workflow.signal
    async def crew_disconnected(self, inp: CrewDisconnectInput) -> None:
        """A single crew has been disconnected. Its activities will fail and retry."""
        self._disconnected_crews.add(inp.crew_id)
        workflow.logger.info(f"Crew {inp.crew_id} disconnected — activities will retry")

    @workflow.signal
    async def crew_reconnected(self, inp: CrewDisconnectInput) -> None:
        """A crew has been reconnected. Its retrying activities will succeed."""
        self._disconnected_crews.discard(inp.crew_id)
        workflow.logger.info(f"Crew {inp.crew_id} reconnected — resuming")

    @workflow.signal
    async def agent_disconnected(self, inp: AgentDisconnectInput) -> None:
        """An agent has gone offline. Other agents compensate."""
        self._disconnected_agents.add(inp.agent_name)
        workflow.logger.info(f"Agent {inp.agent_name} disconnected")

    @workflow.signal
    async def agent_reconnected(self, inp: AgentDisconnectInput) -> None:
        """An agent is back online."""
        self._disconnected_agents.discard(inp.agent_name)
        workflow.logger.info(f"Agent {inp.agent_name} reconnected")

    # --- Main entry ---

    @workflow.run
    async def run(self, inp: MeltdownDemoInput) -> str:
        # Start all crew routes
        route_handles = await self._start_routes()

        # Run signal processing concurrently with route execution.
        # The signal loop reacts to disruption/customer-change signals
        # as they arrive, while routes proceed independently.
        signal_task = asyncio.create_task(self._signal_loop(route_handles, inp))

        # Wait for all routes to complete
        results = await self._await_routes(route_handles)

        # Tell the signal loop to stop, then let it drain any
        # signals that arrived just before routes finished
        self._routes_done = True
        await signal_task

        # Final drain — handle anything that snuck in
        await self._drain_pending_signals(route_handles, inp)

        mode = "ADK" if (not _MOCK_MODE and _ADK_IMPORTS_OK) else "mock"
        return f"Meltdown demo complete ({mode}). Results: {results}"

    # --- Signal processing loop ---

    def _has_pending_signal(self) -> bool:
        """Check if any unprocessed signal is waiting."""
        return (self._disruption is not None and not self._disruption_handled) or len(
            self._pending_changes
        ) > 0

    async def _signal_loop(self, route_handles: dict, inp: MeltdownDemoInput) -> None:
        """
        Background loop that processes signals as they arrive.

        Runs concurrently with route execution. Exits when routes
        are done (_routes_done flag set by the main flow).
        """
        while not self._routes_done:
            # Wait for a signal or periodic check
            try:
                await workflow.wait_condition(
                    lambda: self._has_pending_signal() or self._routes_done,
                    timeout=timedelta(seconds=2),
                )
            except TimeoutError:
                continue

            if self._routes_done:
                break

            await self._drain_pending_signals(route_handles, inp)

    async def _drain_pending_signals(self, route_handles: dict, inp: MeltdownDemoInput) -> None:
        """Process all pending signals: disruption first, then customer changes."""
        # Disruption takes priority
        if self._disruption is not None and not self._disruption_handled:
            self._disruption_handled = True
            await self._disruption_loop(route_handles, inp)

        # Process queued customer changes (FIFO)
        while self._pending_changes:
            change = self._pending_changes.pop(0)
            await self._process_customer_change(change)

    # --- Route lifecycle ---

    async def _start_routes(self) -> dict:
        """Assign orders and start crew route child workflows."""
        result = await workflow.execute_activity(
            assign_orders,
            AssignOrdersInput(),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=FAST_RETRY,
        )

        # Multi-agent coordination: agents reason about the delivery plan
        await workflow.execute_activity(
            coordinate_delivery_plan,
            CoordinateDeliveryInput(assignments=result.assignments),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=FAST_RETRY,
        )

        route_handles = {}
        for i, (crew_id, order_ids) in enumerate(CREW_ASSIGNMENTS.items()):
            if i > 0:
                await workflow.sleep(timedelta(seconds=2))

            orders = []
            for oid in order_ids:
                dest = DELIVERY_DESTINATIONS[oid]
                orders.append(
                    CrewRouteOrder(
                        order_id=oid,
                        hotel=dest["hotel"],
                        delivery_lat=dest["coords"].lat,
                        delivery_lng=dest["coords"].lng,
                    )
                )

            handle = await workflow.start_child_workflow(
                CrewRouteWorkflow.run,
                CrewRouteInput(crew_id=crew_id, orders=orders),
                id=f"route-{crew_id}",
            )
            route_handles[crew_id] = handle

        return route_handles

    async def _await_routes(self, route_handles: dict) -> list[str]:
        """Wait for all crew routes to complete concurrently."""
        results = await asyncio.gather(*route_handles.values(), return_exceptions=True)
        return [str(r) if isinstance(r, Exception) else r for r in results]

    # --- Disruption handling ---

    async def _disruption_loop(self, route_handles: dict, inp: MeltdownDemoInput) -> None:
        """
        Handle a cooler malfunction with operator-in-the-loop.

        Loops: resolve -> recommend -> wait for operator or new conditions.
        Breaks when operator approves, then executes the recovery plan.
        """
        d = self._disruption
        iteration = 1

        while True:
            # Grab any pending condition updates and clear the queue
            updates = list(self._pending_updates)
            self._pending_updates.clear()

            # Run resolver (ADK or mock) with current state
            result = await self._resolve_disruption(d, iteration=iteration, pending_updates=updates)

            if result.recovery_plan is None:
                # No plan (no backup available or resolver error) —
                # signal failed crew to return, nothing to reroute
                workflow.logger.warning(f"No recovery plan produced: {result.error}")
                if d.crew_id in route_handles:
                    await route_handles[d.crew_id].signal(CrewRouteWorkflow.return_to_base)
                return

            # Store recommendation for queries
            self._current_recommendation = result.recovery_plan

            # Publish "recommendation pending" event
            await workflow.execute_activity(
                publish_agent_event,
                PublishAgentEventInput(
                    agent_name="resolver",
                    event_type="recommendation_pending",
                    content=(
                        f"Recovery plan (iteration {iteration}): "
                        f"reroute orders {result.recovery_plan.reroute_order_ids} "
                        f"to {result.recovery_plan.reroute_to_crew_id}, "
                        f"return {result.recovery_plan.return_crew_id} to base"
                    ),
                    summary="Awaiting operator approval",
                ),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )

            # Wait for operator decision OR new condition updates
            self._operator_decision = None
            await workflow.wait_condition(
                lambda: self._operator_decision is not None or len(self._pending_updates) > 0
            )

            if self._operator_decision is not None:
                decision = self._operator_decision
                self._operator_decision = None

                if decision.action == "approve":
                    # Operator approved — execute the plan
                    break
                else:
                    # Operator rejected — loop again
                    iteration += 1
                    continue

            # New conditions arrived (no decision yet) — re-resolve
            self._operator_decision = None
            iteration += 1
            continue

        # Execute approved plan
        await self._execute_recovery(route_handles, self._current_recommendation)

    async def _resolve_disruption(
        self,
        d: DisruptionSignalInput,
        iteration: int = 1,
        pending_updates: list[ConditionUpdate] | None = None,
    ) -> RunDisruptionResolverOutput:
        """
        Run the disruption resolver — ADK agents or mock fallback.

        Both paths return RunDisruptionResolverOutput with the same shape.
        If ADK fails, falls through to mock seamlessly.
        """
        if pending_updates is None:
            pending_updates = []

        resolver_input = RunDisruptionResolverInput(
            disruption_crew_id=d.crew_id,
            cooler_temp_f=d.cooler_temp_f,
            affected_order_ids=d.affected_order_ids,
            description=d.description,
            iteration=iteration,
            pending_updates=pending_updates,
        )

        if not _MOCK_MODE and _ADK_IMPORTS_OK:
            result = await self._run_adk_resolver(d)
            if result.recovery_plan is not None:
                return result

            # ADK failed — fall through to mock
            workflow.logger.warning(
                f"ADK resolver failed ({result.error}), falling back to mock resolver"
            )

        # Mock resolver (deterministic) — also used as ADK fallback
        return await workflow.execute_activity(
            resolve_disruption_mock,
            resolver_input,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=FAST_RETRY,
        )

    async def _run_adk_resolver(self, d: DisruptionSignalInput) -> RunDisruptionResolverOutput:
        """
        Run ADK agents inline in the workflow.

        Each LLM call routes through TemporalModel (invoke_model activity),
        and each tool call is its own Temporal activity (via activity_tool).
        ADK imports are in the top-level pass-through block to satisfy the sandbox.
        """
        resolver_agent = create_disruption_resolver()
        if resolver_agent is None:
            return RunDisruptionResolverOutput(
                recovery_plan=None,
                error="ADK not available",
            )

        session_service = InMemorySessionService()
        runner = Runner(
            agent=resolver_agent,
            app_name="meltdown_demo",
            session_service=session_service,
        )

        session = await session_service.create_session(
            app_name="meltdown_demo",
            user_id="workflow",
        )

        prompt = (
            f"DISRUPTION DETECTED:\n"
            f"AI-Crew: {d.crew_id}\n"
            f"Cooler temperature: {d.cooler_temp_f}F and rising\n"
            f"Affected orders: {d.affected_order_ids}\n"
            f"Details: {d.description}\n\n"
            f"Assess the situation from both operational and customer perspectives, "
            f"then produce a recovery plan. The resolver MUST call "
            f"tool_submit_recovery_plan with the structured decision."
        )

        events_count = 0
        try:
            async for event in runner.run_async(
                user_id="workflow",
                session_id=session.id,
                new_message=Content(parts=[Part(text=prompt)]),
            ):
                events_count += 1
        except Exception as e:
            workflow.logger.error(f"Agent runner failed: {e}")
            return RunDisruptionResolverOutput(
                recovery_plan=None,
                agent_events_published=events_count,
                error=str(e),
            )

        # Read the structured plan from ADK session state
        updated_session = await session_service.get_session(
            app_name="meltdown_demo",
            user_id="workflow",
            session_id=session.id,
        )

        plan_dict = (updated_session.state or {}).get("recovery_plan")
        if plan_dict:
            plan = RecoveryPlan(**plan_dict)
        else:
            plan = None
            workflow.logger.warning("Resolver did not submit a structured plan")

        workflow.logger.info(
            f"Disruption resolver complete: {events_count} events, plan={'yes' if plan else 'no'}"
        )
        return RunDisruptionResolverOutput(
            recovery_plan=plan,
            agent_events_published=events_count,
        )

    async def _execute_recovery(
        self,
        route_handles: dict,
        plan: RecoveryPlan,
    ) -> None:
        """Execute a recovery plan: signal failed crew, reroute orders, notify."""
        if plan.return_crew_id in route_handles:
            await route_handles[plan.return_crew_id].signal(CrewRouteWorkflow.return_to_base)

        await workflow.execute_activity(
            execute_recovery,
            plan.to_execute_recovery_input(),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=FAST_RETRY,
        )

        if plan.reroute_to_crew_id in route_handles:
            for oid in plan.reroute_order_ids:
                dest = DELIVERY_DESTINATIONS.get(oid)
                if dest:
                    await route_handles[plan.reroute_to_crew_id].signal(
                        CrewRouteWorkflow.add_order,
                        CrewRouteOrder(
                            order_id=oid,
                            hotel=dest["hotel"],
                            delivery_lat=dest["coords"].lat,
                            delivery_lng=dest["coords"].lng,
                        ),
                    )

    # --- Customer change handling ---

    async def _process_customer_change(self, change: CustomerChangeInput) -> None:
        """Process a single customer change with human-in-the-loop approval."""
        await workflow.execute_activity(
            publish_agent_event,
            PublishAgentEventInput(
                agent_name="customer_agent",
                event_type="customer_request",
                content=(
                    f"Customer change request for {change.order_id}: "
                    f"{change.change_type} — {change.new_details}"
                ),
            ),
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=FAST_RETRY,
        )

        # Wait for the next queued approval signal so overlapping decisions
        # are consumed in the same order the UI submitted them.
        await workflow.wait_condition(lambda: len(self._pending_approvals) > 0)
        approved = self._pending_approvals.pop(0)

        if approved:
            await workflow.execute_activity(
                execute_customer_change,
                ExecuteCustomerChangeInput(
                    order_id=change.order_id,
                    change_type=change.change_type,
                    new_lat=change.new_lat,
                    new_lng=change.new_lng,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FAST_RETRY,
            )
            await workflow.execute_activity(
                publish_agent_event,
                PublishAgentEventInput(
                    agent_name="resolver",
                    event_type="change_executed",
                    content=f"Customer change approved and executed for {change.order_id}",
                ),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )
        else:
            await workflow.execute_activity(
                publish_agent_event,
                PublishAgentEventInput(
                    agent_name="resolver",
                    event_type="change_rejected",
                    content=f"Customer change rejected for {change.order_id}",
                ),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )
