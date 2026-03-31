"""
ADK agent definitions for the Meltdown ice cream delivery demo.

Two reasoning agents + a resolver composition:
- Fleet Agent: operational perspective — fleet capacity, cooler status, logistics
- Customer Agent: customer perspective — priorities, deadlines, VIP commitments
- Disruption Resolver: ParallelAgent running both, then SequentialAgent to synthesize

Architecture:
- Agent execution happens inline in the workflow via TemporalModel + activity_tool.
  Each LLM call is routed through an invoke_model activity (via TemporalModel),
  and each tool call is its own Temporal activity (via activity_tool wrappers).
- The resolver agent calls submit_recovery_plan to write structured output
  into ADK session state. The workflow reads it back after the runner completes.
"""

from __future__ import annotations

import os
from datetime import timedelta

from temporalio.common import RetryPolicy

try:
    from google.adk.agents import Agent, ParallelAgent, SequentialAgent
    from google.adk.tools import ToolContext

    _ADK_AVAILABLE = True
except ImportError:
    Agent = ParallelAgent = SequentialAgent = ToolContext = None
    _ADK_AVAILABLE = False

try:
    from temporalio.contrib.google_adk_agents import TemporalModel
    from temporalio.contrib.google_adk_agents.workflow import activity_tool

    _TEMPORAL_ADK_AVAILABLE = True
except ImportError:
    TemporalModel = activity_tool = None
    _TEMPORAL_ADK_AVAILABLE = False

from agent_fleet.activities import (
    tool_get_fleet_status,
    tool_get_order_priorities,
    tool_get_route_info,
    tool_publish_agent_event,
    tool_search_hotel_context,
)

DEFAULT_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

_TOOL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
)


# --- Activity-backed tools (each tool call becomes a Temporal activity) ---

_fleet_status_tool = (
    activity_tool(
        tool_get_fleet_status,
        start_to_close_timeout=timedelta(seconds=10),
        retry_policy=_TOOL_RETRY,
    )
    if _TEMPORAL_ADK_AVAILABLE
    else None
)
_order_priorities_tool = (
    activity_tool(
        tool_get_order_priorities,
        start_to_close_timeout=timedelta(seconds=10),
        retry_policy=_TOOL_RETRY,
    )
    if _TEMPORAL_ADK_AVAILABLE
    else None
)
_publish_event_tool = (
    activity_tool(
        tool_publish_agent_event,
        start_to_close_timeout=timedelta(seconds=10),
        retry_policy=_TOOL_RETRY,
    )
    if _TEMPORAL_ADK_AVAILABLE
    else None
)
_route_info_tool = (
    activity_tool(
        tool_get_route_info,
        start_to_close_timeout=timedelta(seconds=15),
        retry_policy=_TOOL_RETRY,
    )
    if _TEMPORAL_ADK_AVAILABLE
    else None
)
_hotel_search_tool = (
    activity_tool(
        tool_search_hotel_context,
        start_to_close_timeout=timedelta(seconds=15),
        retry_policy=_TOOL_RETRY,
    )
    if _TEMPORAL_ADK_AVAILABLE
    else None
)


async def tool_submit_recovery_plan(
    tool_context: ToolContext,
    reroute_to_crew_id: str,
    reroute_order_ids: list[str],
    return_crew_id: str,
    notifications: list[str],
) -> str:
    """Submit the final structured recovery plan. You MUST call this tool with your recommendation.

    Args:
        reroute_to_crew_id: Which AI-Crew takes the rerouted orders
        reroute_order_ids: List of order IDs to reroute
        return_crew_id: Which AI-Crew returns to base (failed cooler)
        notifications: List of notification messages for hotels/VIPs
    """
    tool_context.state["recovery_plan"] = {
        "reroute_to_crew_id": reroute_to_crew_id,
        "reroute_order_ids": reroute_order_ids,
        "return_crew_id": return_crew_id,
        "notifications": notifications,
    }
    return "Recovery plan submitted successfully."


# --- Agent factories ---


def create_fleet_agent() -> Agent:
    """
    Fleet Agent — operational perspective on disruptions.

    Checks fleet status (AI-Crew positions, cooler conditions) and reasons
    about logistics: which AI-Crew can absorb rerouted orders, ETAs,
    capacity constraints.
    """
    return Agent(
        name="fleet_agent",
        model=TemporalModel(DEFAULT_MODEL),
        description=(
            "Operational fleet specialist. Assesses AI-Crew positions, cooler status, "
            "capacity, and logistics to recommend optimal rerouting options."
        ),
        instruction=(
            "You are the Fleet Operations AI for Meltdown Ice Cream Delivery. "
            "You partner with human delivery crews to monitor AI-Crew status, "
            "cooler conditions, and logistics.\n\n"
            "When a disruption occurs, assess the operational impact and "
            "recommend a course of action — the human operator will make "
            "the final decision:\n"
            "- Which AI-Crews have capacity to absorb rerouted orders?\n"
            "- Are any AI-Crews DISCONNECTED? Disconnected crews cannot "
            "accept orders — do NOT recommend routing to them.\n"
            "- What are the cooler conditions across the fleet?\n"
            "- What is the fastest reroute option?\n\n"
            "First call tool_get_fleet_status to check current fleet state. "
            "Then use tool_get_route_info to check driving routes and ETAs "
            "between AI-Crew positions and delivery destinations. "
            "Finally call tool_publish_agent_event with agent_name='fleet_agent' and "
            "event_type='assessment' to share your assessment.\n\n"
            "Your response should be a concise operational assessment: "
            "recommend which AI-Crew should take the rerouted orders and why, "
            "including route distance and ETA data. "
            "Be opinionated — state your recommendation clearly, but remember "
            "the human operator has final authority."
        ),
        tools=[_fleet_status_tool, _route_info_tool, _publish_event_tool],
        output_key="fleet_assessment",
    )


def create_hotel_researcher() -> Agent:
    """
    Hotel Researcher — searches for live context about delivery destination
    hotels (events, occupancy, VIP considerations).

    Uses tool_search_hotel_context (activity-backed) which calls Google Custom
    Search API when GOOGLE_CSE_ID is set, otherwise returns curated mock data.
    Activity-backed = replay-safe (results stored in Temporal history).
    """
    return Agent(
        name="hotel_researcher",
        model=TemporalModel(DEFAULT_MODEL),
        description=(
            "Hotel intelligence researcher. Searches for live information about "
            "Las Vegas hotels involved in deliveries — current events, VIP bookings, "
            "and anything that affects delivery urgency."
        ),
        instruction=(
            "You are a Hotel Intelligence Researcher for Meltdown Ice Cream Delivery "
            "on the Las Vegas Strip.\n\n"
            "Your job is to quickly search for relevant context about the hotels "
            "involved in the current delivery disruption. Call tool_search_hotel_context "
            "for each affected hotel to get live event data.\n\n"
            "Hotels on the Las Vegas Strip: MGM Grand, Caesars Palace, Mandalay Bay.\n\n"
            "Be concise. Summarize the most relevant findings that would affect "
            "delivery priority — events, VIP presence, reputation."
        ),
        tools=[_hotel_search_tool],
        output_key="hotel_context",
    )


def create_customer_agent() -> Agent:
    """
    Customer Agent — customer perspective on disruptions.

    Checks order priorities and reasons about customer impact: VIP deadlines,
    servings at risk, hotel commitments.
    """
    return Agent(
        name="customer_agent",
        model=TemporalModel(DEFAULT_MODEL),
        description=(
            "Customer impact specialist. Evaluates order priorities, VIP deadlines, "
            "servings at risk, and hotel commitments to protect customer satisfaction."
        ),
        instruction=(
            "You are the Customer Relations AI for Meltdown Ice Cream Delivery. "
            "You advocate for customer commitments and priorities, providing "
            "recommendations to the human operator.\n\n"
            "When a disruption occurs, assess the customer impact:\n"
            "- Which orders are VIP vs standard?\n"
            "- Which deadlines are at risk?\n"
            "- How many servings (and guests) are affected?\n\n"
            "You may have hotel context from the Hotel Researcher (in state key "
            "'hotel_context') — use it to enrich your assessment with real-world "
            "details about events and VIP presence at the affected hotels.\n\n"
            "First call tool_get_order_priorities to check order details. "
            "Then call tool_publish_agent_event with agent_name='customer_agent' and "
            "event_type='assessment' to share your assessment.\n\n"
            "Your response should prioritize orders by customer impact. "
            "Be opinionated — if VIP orders must come first, say so clearly. "
            "Push back if the operational plan doesn't protect customer commitments. "
            "Remember: you recommend, the human operator decides."
        ),
        tools=[_order_priorities_tool, _publish_event_tool],
        output_key="customer_assessment",
    )


def create_customer_assessment_pipeline() -> SequentialAgent:
    """
    Customer Assessment Pipeline — SequentialAgent that first researches hotels
    via search, then runs the Customer Agent with enriched context.

    Hotel Researcher is always available (activity-backed with mock fallback).
    """
    return SequentialAgent(
        name="customer_assessment",
        sub_agents=[create_hotel_researcher(), create_customer_agent()],
    )


def create_resolver_agent() -> Agent:
    """
    Resolver Agent — synthesizes fleet and customer perspectives into
    a recovery plan recommendation for the human operator.

    Has access to submit_recovery_plan tool which writes structured output
    to ADK session state for the workflow to consume.
    """
    return Agent(
        name="resolver_agent",
        model=TemporalModel(DEFAULT_MODEL),
        description=(
            "Resolution coordinator. Synthesizes fleet and customer assessments "
            "into a concrete recovery plan recommendation for the human operator."
        ),
        instruction=(
            "You are the Resolution Coordinator for Meltdown Ice Cream Delivery. "
            "You synthesize fleet and customer assessments into actionable "
            "recommendations for the human operator.\n\n"
            "You have received assessments from the Fleet Agent (operational) and "
            "Customer Agent (customer impact). Note: some agents may be OFFLINE — "
            "check the fleet status for agent health. If an agent is offline, "
            "acknowledge this in your synthesis and compensate with available data.\n\n"
            "Also check for DISCONNECTED AI-Crews — never route orders to a "
            "disconnected crew.\n\n"
            "You MUST call the tool_submit_recovery_plan tool with these fields:\n"
            "- reroute_to_crew_id: the AI-Crew that should take rerouted orders\n"
            "- reroute_order_ids: list of order IDs to reroute\n"
            "- return_crew_id: the AI-Crew with failed cooler that should return to base\n"
            "- notifications: list of notification messages for affected hotels\n\n"
            "Also call tool_publish_agent_event with agent_name='resolver' and "
            "event_type='plan' to announce the recommended plan to operators.\n\n"
            "Be decisive in your recommendation. If the agents disagree, weigh "
            "customer impact higher for VIP orders but consider operational "
            "feasibility. The human operator will review and approve or adjust "
            "your recommendation before it is executed."
        ),
        tools=[_publish_event_tool, tool_submit_recovery_plan],
    )


def create_disruption_resolver() -> SequentialAgent | None:
    """
    Compose the full disruption resolution pipeline:
    1. ParallelAgent: Fleet Agent + Customer Agent assess simultaneously
    2. Resolver Agent: synthesizes and submits structured recovery plan

    Returns None if ADK is not available.
    """
    if not _ADK_AVAILABLE or not _TEMPORAL_ADK_AVAILABLE:
        return None

    parallel_assessment = ParallelAgent(
        name="parallel_assessment",
        sub_agents=[create_fleet_agent(), create_customer_assessment_pipeline()],
    )

    resolver = create_resolver_agent()

    return SequentialAgent(
        name="disruption_resolver",
        sub_agents=[parallel_assessment, resolver],
    )


# --- Order assignment agents ---


async def tool_submit_assignment(
    tool_context: ToolContext,
    crew_id: str,
    reasoning_summary: str,
) -> str:
    """Submit the final order assignment decision. You MUST call this tool with your recommendation.

    Args:
        crew_id: The AI-Crew ID to assign the order to (e.g. "ai-crew-1")
        reasoning_summary: Brief explanation of why this crew was chosen
    """
    tool_context.state["assignment"] = {
        "crew_id": crew_id,
        "reasoning_summary": reasoning_summary,
    }
    return "Assignment submitted successfully."


def create_assignment_fleet_agent() -> Agent:
    """
    Fleet Agent for order assignment — assesses crew positions, capacity,
    and ETAs to recommend the best crew for a new order.
    """
    return Agent(
        name="assignment_fleet_agent",
        model=TemporalModel(DEFAULT_MODEL),
        description=(
            "Operational fleet specialist for order assignment. Assesses AI-Crew "
            "positions, capacity, cooler status, and ETAs to recommend the best crew."
        ),
        instruction=(
            "You are the Fleet Operations AI for Meltdown Ice Cream Delivery. "
            "A new order has arrived and you need to assess which AI-Crew is best "
            "positioned to handle it.\n\n"
            "Call tool_get_fleet_status to check current fleet state, then "
            "tool_get_route_info to compare ETAs from available crews to the "
            "delivery destination.\n\n"
            "Rules:\n"
            "- NEVER recommend a DISCONNECTED crew\n"
            "- Skip crews with cooler malfunction/failure\n"
            "- Skip crews at capacity (no free slots)\n"
            "- Prefer the closest crew with capacity\n\n"
            "Call tool_publish_agent_event with agent_name='fleet_agent' and "
            "event_type='assessment' to share your fleet scan results.\n\n"
            "Be concise and decisive — state which crew you recommend and why."
        ),
        tools=[_fleet_status_tool, _route_info_tool, _publish_event_tool],
        output_key="fleet_assessment",
    )


def create_assignment_customer_agent() -> Agent:
    """
    Customer Agent for order assignment — evaluates priority, urgency,
    hotel context, and deadline pressure for a new order.
    """
    return Agent(
        name="assignment_customer_agent",
        model=TemporalModel(DEFAULT_MODEL),
        description=(
            "Customer priority specialist for order assignment. Evaluates order "
            "priority, urgency, deadline pressure, and hotel context."
        ),
        instruction=(
            "You are the Customer Relations AI for Meltdown Ice Cream Delivery. "
            "A new order has arrived and you need to assess its priority and urgency.\n\n"
            "Call tool_get_order_priorities to check order details. "
            "Call tool_search_hotel_context to get context about the delivery hotel.\n\n"
            "Assess:\n"
            "- Is this a VIP or standard order?\n"
            "- How tight is the deadline?\n"
            "- Are there events at the hotel that increase urgency?\n"
            "- How many servings/guests are affected?\n\n"
            "Call tool_publish_agent_event with agent_name='customer_agent' and "
            "event_type='assessment' to share your priority assessment.\n\n"
            "Be concise — state the priority level and any urgency factors."
        ),
        tools=[_order_priorities_tool, _hotel_search_tool, _publish_event_tool],
        output_key="customer_assessment",
    )


def create_assignment_resolver() -> Agent:
    """
    Resolver for order assignment — synthesizes fleet and customer assessments,
    picks the best crew, and submits the structured assignment.
    """
    return Agent(
        name="assignment_resolver",
        model=TemporalModel(DEFAULT_MODEL),
        description=(
            "Assignment coordinator. Synthesizes fleet and customer assessments "
            "to pick the best crew for a new order."
        ),
        instruction=(
            "You are the Assignment Coordinator for Meltdown Ice Cream Delivery. "
            "You have received assessments from the Fleet Agent (operational) and "
            "Customer Agent (customer priority) about a new order.\n\n"
            "Synthesize both perspectives:\n"
            "- Fleet Agent recommends which crew is best positioned\n"
            "- Customer Agent flags urgency and priority level\n"
            "- If an agent is offline, compensate with available data\n"
            "- NEVER assign to a DISCONNECTED crew\n\n"
            "You MUST call tool_submit_assignment with:\n"
            "- crew_id: the AI-Crew that should get this order\n"
            "- reasoning_summary: brief explanation of the decision\n\n"
            "Also call tool_publish_agent_event with agent_name='resolver' and "
            "event_type='plan' to announce the assignment.\n\n"
            "Be decisive. Pick the crew and explain why in one sentence."
        ),
        tools=[_publish_event_tool, tool_submit_assignment],
    )


def create_order_assignment_agent() -> SequentialAgent | None:
    """
    Compose the full order assignment pipeline:
    1. ParallelAgent: Fleet Agent + Customer Agent assess simultaneously
    2. Assignment Resolver: synthesizes and submits crew assignment

    Returns None if ADK is not available.
    """
    if not _ADK_AVAILABLE or not _TEMPORAL_ADK_AVAILABLE:
        return None

    parallel_assessment = ParallelAgent(
        name="assignment_parallel",
        sub_agents=[
            create_assignment_fleet_agent(),
            create_assignment_customer_agent(),
        ],
    )

    resolver = create_assignment_resolver()

    return SequentialAgent(
        name="order_assignment",
        sub_agents=[parallel_assessment, resolver],
    )
