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

try:
    from google.adk.tools import google_search

    _GOOGLE_SEARCH_AVAILABLE = True
except ImportError:
    google_search = None
    _GOOGLE_SEARCH_AVAILABLE = False

from agent_fleet.activities import (
    tool_get_fleet_status,
    tool_get_order_priorities,
    tool_get_route_info,
    tool_publish_agent_event,
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


def create_hotel_researcher() -> Agent | None:
    """
    Hotel Researcher — uses Google Search to gather live context about
    delivery destination hotels (events, occupancy, VIP considerations).

    Uses google_search as its sole tool (Gemini constraint: google_search
    cannot be combined with other tools in the same agent).

    Returns None if google_search is not available.
    """
    if not _GOOGLE_SEARCH_AVAILABLE or google_search is None:
        return None

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
            "involved in the current delivery disruption. Search for:\n"
            "- Current events at the affected hotels (conferences, pool parties, galas)\n"
            "- Any VIP or celebrity presence that increases delivery urgency\n"
            "- Hotel reputation and standards for catering service\n\n"
            "Be concise. Return 2-3 bullet points per hotel with the most relevant "
            "findings. Focus on information that would affect delivery priority.\n\n"
            "Hotels on the Las Vegas Strip: MGM Grand, Caesars Palace, Mandalay Bay."
        ),
        tools=[google_search],
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


def create_customer_assessment_pipeline() -> SequentialAgent | Agent:
    """
    Customer Assessment Pipeline — SequentialAgent that first researches hotels
    via Google Search, then runs the Customer Agent with enriched context.

    Falls back to standalone Customer Agent if google_search is unavailable.
    """
    hotel_researcher = create_hotel_researcher()

    if hotel_researcher is None:
        # google_search not available — run Customer Agent standalone
        return create_customer_agent()

    return SequentialAgent(
        name="customer_assessment",
        sub_agents=[hotel_researcher, create_customer_agent()],
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
            "Customer Agent (customer impact). Synthesize them into a concrete "
            "recommended plan.\n\n"
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
