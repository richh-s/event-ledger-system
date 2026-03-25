from dataclasses import dataclass, field
from typing import Any, Tuple, List, Optional
from enum import Enum
from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.base import ReconstructionStatus

class SessionHealth(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    CRITICAL = "CRITICAL"
    UNKNOWN = "UNKNOWN"

@dataclass
class AgentContext:
    session_id: str
    context_text: str
    last_event_position: int
    pending_work: List[str] = field(default_factory=list)
    session_health_status: SessionHealth = SessionHealth.UNKNOWN

def reconstruct_agent_context(session_id: str, events: list[Any]) -> Tuple[AgentSessionAggregate, AgentContext, ReconstructionStatus]:
    """
    Agent memory reconstruction from event stream with token budget.
    """
    if not events:
        return AgentSessionAggregate(session_id), AgentContext(session_id, "", 0), ReconstructionStatus.HEALTHY

    # If events exist, use the stream_id from the first event for the aggregate
    # to ensure consistency with the event store.
    stream_id = events[0].stream_id if hasattr(events[0], 'stream_id') else session_id
    agg = AgentSessionAggregate(stream_id)
    agg.load_from_history(events)

    # 1. Logic for Selective Preservation
    verbatim_events = []
    summary_parts = []
    
    # Identify verbatim indices: Last 3 and any Error/Failure
    verbatim_indices = set(range(max(0, len(events) - 3), len(events)))
    for i, e in enumerate(events):
        if "Failed" in e.event_type or "Error" in e.event_type:
            verbatim_indices.add(i)

    for i, e in enumerate(events):
        if i in verbatim_indices:
            verbatim_events.append(f"[{e.event_type}] {getattr(e, 'payload', '{}') if hasattr(e, 'payload') else e.to_payload() if hasattr(e, 'to_payload') else str(e)}")
        else:
            summary_parts.append(f"{e.event_type}")

    summary_text = "Previously: " + ", ".join(summary_parts) if summary_parts else ""
    context_text = summary_text + "\nRecent History:\n" + "\n".join(verbatim_events)

    # 2. Identify Pending work
    pending_work = []
    if agg.last_successful_node and not agg.is_completed and not agg.is_failed:
        pending_work.append(f"Resume from after node: {agg.last_successful_node}")

    # 3. Status logic
    status = agg.reconstruction_status
    # Check for decision without completion
    last_event = events[-1]
    is_decision_event = "Decision" in last_event.event_type or "Analysis" in last_event.event_type
    if is_decision_event and not agg.is_completed:
        status = ReconstructionStatus.NEEDS_RECONCILIATION

    ctx = AgentContext(
        session_id=session_id,
        context_text=context_text,
        last_event_position=len(events),
        pending_work=pending_work,
        session_health_status=SessionHealth.HEALTHY if not agg.is_failed else SessionHealth.CRITICAL
    )
    
    return agg, ctx, status
