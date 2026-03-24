from __future__ import annotations
from typing import Any
from src.schema.events import StoredEvent
from src.projections.base import BaseProjection

class DecisionTimelineProjection(BaseProjection):
    """
    Projection 2: Complete, causally-linked decision history.
    Maintains the chronological record of each loan application's state transitions.
    """

    @property
    def projection_name(self) -> str:
        return "decision_timeline"

    async def handle_event(self, conn, event: StoredEvent) -> None:
        """Appends events to a chronological timeline with sequence numbers."""
        
        event_type = event.event_type
        payload = event.payload
        metadata = event.metadata or {}
        app_id = payload.get("application_id")
        
        if not app_id:
            return  # Skip if application_id is missing

        # Calculate chronological sequence for this application
        # ----------------------------------------------------------------------
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM decision_timeline WHERE application_id = $1",
            app_id
        )
        sequence = (count or 0) + 1

        # Extract specific metadata
        # ----------------------------------------------------------------------
        causation_id = metadata.get("causation_id")
        
        # Link contributing sessions for orchestrator decisions
        agent_session_ids = []
        if event_type == "DecisionGenerated":
            agent_session_ids = payload.get("contributing_sessions", [])
        elif "session_id" in payload:
            agent_session_ids = [payload["session_id"]]

        # Determine if this is a terminal event
        terminal_events = ["ApplicationApproved", "ApplicationDeclined"]
        is_terminal = event_type in terminal_events

        # UPSERT event to the timeline
        # ----------------------------------------------------------------------
        await conn.execute(
            """
            INSERT INTO decision_timeline (
                event_id, application_id, sequence, event_type, causation_id, 
                description, payload, agent_session_ids, recorded_at, global_position, is_terminal
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (event_id) DO NOTHING
            """,
            event.event_id,
            app_id,
            sequence,
            event_type,
            causation_id,
            f"Event recorded: {event_type}", # description placeholder
            payload,
            agent_session_ids,
            event.recorded_at,
            event.global_position,
            is_terminal
        )
