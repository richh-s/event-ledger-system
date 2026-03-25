from __future__ import annotations
import logging
from src.projections.base import BaseProjection
from src.models import StoredEvent

logger = logging.getLogger(__name__)

class AgentDecisionTraceProjection(BaseProjection):
    """Projection 2: Links decisions back to the causal agent session."""
    @property
    def projection_name(self) -> str:
        return "agent_decision_trace"

    async def handle_event(self, conn, event: StoredEvent) -> None:
        p = event.payload
        metadata = event.metadata or {}
        correlation_id = metadata.get('correlation_id')
        
        # We only care about AgentOutputWritten to track the trace
        # OR DecisionGenerated which is the terminal business event
        if event.event_type == "AgentOutputWritten":
            app_id = p.get('application_id')
            session_id = p.get('session_id')
            if not app_id or not session_id:
                return
                
            await conn.execute(
                """
                INSERT INTO agent_decision_trace (
                    application_id, session_id, agent_type, 
                    output_summary, recorded_at, global_position, correlation_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (application_id, session_id, global_position) DO UPDATE SET
                    output_summary = EXCLUDED.output_summary,
                    correlation_id = EXCLUDED.correlation_id
                """,
                app_id, session_id, p.get('agent_type'),
                p.get('output_summary'), event.recorded_at, 
                event.global_position, correlation_id
            )
        
        elif event.event_type == "AgentSessionStarted":
            # Update trace with model/graph version if we already have a skeleton row
            # Actually, better to just insert/upsert the basic info
            app_id = p.get('application_id')
            session_id = p.get('session_id')
            if not app_id or not session_id:
                return

            await conn.execute(
                """
                INSERT INTO agent_decision_trace (
                    application_id, session_id, agent_type,
                    model_version, graph_version, recorded_at, global_position, correlation_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (application_id, session_id, global_position) DO UPDATE SET
                    model_version = EXCLUDED.model_version,
                    graph_version = EXCLUDED.graph_version
                """,
                app_id, session_id, p.get('agent_type'),
                p.get('model_version'), p.get('langgraph_graph_version'),
                event.recorded_at, event.global_position, correlation_id
            )

    async def rebuild_from_scratch(self, conn) -> None:
        await conn.execute("TRUNCATE agent_decision_trace")
        await conn.execute(
            "UPDATE projection_checkpoints SET last_position = 0 WHERE projection_name = $1",
            self.projection_name
        )
