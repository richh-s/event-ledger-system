from __future__ import annotations
from typing import Any
from src.schema.events import StoredEvent
from src.projections.base import BaseProjection

class AgentSessionTraceProjection(BaseProjection):
    """
    Projection 3: Node-by-node agent execution trace.
    Maintains a detailed audit trail of exactly what each AI agent did during its session.
    """

    @property
    def projection_name(self) -> str:
        return "agent_session_trace"

    async def handle_event(self, conn, event: StoredEvent) -> None:
        """Processes agent session-related events into a trace view."""
        
        event_type = event.event_type
        payload = event.payload
        session_id = payload.get("session_id")
        
        if not session_id:
            return  # Skip if session_id is missing

        # 1. AgentSessionStarted → INSERT session header
        # ----------------------------------------------------------------------
        if event_type == "AgentSessionStarted":
            await conn.execute(
                """
                INSERT INTO agent_session_header (
                    session_id, agent_type, model_version, context_source
                ) VALUES ($1, $2, $3, $4)
                ON CONFLICT (session_id) DO NOTHING
                """,
                session_id, 
                payload.get("agent_type"), 
                payload.get("model_version"), 
                payload.get("context_source")
            )

        # 2. AgentNodeExecuted → INSERT trace row
        # ----------------------------------------------------------------------
        elif event_type == "AgentNodeExecuted":
            await conn.execute(
                """
                INSERT INTO agent_session_trace (
                    session_id, node_sequence, node_name, input_keys, output_keys, 
                    llm_called, llm_tokens_input, llm_tokens_output, llm_cost_usd, 
                    duration_ms, recorded_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (session_id, node_sequence) DO NOTHING
                """,
                session_id,
                payload.get("node_sequence"),
                payload.get("node_name"),
                payload.get("input_keys"),
                payload.get("output_keys"),
                payload.get("llm_called"),
                payload.get("llm_tokens_input"),
                payload.get("llm_tokens_output"),
                payload.get("llm_cost_usd"),
                payload.get("duration_ms"),
                event.recorded_at
            )

        # 3. AgentToolCalled → UPDATE most recent trace row, append tool call
        # ----------------------------------------------------------------------
        elif event_type == "AgentToolCalled":
            tool_call = {
                "tool_name": payload.get("tool_name"),
                "tool_input": payload.get("tool_input_summary"),
                "tool_output": payload.get("tool_output_summary"),
                "duration_ms": payload.get("tool_duration_ms"),
                "called_at": event.recorded_at.isoformat()
            }
            
            # Find the MOST RECENT node sequence for this session
            last_seq = await conn.fetchval(
                "SELECT MAX(node_sequence) FROM agent_session_trace WHERE session_id = $1",
                session_id
            )
            
            if last_seq is not None:
                await conn.execute(
                    """
                    UPDATE agent_session_trace SET 
                        tool_calls = tool_calls || $1::jsonb
                    WHERE session_id = $2 AND node_sequence = $3
                    """,
                    [tool_call], # Wrap in list for concatenation
                    session_id,
                    last_seq
                )

        # 4. AgentSessionCompleted → UPDATE session header with stats
        # ----------------------------------------------------------------------
        elif event_type == "AgentSessionCompleted":
            await conn.execute(
                """
                UPDATE agent_session_header SET 
                    total_nodes = $1, 
                    total_llm_calls = $2, 
                    total_cost_usd = $3, 
                    completed_at = $4,
                    updated_at = NOW()
                WHERE session_id = $5
                """,
                payload.get("total_nodes_executed"),
                payload.get("total_llm_calls"),
                payload.get("total_cost_usd"),
                event.recorded_at,
                session_id
            )
