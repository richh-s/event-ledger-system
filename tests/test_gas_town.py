import pytest
import uuid
from datetime import datetime
from src.integrity.gas_town import reconstruct_agent_context
from src.models.events import (
    AgentType, AgentSessionStarted, AgentNodeExecuted, AgentContextLoaded,
    AgentOutputWritten, AgentSessionCompleted
)
from src.aggregates.base import ReconstructionStatus

@pytest.mark.asyncio
async def test_gas_town_reconstruction_lifecycle():
    """
    Simulated crash recovery: 5 events appended,
    reconstruct_agent_context() called without in-memory agent, 
    verify reconstructed context and selective preservation.
    """
    session_id = f"sess-{uuid.uuid4().hex[:6]}"
    
    # Pre-generate IDs to link them
    ctx_event_id = uuid.uuid4()
    
    # 1. Simulate 5 events (Crash after 5)
    events = [
        AgentSessionStarted(
            session_id=session_id, agent_type=AgentType.CREDIT_ANALYSIS, agent_id="A1",
            application_id="APP-1", model_version="gpt-4", langgraph_graph_version="1",
            context_source="s3://", context_token_count=100, started_at=datetime.utcnow()
        ),
        AgentNodeExecuted(
            session_id=session_id, agent_type=AgentType.CREDIT_ANALYSIS, node_name="start",
            node_sequence=1, input_keys=[], output_keys=["k1"], llm_called=False,
            duration_ms=10, executed_at=datetime.utcnow()
        ),
        AgentContextLoaded(
            event_id=ctx_event_id,
            session_id=session_id, agent_type=AgentType.CREDIT_ANALYSIS, 
            context_source="loan-APP1",
            context_version=1,
            context_hash="sha256:abc",
            loaded_at=datetime.utcnow()
        ),
        AgentNodeExecuted(
            session_id=session_id, agent_type=AgentType.CREDIT_ANALYSIS, node_name="analyzer",
            node_sequence=2, input_keys=["k1"], output_keys=["decision"], llm_called=True,
            duration_ms=500, executed_at=datetime.utcnow()
        ),
        AgentOutputWritten(
            session_id=session_id, agent_type=AgentType.CREDIT_ANALYSIS, 
            application_id="APP-1",
            events_written=[],
            context_event_id=str(ctx_event_id),
            output_summary="Approved", 
            written_at=datetime.utcnow()
        )
    ]
    
    # 2. Call reconstruct_agent_context
    agg, ctx, status = reconstruct_agent_context(session_id, events)
    
    # 3. Verify reconstructed state (Rubric Assertions)
    assert status == ReconstructionStatus.HEALTHY
    assert agg.nodes_executed == 2
    assert ctx.last_event_position == 5
    assert "Previously: AgentSessionStarted, AgentNodeExecuted" in ctx.context_text
    assert "[AgentOutputWritten]" in ctx.context_text # Preserved because it's last-3
    assert len(ctx.pending_work) > 0
    
    print("✅ Gas Town Reconstruction successful (Context + Preservation)")
    
    # 4. Verify sufficient to continue
    next_event = AgentSessionCompleted(
        session_id=session_id, agent_type=AgentType.CREDIT_ANALYSIS,
        application_id="APP-1",
        total_nodes_executed=2, total_llm_calls=1, total_tokens_used=150,
        total_cost_usd=0.01, completed_at=datetime.utcnow(), total_duration_ms=1000,
    )
    # This should not raise PreconditionFailedError or DomainError
    agg.complete_session(next_event)
    assert agg.is_completed is True
