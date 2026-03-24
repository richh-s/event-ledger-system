import pytest
from datetime import datetime
import uuid
from src.aggregates.agent_session import AgentSessionAggregate
from src.models.events import (
    AgentSessionStarted,
    AgentContextLoaded,
    AgentOutputWritten,
    AgentType,
    DomainError,
)

def test_gas_town_rejects_output_without_context():
    # Setup
    stream_id = f"agent-decision_orchestrator-{uuid.uuid4()}"
    aggregate = AgentSessionAggregate(stream_id)
    
    # 1. Start session
    evt_start = AgentSessionStarted(
        session_id="SESS-1", agent_type=AgentType.DECISION_ORCHESTRATOR, agent_id="AG-1",
        application_id="APP-1", model_version="v1", langgraph_graph_version="v1",
        context_source="loan-APP-1", context_token_count=100, started_at=datetime.now()
    )
    aggregate.start_session(evt_start)
    
    # 2. Try to write output without loading context
    evt_output = AgentOutputWritten(
        session_id="SESS-1", agent_type=AgentType.DECISION_ORCHESTRATOR, application_id="APP-1",
        context_event_id="fake-id", events_written=[], output_summary="test", written_at=datetime.now()
    )
    
    with pytest.raises(DomainError) as exc_info:
        aggregate.record_output_written(evt_output)
    
    assert "Gas Town Violation" in str(exc_info.value)
    print("✅ Gas Town successfully rejected output before context load")

def test_gas_town_accepts_output_with_valid_context():
    # Setup
    stream_id = f"agent-decision_orchestrator-{uuid.uuid4()}"
    aggregate = AgentSessionAggregate(stream_id)
    
    # 1. Start session
    evt_start = AgentSessionStarted(
        session_id="SESS-1", agent_type=AgentType.DECISION_ORCHESTRATOR, agent_id="AG-1",
        application_id="APP-1", model_version="v1", langgraph_graph_version="v1",
        context_source="loan-APP-1", context_token_count=100, started_at=datetime.now()
    )
    aggregate.start_session(evt_start)
    
    # 2. Load context
    evt_ctx = AgentContextLoaded(
        session_id="SESS-1", agent_type=AgentType.DECISION_ORCHESTRATOR,
        context_source="loan-APP-1", context_version=0, context_hash="abc", loaded_at=datetime.now()
    )
    aggregate.record_context_loaded(evt_ctx)
    
    # 3. Write output WITH valid context reference
    evt_output = AgentOutputWritten(
        session_id="SESS-1", agent_type=AgentType.DECISION_ORCHESTRATOR, application_id="APP-1",
        context_event_id=str(evt_ctx.event_id), events_written=[], output_summary="test", written_at=datetime.now()
    )
    
    # This should succeed
    aggregate.record_output_written(evt_output)
    assert len(aggregate.uncommitted_events) == 3
    print("✅ Gas Town successfully accepted valid output with context linkage")

def test_gas_town_reconstruction_detects_violation():
    # Setup
    from src.schema.events import StoredEvent
    stream_id = f"agent-decision_orchestrator-{uuid.uuid4()}"
    
    # 1. Create raw events for reconstruction
    evt_start = AgentSessionStarted(
        session_id="SESS-1", agent_type=AgentType.DECISION_ORCHESTRATOR, agent_id="AG-1",
        application_id="APP-1", model_version="v1", langgraph_graph_version="v1",
        context_source="loan-APP-1", context_token_count=100, started_at=datetime.now()
    )
    
    # Invalid output bypassing aggregate
    evt_output = AgentOutputWritten(
        session_id="SESS-1", agent_type=AgentType.DECISION_ORCHESTRATOR, application_id="APP-1",
        context_event_id="fake-id", events_written=[], output_summary="test", written_at=datetime.now()
    )
    
    stored_start = StoredEvent(
        event_id=evt_start.event_id, stream_id=stream_id, stream_position=1,
        event_type=evt_start.event_type, event_version=evt_start.event_version,
        payload=evt_start.to_payload(), recorded_at=datetime.now()
    )
    stored_output = StoredEvent(
        event_id=evt_output.event_id, stream_id=stream_id, stream_position=2,
        event_type=evt_output.event_type, event_version=evt_output.event_version,
        payload=evt_output.to_payload(), recorded_at=datetime.now()
    )
    
    aggregate = AgentSessionAggregate(stream_id)
    aggregate.load_from_history([stored_start, stored_output])
    
    from src.aggregates.base import ReconstructionStatus
    assert aggregate.reconstruction_status == ReconstructionStatus.NEEDS_RECONCILIATION
    assert "Missing context reference: fake-id" in aggregate.reconstruction_issues
    print("✅ Gas Town reconstruction detected broken reference and flagged for reconciliation")

if __name__ == "__main__":
    test_gas_town_rejects_output_without_context()
    test_gas_town_accepts_output_with_valid_context()
    test_gas_town_reconstruction_detects_violation()

