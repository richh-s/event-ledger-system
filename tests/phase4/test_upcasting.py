import asyncio
import uuid
from datetime import datetime
from src.schema.events import StoredEvent
from src.upcasting.registry import registry
from src.upcasting.upcasters import initialize_upcasters

# Ensure upcasters are registered
initialize_upcasters()

def test_upcast_credit_analysis():
    # Legacy v1 event (missing fields)
    v1_payload = {
        "application_id": "APP-123",
        "session_id": "SESS-456",
        "decision": {"recommendation": "APPROVE"},
        "model_deployment_id": "dep-001",
        "input_data_hash": "hash-abc",
        "analysis_duration_ms": 100,
        "completed_at": "2024-01-01T00:00:00Z"
    }
    
    event = StoredEvent(
        event_id=uuid.uuid4(),
        stream_id="credit-APP-123",
        stream_position=1,
        event_type="CreditAnalysisCompleted",
        event_version=1,
        payload=v1_payload,
        metadata={"version": 1},
        recorded_at=datetime.now()
    )
    
    upcasted = registry.upcast(event)
    
    assert upcasted.event_version == 2
    assert upcasted.payload["model_version"] == "unknown"
    assert upcasted.payload["confidence_score"] is None
    assert "regulatory_basis" in upcasted.payload
    assert upcasted.metadata["version"] == 2
    print("✅ CreditAnalysisCompleted upcast successful")

def test_upcast_decision_generated():
    v1_payload = {
        "application_id": "APP-123",
        "orchestrator_session_id": "SESS-789",
        "recommendation": "APPROVE",
        "approved_amount_usd": 10000,
        "executive_summary": "Looks good",
        "generated_at": "2024-01-01T00:00:00Z"
    }
    
    event = StoredEvent(
        event_id=uuid.uuid4(),
        stream_id="loan-APP-123",
        stream_position=50,
        event_type="DecisionGenerated",
        event_version=1,
        payload=v1_payload,
        metadata={"version": 1},
        recorded_at=datetime.now()
    )
    
    upcasted = registry.upcast(event)
    
    assert upcasted.event_version == 2
    assert upcasted.payload["confidence_score"] == 0.0
    assert upcasted.payload["contributing_agent_sessions"] == []
    assert upcasted.metadata["version"] == 2
    print("✅ DecisionGenerated upcast successful")

if __name__ == "__main__":
    test_upcast_credit_analysis()
    test_upcast_decision_generated()
