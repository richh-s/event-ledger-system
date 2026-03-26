import pytest
import asyncio

from src.event_store import EventStore
from src.models import BaseEvent
from src.exceptions import OptimisticConcurrencyError, StreamArchivedError

class DummyEvent(BaseEvent):
    event_type: str = "Dummy"
    payload_data: dict = {}

    def to_payload(self) -> dict:
        return self.payload_data


@pytest.mark.asyncio
async def test_double_decision_optimistic_concurrency(db):
    """
    The Double-Decision Test:
    Two AI agents simultaneously attempt to append a CreditAnalysisCompleted event 
    to the same loan application stream. Both read the stream at version 3 and pass 
    expected_version=3 to their append call. exactly one must succeed. 
    The other must receive OptimisticConcurrencyError and retry.
    """
    store = EventStore(db)
    stream_id = "loan-12345"
    
    # 1. Setup: Create stream and push to expected_version = 3
    print("STEP 1")
    initial_events = [
        DummyEvent(event_type="LoanApplicationStarted", payload_data={"borrower": "Alice"}),
        DummyEvent(event_type="KYCCheckPassed", payload_data={"score": 95}),
        DummyEvent(event_type="DocumentUploaded", payload_data={"doc_type": "W2"}),
    ]
    await store.append(stream_id, initial_events, expected_version=-1, aggregate_type="LoanApplication")
    
    current_v = await store.stream_version(stream_id)
    assert current_v == 3

    print("STEP 2")
    # 2. Both agents prepare their CreditAnalysisCompleted event independently
    agent_1_event = DummyEvent(event_type="CreditAnalysisCompleted", payload_data={"agent_id": "Agent-Alpha", "decision": "Approve"})
    agent_2_event = DummyEvent(event_type="CreditAnalysisCompleted", payload_data={"agent_id": "Agent-Beta", "decision": "Reject"})

    print("STEP 3")
    # 3. First agent succeeds
    winning_version = await store.append(stream_id, [agent_1_event], expected_version=3)
    
    print("STEP 4")
    # 4. Second agent fails with OptimisticConcurrencyError because version is now 4
    with pytest.raises(OptimisticConcurrencyError) as exc_info:
        await store.append(stream_id, [agent_2_event], expected_version=3)
        
    error = exc_info.value
    assert error.stream_id == stream_id
    assert error.expected == 3
    assert error.actual == 4

    # (a) total events appended to the stream = 4 (not 5)
    events = await store.load_stream(stream_id)
    assert len(events) == 4
    
    assert events[-1].stream_position == 4
    assert events[-1].event_type == "CreditAnalysisCompleted"


@pytest.mark.asyncio
async def test_archived_stream_blocks_writes(db):
    """Verify that an archived stream accepts reads but definitively blocks appends."""
    store = EventStore(db)
    stream_id = "compliance-999"
    
    await store.append(stream_id, [DummyEvent(event_type="CheckStarted", payload_data={})], -1, aggregate_type="ComplianceRecord")
    
    # Archive the stream
    await store.archive_stream(stream_id)
    
    # Attempt to write
    with pytest.raises(StreamArchivedError):
        await store.append(stream_id, [DummyEvent(event_type="CheckEnded", payload_data={})], 1)
        
    # Attempt to read (should succeed)
    events = await store.load_stream(stream_id)
    assert len(events) == 1
