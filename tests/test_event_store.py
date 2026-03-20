import pytest
import asyncio

from ledger.event_store import EventStore
from ledger.models import BaseEvent
from ledger.exceptions import OptimisticConcurrencyError, StreamArchivedError


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
    initial_events = [
        BaseEvent(event_type="LoanApplicationStarted", payload={"borrower": "Alice"}),
        BaseEvent(event_type="KYCCheckPassed", payload={"score": 95}),
        BaseEvent(event_type="DocumentUploaded", payload={"doc_type": "W2"}),
    ]
    await store.append(stream_id, initial_events, expected_version=-1, aggregate_type="LoanApplication")
    
    current_v = await store.stream_version(stream_id)
    assert current_v == 3

    # 2. Both agents prepare their CreditAnalysisCompleted event independently
    agent_1_event = BaseEvent(event_type="CreditAnalysisCompleted", payload={"agent_id": "Agent-Alpha", "decision": "Approve"})
    agent_2_event = BaseEvent(event_type="CreditAnalysisCompleted", payload={"agent_id": "Agent-Beta", "decision": "Reject"})

    # 3. Both agents attempt to append at expected_version 3 simultaneously
    async def agent_action(event):
        try:
            return await store.append(stream_id, [event], expected_version=3)
        except Exception as e:
            return e

    results = await asyncio.gather(
        agent_action(agent_1_event),
        agent_action(agent_2_event)
    )

    # 4. Analyze results: Exactly one success, Exactly one OptimisticConcurrencyError
    successes = [r for r in results if isinstance(r, int)]
    errors = [r for r in results if isinstance(r, OptimisticConcurrencyError)]
    
    assert len(successes) == 1, f"Expected 1 success, got {len(successes)}"
    assert len(errors) == 1, f"Expected 1 OptimisticConcurrencyError, got {len(errors)}"
    
    winning_version = successes[0]
    
    # (b) the winning task's event has stream_position=4
    assert winning_version == 4
    
    # (c) the losing task's OptimisticConcurrencyError is explicitly raised with tracking info
    error = errors[0]
    assert error.stream_id == stream_id
    assert error.expected_version == 3
    assert error.actual_version == 4  # It hit the DB after it was already 4

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
    
    await store.append(stream_id, [BaseEvent(event_type="CheckStarted", payload={})], -1, aggregate_type="ComplianceRecord")
    
    # Archive the stream
    await store.archive_stream(stream_id)
    
    # Attempt to write
    with pytest.raises(StreamArchivedError):
        await store.append(stream_id, [BaseEvent(event_type="CheckEnded", payload={})], 1)
        
    # Attempt to read (should succeed)
    events = await store.load_stream(stream_id)
    assert len(events) == 1
