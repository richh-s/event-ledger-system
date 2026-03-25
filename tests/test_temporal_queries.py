import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from src.database import Database
from src.event_store import EventStore
from src.schema.events import StoredEvent
from src.mcp.resources import get_decision_timeline

@pytest.fixture
async def infra():
    import os
    db = Database(os.getenv("DATABASE_URL"))
    await db.connect()
    store = EventStore(db)
    yield db, store
    await db.disconnect()

@pytest.mark.asyncio
async def test_as_of_temporal_timeline(infra):
    db, store = infra
    app_id = f"TEMP-{uuid4().hex[:4]}"
    
    # 1. First event: Submitted (T-10m)
    t1 = datetime.now(timezone.utc) - timedelta(minutes=10)
    e1 = StoredEvent(
        event_id=uuid4(),
        stream_id=f"loan-{app_id}",
        stream_position=1,
        event_type="ApplicationSubmitted",
        payload={"application_id": app_id, "applicant_id": "T-1"},
        recorded_at=t1
    )
    
    # 2. Second event: Analysis (T-5m)
    t2 = datetime.now(timezone.utc) - timedelta(minutes=5)
    e2 = StoredEvent(
        event_id=uuid4(),
        stream_id=f"loan-{app_id}",
        stream_position=2,
        event_type="CreditAnalysisCompleted",
        payload={"application_id": app_id, "risk": "LOW"},
        recorded_at=t2
    )

    # Directly bypass store logic for precise recorded_at control in test
    async with db.transaction() as conn:
        for e in [e1, e2]:
            await conn.execute(
                """
                INSERT INTO events (event_id, stream_id, stream_position, event_type, payload, metadata, recorded_at)
                VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, $6)
                """,
                e.event_id, e.stream_id, e.stream_position, e.event_type, e.payload, e.recorded_at
            )

    # 3. Request timeline as_of T-7m (should only see first event)
    as_of = (t1 + timedelta(minutes=2)).isoformat()
    timeline = await get_decision_timeline(app_id, as_of=as_of)
    
    # Debug info if fails
    if isinstance(timeline, dict) and "error" in timeline:
        pytest.fail(f"Temporal Query failed: {timeline['error']}")
        
    assert len(timeline) == 1
    assert timeline[0]["event_type"] == "ApplicationSubmitted"

    # 4. Request timeline as_of NOW (should see both)
    timeline_now = await get_decision_timeline(app_id) # Uses projection by default, but test reconstructs from store when as_of is passed
    # Wait, the get_decision_timeline helper uses projection for NO as_of. 
    # Let's use as_of=FUTURE to force store replay logic.
    timeline_all = await get_decision_timeline(app_id, as_of=datetime.utcnow().isoformat())
    assert len(timeline_all) == 2
