import pytest
import asyncio
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from src.database import Database
from src.event_store import EventStore
from src.models.events import StoredEvent
from src.mcp.resources import get_compliance_view

@pytest.mark.asyncio
async def test_as_of_temporal_timeline(db):
    app_id = f"TEMP-{uuid4().hex[:4]}"
    
    # 1. First event: Rule 1 passed (T-10m)
    t1 = datetime.now(timezone.utc) - timedelta(minutes=10)
    e1 = StoredEvent(
        event_id=uuid4(),
        stream_id=f"compliance-{app_id}",
        stream_position=1,
        event_type="ComplianceRulePassed",
        payload={"application_id": app_id, "rule_id": "rule-1", "rule_version": "v1.0"},
        recorded_at=t1
    )
    
    # 2. Second event: Rule 1 failed (T-5m)
    t2 = datetime.now(timezone.utc) - timedelta(minutes=5)
    e2 = StoredEvent(
        event_id=uuid4(),
        stream_id=f"compliance-{app_id}",
        stream_position=2,
        event_type="ComplianceRuleFailed",
        payload={"application_id": app_id, "rule_id": "rule-1", "rule_version": "v1.0", "is_hard_block": True},
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
                e.event_id, e.stream_id, e.stream_position, e.event_type, json.dumps(e.payload), e.recorded_at
            )

    # Provide projection of the events so temporal query can look them up
    from src.projections.compliance_audit import ComplianceAuditViewProjection
    proj = ComplianceAuditViewProjection()
    # Need to give global positions to the events
    async with db.get_connection() as conn:
        rows = await conn.fetch("SELECT global_position FROM events WHERE stream_id = $1 ORDER BY stream_position ASC", f"compliance-{app_id}")
        e1.global_position = rows[0]['global_position']
        e2.global_position = rows[1]['global_position']
        
        async with db.transaction() as tx:
            await proj.handle_event(tx, e1)
            await proj.handle_event(tx, e2)

    # 3. Request timeline as_of T-7m (should see PASS)
    as_of = (t1 + timedelta(minutes=2)).isoformat()
    timeline_past = await get_compliance_view(app_id, timestamp=as_of)
    
    assert timeline_past["has_hard_block"] is False
    assert len(timeline_past["rules"]) == 1
    assert timeline_past["rules"][0]["result"] == "PASS"

    # 4. Request timeline as_of NOW (should see FAIL)
    timeline_now = await get_compliance_view(app_id, timestamp=datetime.now(timezone.utc).isoformat())
    
    assert timeline_now["has_hard_block"] is True
    assert len(timeline_now["rules"]) == 1
    assert timeline_now["rules"][0]["result"] == "FAIL"
