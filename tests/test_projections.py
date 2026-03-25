import pytest
import asyncio
from datetime import datetime
from uuid import uuid4
from src.database import Database
from src.event_store import EventStore
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.decision_timeline import DecisionTimelineProjection
from src.projections.agent_session_trace import AgentSessionTraceProjection
from src.schema.events import ApplicationSubmitted, LoanPurpose, AgentEvent, StoredEvent

@pytest.fixture
async def db():
    import os
    db = Database(os.getenv("DATABASE_URL"))
    await db.connect()
    yield db
    await db.disconnect()

@pytest.mark.asyncio
async def test_application_summary_projection(db):
    proj = ApplicationSummaryProjection()
    app_id = f"TEST-APP-{uuid4().hex[:4]}"
    
    # 1. Create a submission event
    event = StoredEvent(
        event_id=uuid4(),
        stream_id=f"loan-{app_id}",
        stream_position=1,
        global_position=100,
        event_type="ApplicationSubmitted",
        payload={
            "application_id": app_id,
            "applicant_id": "APPLICANT-001",
            "requested_amount_usd": 50000,
            "loan_purpose": "WORKING_CAPITAL"
        },
        recorded_at=datetime.utcnow()
    )
    
    async with db.transaction() as conn:
        await proj.handle_event(conn, event)
    
    # Verify insert
    async with db.get_connection() as conn:
        row = await conn.fetchrow("SELECT * FROM application_summary WHERE application_id = $1", app_id)
        assert row is not None
        assert row['state'] == "SUBMITTED"
        assert float(row['requested_amount_usd']) == 50000

    # 2. Update with credit analysis
    event_credit = StoredEvent(
        event_id=uuid4(),
        stream_id=f"credit-{app_id}",
        stream_position=1,
        global_position=101,
        event_type="CreditAnalysisCompleted",
        payload={
            "application_id": app_id,
            "decision": {"risk_tier": "LOW", "confidence": 0.95}
        },
        recorded_at=datetime.utcnow()
    )
    
    async with db.transaction() as conn:
        await proj.handle_event(conn, event_credit)
        
    async with db.get_connection() as conn:
        row = await conn.fetchrow("SELECT * FROM application_summary WHERE application_id = $1", app_id)
        assert row['credit_risk_tier'] == "LOW"
        assert row['credit_confidence'] == 0.95

@pytest.mark.asyncio
async def test_decision_timeline_projection(db):
    proj = DecisionTimelineProjection()
    app_id = f"TEST-APP-{uuid4().hex[:4]}"
    
    event = StoredEvent(
        event_id=uuid4(),
        stream_id=f"loan-{app_id}",
        stream_position=1,
        global_position=200,
        event_type="ApplicationSubmitted",
        payload={"application_id": app_id, "applicant_id": "U-1"},
        recorded_at=datetime.utcnow()
    )
    
    async with db.transaction() as conn:
        await proj.handle_event(conn, event)
        
    async with db.get_connection() as conn:
        rows = await conn.fetch("SELECT * FROM decision_timeline WHERE application_id = $1 ORDER BY sequence ASC", app_id)
        assert len(rows) == 1
        assert rows[0]['event_type'] == "ApplicationSubmitted"
        assert rows[0]['sequence'] == 1

@pytest.mark.asyncio
async def test_agent_session_trace_projection(db):
    proj = AgentSessionTraceProjection()
    session_id = f"SESS-{uuid4().hex[:4]}"
    
    # 1. Session Started
    event_start = StoredEvent(
        event_id=uuid4(),
        stream_id=f"agent-credit-{session_id}",
        stream_position=1,
        global_position=300,
        event_type="AgentSessionStarted",
        payload={"session_id": session_id, "agent_type": "credit_analysis", "model_version": "v1"},
        recorded_at=datetime.utcnow()
    )
    
    async with db.transaction() as conn:
        await proj.handle_event(conn, event_start)
    
    # 2. Node Executed
    event_node = StoredEvent(
        event_id=uuid4(),
        stream_id=f"agent-credit-{session_id}",
        stream_position=2,
        global_position=301,
        event_type="AgentNodeExecuted",
        payload={
            "session_id": session_id, 
            "node_name": "validate_inputs", 
            "node_sequence": 1,
            "duration_ms": 100
        },
        recorded_at=datetime.utcnow()
    )
    
    async with db.transaction() as conn:
        await proj.handle_event(conn, event_node)
        
    async with db.get_connection() as conn:
        h = await conn.fetchrow("SELECT * FROM agent_session_header WHERE session_id = $1", session_id)
        assert h is not None
        
        t = await conn.fetch("SELECT * FROM agent_session_trace WHERE session_id = $1", session_id)
        assert len(t) == 1
        assert t[0]['node_name'] == "validate_inputs"
