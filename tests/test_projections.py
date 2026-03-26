import pytest
import asyncio
from datetime import datetime, timezone
from uuid import uuid4
from src.database import Database
from src.event_store import EventStore
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.compliance_audit import ComplianceAuditViewProjection
from src.projections.agent_performance import AgentPerformanceLedgerProjection
from src.models.events import ApplicationSubmitted, LoanPurpose, AgentEvent, StoredEvent

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
        recorded_at=datetime.now(timezone.utc)
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
        recorded_at=datetime.now(timezone.utc)
    )
    
    async with db.transaction() as conn:
        await proj.handle_event(conn, event_credit)
        
    async with db.get_connection() as conn:
        row = await conn.fetchrow("SELECT * FROM application_summary WHERE application_id = $1", app_id)
        assert row is not None
        # Align with Phase 3 schema column 'risk_tier'
        assert row['risk_tier'] == "LOW"

@pytest.mark.asyncio
async def test_compliance_audit_view_projection(db):
    proj = ComplianceAuditViewProjection()
    app_id = f"TEST-APP-{uuid4().hex[:4]}"
    
    event = StoredEvent(
        event_id=uuid4(),
        stream_id=f"compliance-{app_id}",
        stream_position=1,
        global_position=200,
        event_type="ComplianceRulePassed",
        payload={"application_id": app_id, "rule_id": "r1", "rule_version": "v1.0"},
        recorded_at=datetime.now(timezone.utc)
    )
    
    async with db.transaction() as conn:
        await proj.handle_event(conn, event)
        
    async with db.get_connection() as conn:
        rows = await conn.fetch("SELECT * FROM compliance_audit_view WHERE application_id = $1", app_id)
        assert len(rows) == 1
        assert rows[0]['result'] == "PASS"

@pytest.mark.asyncio
async def test_agent_performance_ledger_projection(db):
    proj = AgentPerformanceLedgerProjection()
    session_id = f"SESS-{uuid4().hex[:4]}"
    
    event_node = StoredEvent(
        event_id=uuid4(),
        stream_id=f"agent-credit-{session_id}",
        stream_position=2,
        global_position=301,
        event_type="AgentNodeExecuted",
        payload={
            "session_id": session_id,
            "agent_type": "credit_analysis",
            "model_version": "v1",
            "node_name": "validate_inputs", 
            "node_sequence": 1,
            "duration_ms": 100,
            "confidence_score": 0.8
        },
        recorded_at=datetime.now(timezone.utc)
    )
    
    async with db.transaction() as conn:
        await proj.handle_event(conn, event_node)
        
    async with db.get_connection() as conn:
        rows = await conn.fetch("SELECT * FROM agent_performance_ledger WHERE agent_id = $1", "credit_analysis")
        assert len(rows) == 1
        assert float(rows[0]['analyses_completed']) == 1
        assert float(rows[0]['avg_duration_ms']) == 100

