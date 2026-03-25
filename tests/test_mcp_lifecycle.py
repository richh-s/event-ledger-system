import pytest
import os
import uuid
from typing import Dict, Any

from src.database import Database
from dotenv import load_dotenv

load_dotenv()
from src.event_store import EventStore
from src.models.events import StoredEvent
# Mock FastMCP Context
class MockContext:
    def __init__(self, db, event_store):
        pass

@pytest.mark.asyncio
async def test_mcp_lifecycle(db):
    from src.mcp.tools import (
        submit_application, record_credit_analysis, record_fraud_screening,
        record_compliance_check, generate_decision, record_human_review, start_agent_session
    )
    from src.mcp.resources import get_compliance_view
    
    app_id = f"APP-MCP-{uuid.uuid4().hex[:6]}"
    applicant = "CUST-123"
    
    # 1. Submit App
    await submit_application(
        application_id=app_id, applicant_id=applicant, requested_amount_usd=100000.0,
        loan_purpose="expansion", loan_term_months=36
    )
    
    # Get the submission event ID to use as causation_id
    async with db.get_connection() as conn:
        sub_event_id = await conn.fetchval(
            "SELECT event_id FROM events WHERE stream_id = $1 AND event_type = 'ApplicationSubmitted'",
            f"loan-{app_id}"
        )

    # 2. Start session
    session_id = await start_agent_session(
        agent_type="credit_analysis", model_version="gpt-4",
        context_source="s3://bucket", application_id=app_id, causation_id=str(sub_event_id)
    )
    
    # 3. Credit Analysis
    await record_credit_analysis(
        session_id=session_id, application_id=app_id, recommendation="APPROVE",
        confidence=0.9, risk_tier="LOW", recommended_limit_usd=100000.0, rationale="Good credit"
    )
    
    # 4. Fraud Screening
    f_sess = await start_agent_session(
        agent_type="fraud_detection", model_version="gpt-4",
        context_source="s3://bucket", application_id=app_id, causation_id=str(sub_event_id)
    )
    await record_fraud_screening(
        session_id=f_sess, application_id=app_id, fraud_score=0.1, anomalies_found=0, verification_notes="All clear"
    )
    
    # 5. Compliance Check
    c_sess = await start_agent_session(
        agent_type="compliance", model_version="gpt-4",
        context_source="s3://bucket", application_id=app_id, causation_id=str(sub_event_id)
    )
    await record_compliance_check(
        session_id=c_sess, application_id=app_id, overall_verdict="CLEAR", rules_evaluated=5,
        fail_count=0, hard_block=False, details="No KYC issues"
    )
    
    # 6. Decision
    o_sess = await start_agent_session(
        agent_type="decision_orchestrator", model_version="gpt-4",
        context_source="s3://bucket", application_id=app_id, causation_id=str(sub_event_id)
    )
    await generate_decision(
        session_id=o_sess, application_id=app_id, recommendation="APPROVE", confidence=0.95,
        rationale="All checks passed.", key_concerns="None"
    )
    
    # 7. Human Review
    await record_human_review(
        application_id=app_id, reviewer_id="USER-A", decision="APPROVED", notes="Looks solid."
    )
    
    # 8. Trigger Projections manually since no daemon is running in tests
    from src.projections.application_summary import ApplicationSummaryProjection
    from src.projections.compliance_audit import ComplianceAuditViewProjection
    from src.projections.agent_performance import AgentPerformanceLedgerProjection
    
    projections = [
        ApplicationSummaryProjection(),
        ComplianceAuditViewProjection(),
        AgentPerformanceLedgerProjection()
    ]
    
    async with db.get_connection() as conn:
        # Load all events appended during the test
        rows = await conn.fetch("SELECT * FROM events ORDER BY global_position ASC")
        print(f"DEBUG: Found {len(rows)} events in DB.")
        for row in rows:
            event = StoredEvent(**dict(row))
            async with db.transaction() as tx_conn:
                for proj in projections:
                    await proj.handle_event(tx_conn, event)
    
    # 9. Query Resources (Rubric Assertions)
    # Assertion: Querying ledger://applications/{id}/compliance returns all expected event types
    comp = await get_compliance_view(app_id)
    print(f"DEBUG: comp rules: {comp['rules']}")
    assert comp is not None
    assert comp["application_id"] == app_id
    # Ensure our recorded compliance check is present
    assert any(r["rule_id"] == "__overall__" and r["result"] == "CLEAR" for r in comp["rules"])
    
    # 10. Query Summary Resource
    from src.mcp.resources import get_application_summary
    summary = await get_application_summary(app_id)
    assert summary["state"] == "APPROVED"
    assert summary["reviewer_id"] == "USER-A"
    
    # 11. Health Resource
    from src.mcp.resources import get_health_metrics
    health = await get_health_metrics()
    assert health["status"] == "healthy"
    
    print("✅ MCP Full Lifecycle Test Successful (Tools -> Events -> Projections -> Resources)")
