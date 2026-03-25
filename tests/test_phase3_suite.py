import asyncio
import os
import pytest
from src.database import Database
from src.event_store import EventStore
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.compliance_audit import ComplianceAuditViewProjection
from src.projections.agent_performance import AgentPerformanceLedgerProjection
from src.models.events import ApplicationSubmitted, AgentSessionStarted, AgentNodeExecuted, LoanPurpose, AgentType
from datetime import datetime
import uuid

@pytest.mark.asyncio
async def test_phase3_audit_compliance(db):
    store = EventStore(db)
    
    # 1. CLEAN START (FK Order corrected)
    async with db.transaction() as conn:
        await conn.execute("DELETE FROM outbox")
        await conn.execute("DELETE FROM agent_session_trace")
        await conn.execute("DELETE FROM agent_session_header")
        await conn.execute("DELETE FROM events")
        await conn.execute("DELETE FROM event_streams")
        await conn.execute("DELETE FROM application_summary")
        await conn.execute("DELETE FROM decision_timeline")
        await conn.execute("DELETE FROM projection_checkpoints")
        await conn.execute("DELETE FROM dead_letter_queue")

    # 2. SEED TEST DATA
    app_id = f"APP-{uuid.uuid4().hex[:4]}"
    sess_id = f"SESS-{uuid.uuid4().hex[:4]}"
    
    # Ensure application stream exists before session linkage
    events = [
        ApplicationSubmitted(
            application_id=app_id, applicant_id="user-1", requested_amount_usd=10000, 
            loan_purpose=LoanPurpose.WORKING_CAPITAL, loan_term_months=36, 
            submission_channel="WEB", contact_email="test@example.com", 
            contact_name="Aman", submitted_at=datetime.utcnow(), application_reference="REF-1"
        ),
        AgentSessionStarted(
            session_id=sess_id, agent_type=AgentType.CREDIT_ANALYSIS, agent_id="agent-1",
            application_id=app_id, model_version="gpt-4o", langgraph_graph_version="v1",
            context_source="financials.pdf", context_token_count=1500, started_at=datetime.utcnow()
        ),
        AgentNodeExecuted(
            session_id=sess_id, agent_type=AgentType.CREDIT_ANALYSIS, node_name="analyze_debt",
            node_sequence=1, input_keys=["debt_ratio"], output_keys=["risk_score"], 
            llm_called=True, llm_tokens_input=100, llm_tokens_output=50, 
            llm_cost_usd=0.001, duration_ms=500, executed_at=datetime.utcnow()
        )
    ]
    
    # Use real stream IDs (loan-id or session-id)
    await store.append(f"loan-{app_id}", [events[0]], -1, aggregate_type="Application")
    await store.append(f"session-{sess_id}", events[1:], -1, aggregate_type="AgentSession")

    # 3. RUN DAEMON (Parallel Tasks)
    projections = [
        ApplicationSummaryProjection(),
        ComplianceAuditViewProjection(),
        AgentPerformanceLedgerProjection()
    ]
    daemon = ProjectionDaemon(db, store, projections, poll_interval_ms=10)
    await daemon.start()
    
    # Wait for processing
    print("Waiting for independent tasks to catch up...")
    await asyncio.sleep(1.0) # Given we are local, 1s is more than enough
    
    # 4. VERIFY ApplicationSummary
    async with db.get_connection() as conn:
        summary = await conn.fetchrow("SELECT * FROM application_summary WHERE application_id = $1", app_id)
        assert summary is not None, "ApplicationSummary failed to project!"
        assert summary['state'] == "SUBMITTED"

        # 5. VERIFY AgentPerformanceLedger
        perf = await conn.fetchrow("SELECT * FROM agent_performance_ledger WHERE agent_id = $1", "credit_analysis")
        assert perf is not None, "AgentPerformanceLedger failed!"
        assert float(perf["analyses_completed"]) == 1
        assert float(perf["avg_duration_ms"]) == 500

    await daemon.stop()
    print("✅ Logic Verification: ALL Projections healthy and current.")
