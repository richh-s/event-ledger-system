from __future__ import annotations
from datetime import datetime, timezone
from uuid import uuid4
from typing import Optional, List, Dict, Any

import traceback

from src.database import get_db, Database
from src.event_store import EventStore
from src.aggregates.repository import AggregateRepository
from src.aggregates.loan_application import LoanApplicationAggregate
from src.aggregates.agent_session import AgentSessionAggregate
from src.models.events import (
    ApplicationSubmitted, CreditAnalysisCompleted, FraudScreeningCompleted,
    ComplianceCheckCompleted, DecisionGenerated, HumanReviewCompleted,
    AgentSessionStarted, AgentType, LoanPurpose, CreditDecision, RiskTier,
    ComplianceVerdict, LedgerError
)
from src.integrity.audit_chain import AuditChainVerifier

def handle_mcp_errors(func):
    """Decorator to catch exceptions and return them as typed JSON strings for LLMs."""
    import functools
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            error_obj = {
                "error_type": e.__class__.__name__,
                "message": str(e),
                "context": getattr(e, "context_info", {"traceback": traceback.format_exc()}),
                "suggested_action": getattr(e, "suggested_action", "review_inputs_and_retry")
            }
            import json
            return json.dumps({"error": error_obj})
    return wrapper

async def get_ops():
    db = get_db()
    if not db._pool:
        await db.connect()
    store = EventStore(db)
    repo = AggregateRepository(store, db)
    return db, store, repo

@handle_mcp_errors
async def submit_application(
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float,
    loan_purpose: str,
    loan_term_months: int = 12
) -> str:
    db, store, repo = await get_ops()
    event = ApplicationSubmitted(
        application_id=application_id,
        applicant_id=applicant_id,
        requested_amount_usd=requested_amount_usd,
        loan_purpose=LoanPurpose(loan_purpose),
        loan_term_months=loan_term_months,
        submission_channel="MCP",
        contact_name="James Smith",
        contact_email="james@example.com",
        submitted_at=datetime.now(timezone.utc),
        application_reference=f"REF-{application_id}"
    )
    agg = await repo.load(LoanApplicationAggregate, f"loan-{application_id}")
    agg.submit_application(event)
    await repo.save(agg, "LoanApplication", correlation_id=application_id)
    return f"Application {application_id} submitted."

@handle_mcp_errors
async def record_credit_analysis(
    session_id: str,
    application_id: str,
    recommendation: str,
    confidence: float,
    risk_tier: str,
    recommended_limit_usd: float,
    rationale: str
) -> str:
    """
    record_credit_analysis: Record the results of a credit analysis agent session.
    Preconditions for LLM:
    - The application_id must exist in the ledger.
    - The session_id must match the active or recently completed credit_analysis agent session.
    - You must evaluate the risk_tier as 'LOW', 'MEDIUM', or 'HIGH'.
    """
    db, store, repo = await get_ops()
    decision = CreditDecision(
        recommendation=recommendation,
        confidence=confidence,
        risk_tier=RiskTier(risk_tier),
        recommended_limit_usd=recommended_limit_usd,
        rationale=rationale
    )
    event = CreditAnalysisCompleted(
        session_id=session_id,
        application_id=application_id,
        decision=decision,
        model_version="gpt-4",
        model_deployment_id="dep-123",
        input_data_hash="sha256:abc",
        analysis_duration_ms=1500,
        completed_at=datetime.utcnow()
    )
    await store.append(f"credit-{application_id}", [event], expected_version=-1, aggregate_type="CreditRecord")
    
    # Also update the main LoanApplicationAggregate
    agg = await repo.load(LoanApplicationAggregate, f"loan-{application_id}")
    agg.record_credit_analysis(event)
    await repo.save(agg, "LoanApplication", correlation_id=application_id)

    return f"Credit analysis recorded for {application_id}."

@handle_mcp_errors
async def record_fraud_screening(
    session_id: str,
    application_id: str,
    fraud_score: float,
    anomalies_found: int,
    verification_notes: str
) -> str:
    """
    record_fraud_screening: Record the results of a fraud detection agent session.
    Preconditions for LLM:
    - The application_id must exist in the ledger.
    - The session_id must match the fraud_detection agent session.
    - You must summarize the anomalies_found as an integer count.
    """
    db, store, repo = await get_ops()
    event = FraudScreeningCompleted(
        session_id=session_id,
        application_id=application_id,
        fraud_score=fraud_score,
        risk_level="LOW",
        anomalies_found=anomalies_found,
        recommendation="PASS",
        screening_model_version="deepseek-v3.1",
        input_data_hash="sha256:abc",
        completed_at=datetime.utcnow()
    )
    await store.append(f"fraud-{application_id}", [event], expected_version=-1, aggregate_type="FraudScreening")

    # Also update the main LoanApplicationAggregate
    agg = await repo.load(LoanApplicationAggregate, f"loan-{application_id}")
    agg.record_fraud_screening(event)
    await repo.save(agg, "LoanApplication", correlation_id=application_id)

    return f"Fraud screening recorded for {application_id}."

@handle_mcp_errors
async def record_compliance_check(
    session_id: str,
    application_id: str,
    overall_verdict: str,
    rules_evaluated: int,
    fail_count: int,
    hard_block: bool,
    details: str
) -> str:
    """
    record_compliance_check: Record the results of a compliance agent session.
    Preconditions for LLM:
    - The application_id must exist in the ledger.
    - The session_id must match the compliance agent session.
    - You must evaluate the overall_verdict as 'CLEAR', 'BLOCKED', or 'CONDITIONAL'.
    - You must provide the exact count of rules_evaluated and fail_count.
    """
    db, store, repo = await get_ops()
    event = ComplianceCheckCompleted(
        session_id=session_id,
        application_id=application_id,
        rules_evaluated=rules_evaluated,
        rules_passed=rules_evaluated - fail_count,
        rules_failed=fail_count,
        rules_noted=0,
        has_hard_block=hard_block,
        overall_verdict=overall_verdict, # Pydantic will validate, but pass string
        regulation_version="2026-Q1",
        completed_at=datetime.now(timezone.utc)
    )
    await store.append(f"compliance-{application_id}", [event], expected_version=-1, aggregate_type="ComplianceRecord")

    # Also update the main LoanApplicationAggregate
    agg = await repo.load(LoanApplicationAggregate, f"loan-{application_id}")
    agg.record_compliance_check(event)
    await repo.save(agg, "LoanApplication", correlation_id=application_id)

    return f"Compliance check recorded for {application_id}."

@handle_mcp_errors
async def generate_decision(
    session_id: str,
    application_id: str,
    recommendation: str,
    confidence: float,
    rationale: str,
    key_concerns: Optional[str] = None,
    contributing_sessions: Optional[List[str]] = None,
    model_versions: Optional[Dict[str, str]] = None
) -> str:
    """
    generate_decision: Record the final recommendation from the decision orchestrator.
    Preconditions for LLM:
    - The application_id must exist in the ledger.
    - The session_id must match the decision_orchestrator agent session.
    - All prerequisite agent analyses (credit, fraud, compliance) must be fully completed.
    - recommendation must be explicitly stated (e.g., 'APPROVE', 'DECLINE').
    """
    db, store, repo = await get_ops()
    event = DecisionGenerated(
        application_id=application_id,
        orchestrator_session_id=session_id,
        recommendation=recommendation,
        confidence=confidence,
        executive_summary=rationale,
        key_risks=[key_concerns] if key_concerns else [],
        contributing_sessions=contributing_sessions or [],
        model_versions=model_versions or {"orchestrator": "decision-v1"},
        generated_at=datetime.now(timezone.utc)
    )
    agg = await repo.load(LoanApplicationAggregate, f"loan-{application_id}")
    agg.generate_decision(event)
    await repo.save(agg, "LoanApplication", correlation_id=application_id)
    return f"Decision generated for {application_id}."

@handle_mcp_errors
async def record_human_review(
    application_id: str,
    reviewer_id: str,
    decision: str,
    notes: str
) -> str:
    """
    record_human_review: Record the manual human review verdict.
    Preconditions for LLM:
    - The application_id must be in a 'PENDING_HUMAN_REVIEW' state or require an override.
    - decision must be a conclusive string (e.g., 'APPROVED', 'DECLINED').
    - You must have an established reviewer_id interacting with the system.
    """
    db, store, repo = await get_ops()
    event = HumanReviewCompleted(
        application_id=application_id,
        reviewer_id=reviewer_id,
        override=(decision == "APPROVED"),
        original_recommendation="REFER", 
        final_decision=decision,
        override_reason=notes if decision == "APPROVED" else None,
        reviewed_at=datetime.now(timezone.utc)
    )
    agg = await repo.load(LoanApplicationAggregate, f"loan-{application_id}")
    agg.complete_human_review(event)
    await repo.save(agg, "LoanApplication", correlation_id=application_id)
    return f"Human review recorded for {application_id}."

@handle_mcp_errors
async def start_agent_session(
    agent_type: str,
    model_version: str,
    context_source: str,
    application_id: str,
    causation_id: Optional[str] = None
) -> str:
    """
    start_agent_session: Initialize a new agent session with Gas Town enforcement.
    Preconditions for LLM:
    - You must know the predefined agent_type (e.g., 'credit_analysis', 'fraud_detection').
    - The application_id must explicitly exist and be referenced.
    - The context_source must indicate where the context is fetched from (e.g., 's3', 'db').
    """
    db, store, repo = await get_ops()
    session_id = str(uuid4())
    event = AgentSessionStarted(
        session_id=session_id,
        agent_type=AgentType(agent_type),
        agent_id=f"agent-{agent_type}-mcp",
        application_id=application_id,
        model_version=model_version,
        langgraph_graph_version="1.0",
        context_source=context_source,
        context_token_count=100,
        started_at=datetime.utcnow()
    )
    agg = await repo.load(AgentSessionAggregate, f"session-{session_id}")
    agg.start_session(event)
    await repo.save(agg, "AgentSession", correlation_id=f"corr-{application_id}", causation_id=causation_id)
    return session_id

@handle_mcp_errors
async def run_integrity_check(stream_id: str) -> Dict[str, Any]:
    """
    run_integrity_check: Perform SHA-256 hash chain verification for a given stream.
    Preconditions for LLM:
    - The stream_id must be a valid, existing stream in the ledger (e.g., 'loan-APP-123').
    - You should not call this until there is an established chain of events on the stream to verify.
    """
    db, store, repo = await get_ops()
    verifier = AuditChainVerifier(db, store)
    return await verifier.verify_stream_integrity(stream_id)
