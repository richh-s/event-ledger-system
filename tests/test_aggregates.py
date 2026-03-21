import pytest
from decimal import Decimal
from datetime import datetime
from src.aggregates.loan_application import LoanApplicationAggregate
from src.aggregates.credit_record import CreditRecordAggregate
from src.aggregates.fraud_screening import FraudScreeningAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.models.events import (
    ApplicationSubmitted,
    DocumentUploadRequested,
    DocumentUploaded,
    CreditAnalysisRequested,
    DecisionGenerated,
    ApplicationApproved,
    CreditRecordOpened,
    CreditAnalysisCompleted,
    FraudScreeningInitiated,
    FraudScreeningCompleted,
    ComplianceCheckInitiated,
    ComplianceRuleFailed,
    DomainError,
    LoanPurpose,
    DocumentType,
    DocumentFormat,
    CreditDecision,
    RiskTier,
)

def test_loan_application_transitions():
    app_id = "test-1"
    agg = LoanApplicationAggregate(f"loan-{app_id}")
    
    # 1. Submitted
    agg.submit_application(ApplicationSubmitted(
        application_id=app_id, applicant_id="u-1", requested_amount_usd=Decimal("100"),
        loan_purpose=LoanPurpose.WORKING_CAPITAL, loan_term_months=12, submission_channel="web",
        contact_email="test@test.com", contact_name="Test", submitted_at=datetime.now(),
        application_reference="ref"
    ))
    
    # 2. Upload requested
    agg.request_document_upload(DocumentUploadRequested(
        application_id=app_id, required_document_types=[], deadline=datetime.now(), requested_by="sys"
    ))
    
    # Invalid transition
    with pytest.raises(DomainError):
        agg.generate_decision(DecisionGenerated.model_construct(
            application_id=app_id, orchestrator_session_id="s1", recommendation="APPROVE",
            executive_summary="sum", generated_at=datetime.now(),
            confidence=0.9, event_type="DecisionGenerated", event_version=1
        ))

def test_credit_record_no_duplicates():
    agg = CreditRecordAggregate("credit-test")
    agg.open_record(CreditRecordOpened(
        application_id="test", applicant_id="u-1", opened_at=datetime.now()
    ))
    
    analysis = CreditAnalysisCompleted(
        application_id="test", session_id="s1",
        decision=CreditDecision(risk_tier=RiskTier.LOW, recommended_limit_usd=Decimal("100"), confidence=0.9, rationale="ok"),
        model_version="v1", model_deployment_id="d1", input_data_hash="h", analysis_duration_ms=10, completed_at=datetime.now()
    )
    
    agg.complete_analysis(analysis)
    
    with pytest.raises(DomainError, match="already has a completed analysis"):
        agg.complete_analysis(analysis)

def test_fraud_score_range_validation():
    agg = FraudScreeningAggregate("fraud-test")
    agg.initiate_screening(FraudScreeningInitiated(
        application_id="test", session_id="s1", screening_model_version="v1", initiated_at=datetime.now()
    ))
    
    with pytest.raises(DomainError, match="out of range"):
        # Bypass Pydantic validation to test domain logic validation
        agg.complete_screening(FraudScreeningCompleted.model_construct(
            application_id="test", session_id="s1", fraud_score=1.5, risk_level="HIGH",
            anomalies_found=0, recommendation="check", screening_model_version="v1",
            input_data_hash="h", completed_at=datetime.now(),
            event_type="FraudScreeningCompleted", event_version=1
        ))

def test_compliance_hard_block_stops_evaluation():
    agg = ComplianceRecordAggregate("comp-test")
    agg.initiate_check(ComplianceCheckInitiated(
        application_id="test", session_id="s1", regulation_set_version="v1", rules_to_evaluate=["r1"], initiated_at=datetime.now()
    ))
    
    # Hard block failed
    agg.record_rule_failed(ComplianceRuleFailed(
        application_id="test", session_id="s1", rule_id="r1", rule_name="r1", rule_version="1",
        failure_reason="block", is_hard_block=True, remediation_available=False, evidence_hash="h", evaluated_at=datetime.now()
    ))
    
    # Next rule evaluated -> should fail
    with pytest.raises(DomainError, match="encountered a hard block"):
        agg.record_rule_failed(ComplianceRuleFailed(
            application_id="test", session_id="s1", rule_id="r2", rule_name="r2", rule_version="1",
            failure_reason="fail2", is_hard_block=False, remediation_available=False, evidence_hash="h", evaluated_at=datetime.now()
        ))
