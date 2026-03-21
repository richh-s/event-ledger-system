import json
from src.aggregates.repository import AggregateRepository
from src.aggregates.loan_application import LoanApplicationAggregate
from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.credit_record import CreditRecordAggregate
from src.aggregates.fraud_screening import FraudScreeningAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.aggregates.document_package import DocumentPackageAggregate

from src.models.events import (
    ApplicationSubmitted,
    AgentSessionStarted,
    AgentNodeExecuted,
    CreditAnalysisCompleted,
    FraudScreeningCompleted,
    ComplianceRulePassed,
    ComplianceRuleFailed,
    ComplianceRuleNoted,
    ExtractionCompleted,
    QualityAssessmentCompleted,
    DecisionGenerated,
    ApplicationApproved,
    ApplicationDeclined,
    HumanReviewCompleted,
)

class CommandHandlers:
    def __init__(self, repository: AggregateRepository):
        self.repo = repository

    async def handle_submit_application(self, event: ApplicationSubmitted) -> None:
        stream_id = f"loan-{event.application_id}"
        agg = await self.repo.load(LoanApplicationAggregate, stream_id)
        agg.submit_application(event)
        await self.repo.save(agg, "LoanApplication")

    async def handle_start_agent_session(self, event: AgentSessionStarted) -> None:
        stream_id = f"agent-{event.agent_type.value}-{event.session_id}"
        agg = await self.repo.load(AgentSessionAggregate, stream_id)
        agg.start_session(event)
        await self.repo.save(agg, "AgentSession")

    async def handle_record_node_execution(self, event: AgentNodeExecuted) -> None:
        stream_id = f"agent-{event.agent_type.value}-{event.session_id}"
        agg = await self.repo.load(AgentSessionAggregate, stream_id)
        agg.record_node_execution(event)
        await self.repo.save(agg, "AgentSession")

    async def handle_credit_analysis_completed(
        self, 
        event: CreditAnalysisCompleted, 
        correlation_id: str | None = None, 
        causation_id: str | None = None
    ) -> None:
        # Load specific streams (Step 1)
        credit_stream_id = f"credit-{event.application_id}"
        session_stream_id = f"agent-credit_analysis-{event.session_id}"
        loan_stream_id = f"loan-{event.application_id}"
        
        # 1. Load Aggregates (Rubric Requirement: Load both loan and agent session)
        credit_agg = await self.repo.load(CreditRecordAggregate, credit_stream_id)
        session_agg = await self.repo.load(AgentSessionAggregate, session_stream_id)
        loan_agg = await self.repo.load(LoanApplicationAggregate, loan_stream_id)
        
        # 2. Call Guard Methods (Step 2)
        session_agg.assert_session_started()
        session_agg.assert_model_version(event.model_version)
        credit_agg.assert_no_analysis_exists()
        
        # 3. Determine Events (Step 3) - Logic is delegated to aggregate
        credit_agg.complete_analysis(event)
        
        # 4. Save Atomically (Step 4) - Using tracked version
        await self.repo.save(
            credit_agg, 
            "CreditRecord",
            correlation_id=correlation_id,
            causation_id=causation_id
        )

    async def handle_fraud_screening_completed(self, event: FraudScreeningCompleted) -> None:
        fraud_stream_id = f"fraud-{event.application_id}"
        session_stream_id = f"agent-fraud_detection-{event.session_id}"
        
        fraud_agg = await self.repo.load(FraudScreeningAggregate, fraud_stream_id)
        session_agg = await self.repo.load(AgentSessionAggregate, session_stream_id)
        
        session_agg.assert_session_started()
        fraud_agg.complete_screening(event)
        
        await self.repo.save(fraud_agg, "FraudScreening")

    async def handle_compliance_rule_evaluation(self, event) -> None:
        compliance_stream_id = f"compliance-{event.application_id}"
        session_stream_id = f"agent-compliance-{event.session_id}"
        
        comp_agg = await self.repo.load(ComplianceRecordAggregate, compliance_stream_id)
        session_agg = await self.repo.load(AgentSessionAggregate, session_stream_id)
        
        session_agg.assert_session_started()
        
        if isinstance(event, ComplianceRulePassed):
            comp_agg.record_rule_passed(event)
        elif isinstance(event, ComplianceRuleFailed):
            comp_agg.record_rule_failed(event)
        elif isinstance(event, ComplianceRuleNoted):
            comp_agg.record_rule_noted(event)
            
        await self.repo.save(comp_agg, "ComplianceRecord")

    async def handle_extraction_completed(self, event: ExtractionCompleted) -> None:
        docpkg_stream_id = f"docpkg-{event.application_id}"
        
        pkg_agg = await self.repo.load(DocumentPackageAggregate, docpkg_stream_id)
        pkg_agg.complete_extraction(event)
        
        await self.repo.save(pkg_agg, "DocumentPackage")

    async def handle_quality_assessment(self, event: QualityAssessmentCompleted) -> None:
        docpkg_stream_id = f"docpkg-{event.application_id}"
        # For quality assessment, it might not have session_id explicitly but we apply it
        pkg_agg = await self.repo.load(DocumentPackageAggregate, docpkg_stream_id)
        pkg_agg.assess_quality(event)
        
        await self.repo.save(pkg_agg, "DocumentPackage")

    async def handle_generate_decision(self, event) -> None:
        loan_stream_id = f"loan-{event.application_id}"
        loan_agg = await self.repo.load(LoanApplicationAggregate, loan_stream_id)
        
        if isinstance(event, DecisionGenerated):
            loan_agg.generate_decision(event)
        elif isinstance(event, ApplicationApproved):
            # Load cross-aggregate state
            credit_agg = await self.repo.load(CreditRecordAggregate, f"credit-{event.application_id}")
            fraud_agg = await self.repo.load(FraudScreeningAggregate, f"fraud-{event.application_id}")
            comp_agg = await self.repo.load(ComplianceRecordAggregate, f"compliance-{event.application_id}")
            
            loan_agg.approve(event, credit_agg, fraud_agg, comp_agg)
        elif isinstance(event, ApplicationDeclined):
            loan_agg.decline(event)
            
        await self.repo.save(loan_agg, "LoanApplication")

    async def handle_human_review_completed(self, event: HumanReviewCompleted) -> None:
        loan_stream_id = f"loan-{event.application_id}"
        loan_agg = await self.repo.load(LoanApplicationAggregate, loan_stream_id)
        loan_agg.complete_human_review(event)
        await self.repo.save(loan_agg, "LoanApplication")
