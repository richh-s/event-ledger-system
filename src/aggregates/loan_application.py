from typing import Any
from src.aggregates.base import BaseAggregate
from src.aggregates.credit_record import CreditRecordAggregate
from src.aggregates.fraud_screening import FraudScreeningAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate

from src.models.events import (
    BaseEvent,
    ApplicationState,
    ApplicationSubmitted,
    DocumentUploadRequested,
    DocumentUploaded,
    CreditAnalysisRequested,
    FraudScreeningRequested,
    ComplianceCheckRequested,
    DecisionRequested,
    DecisionGenerated,
    HumanReviewRequested,
    HumanReviewCompleted,
    ApplicationApproved,
    ApplicationDeclined,
    DomainError,
)

class LoanApplicationAggregate(BaseAggregate):
    """
    Orchestrates the lifecycle of a loan application.
    Rule 1: State machine transitions (assert_valid_transition).
    Rule 6: Compliance dependency for approval (verifies no hard block).
    """
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.state: ApplicationState | None = None
        
    def _assert_valid_transition(self, to_state: ApplicationState) -> None:
        valid = False
        if self.state is None:
            if to_state == ApplicationState.SUBMITTED:
                valid = True
        elif self.state == ApplicationState.SUBMITTED:
            if to_state in (ApplicationState.DOCUMENTS_PENDING, ApplicationState.DOCUMENTS_UPLOADED):
                valid = True
        elif self.state in (ApplicationState.DOCUMENTS_PENDING, ApplicationState.DOCUMENTS_UPLOADED):
            if to_state in (ApplicationState.DOCUMENTS_PROCESSED, ApplicationState.DOCUMENTS_UPLOADED):
                valid = True
        elif self.state == ApplicationState.DOCUMENTS_PROCESSED:
            if to_state == ApplicationState.CREDIT_ANALYSIS_REQUESTED:
                valid = True
            elif to_state == ApplicationState.CREDIT_ANALYSIS_COMPLETE:
                valid = True
        elif self.state == ApplicationState.CREDIT_ANALYSIS_REQUESTED:
            if to_state == ApplicationState.CREDIT_ANALYSIS_COMPLETE:
                valid = True
        elif self.state == ApplicationState.CREDIT_ANALYSIS_COMPLETE:
            if to_state == ApplicationState.FRAUD_SCREENING_REQUESTED:
                valid = True
            elif to_state == ApplicationState.FRAUD_SCREENING_COMPLETE:
                valid = True
        elif self.state == ApplicationState.FRAUD_SCREENING_REQUESTED:
            if to_state == ApplicationState.FRAUD_SCREENING_COMPLETE:
                valid = True
        elif self.state == ApplicationState.FRAUD_SCREENING_COMPLETE:
            if to_state in (ApplicationState.COMPLIANCE_CHECK_REQUESTED, ApplicationState.PENDING_DECISION, ApplicationState.APPROVED, ApplicationState.DECLINED, ApplicationState.REFERRED):
                valid = True
        elif self.state == ApplicationState.COMPLIANCE_CHECK_REQUESTED:
            if to_state in (ApplicationState.COMPLIANCE_CHECK_COMPLETE, ApplicationState.DECLINED_COMPLIANCE):
                valid = True
        elif self.state == ApplicationState.COMPLIANCE_CHECK_COMPLETE:
            if to_state in (ApplicationState.PENDING_DECISION, ApplicationState.APPROVED, ApplicationState.DECLINED):
                valid = True
        elif self.state == ApplicationState.PENDING_DECISION:
            if to_state in (ApplicationState.APPROVED, ApplicationState.DECLINED, ApplicationState.PENDING_HUMAN_REVIEW):
                valid = True
        elif self.state == ApplicationState.PENDING_HUMAN_REVIEW:
            if to_state in (ApplicationState.APPROVED, ApplicationState.DECLINED):
                valid = True
        
        # Valid from any state
        if to_state == ApplicationState.DECLINED_COMPLIANCE: 
            valid = True

        if not valid:
            raise DomainError(f"Invalid state transition from {self.state} to {to_state}")

    def apply_ApplicationSubmitted(self, event: ApplicationSubmitted) -> None:
        self.state = ApplicationState.SUBMITTED

    def apply_DocumentUploadRequested(self, event: DocumentUploadRequested) -> None:
        if self.state != ApplicationState.DOCUMENTS_UPLOADED:
            self.state = ApplicationState.DOCUMENTS_PENDING

    def apply_DocumentUploaded(self, event: DocumentUploaded) -> None:
        self.state = ApplicationState.DOCUMENTS_UPLOADED

    def apply_CreditAnalysisRequested(self, event: CreditAnalysisRequested) -> None:
        self.state = ApplicationState.CREDIT_ANALYSIS_REQUESTED

    # ... Other transitions managed implicitly by decisions or direct calls
    def apply_DecisionGenerated(self, event: DecisionGenerated) -> None:
        self.state = ApplicationState.PENDING_DECISION

    def apply_ApplicationApproved(self, event: ApplicationApproved) -> None:
        self.state = ApplicationState.APPROVED

    def apply_ApplicationDeclined(self, event: ApplicationDeclined) -> None:
        self.state = ApplicationState.DECLINED

    def apply_HumanReviewRequested(self, event: HumanReviewRequested) -> None:
        self.state = ApplicationState.PENDING_HUMAN_REVIEW

    def apply_HumanReviewCompleted(self, event: HumanReviewCompleted) -> None:
        if event.final_decision == "APPROVED":
            pass # wait for ApplicationApproved
        elif event.final_decision == "DECLINED":
            pass # wait for ApplicationDeclined

    def get_state(self) -> dict[str, Any]:
        return {
            "state": self.state.value if self.state else None,
            "version": self.version,
        }

    def restore_state(self, state: dict[str, Any]) -> None:
        state_val = state.get("state")
        self.state = ApplicationState(state_val) if state_val else None
        self.version = state.get("version", 0)

    # Command methods
    def submit_application(self, event: ApplicationSubmitted) -> None:
        self._assert_valid_transition(ApplicationState.SUBMITTED)
        self.append_event(event)

    def request_document_upload(self, event: DocumentUploadRequested) -> None:
        self._assert_valid_transition(ApplicationState.DOCUMENTS_PENDING)
        self.append_event(event)
        
    def add_document(self, event: DocumentUploaded) -> None:
        self._assert_valid_transition(ApplicationState.DOCUMENTS_UPLOADED)
        self.append_event(event)

    def generate_decision(self, event: DecisionGenerated) -> None:
        self._assert_valid_transition(ApplicationState.PENDING_DECISION)
        self.append_event(event)

    def assert_no_hard_block(self, compliance_record: ComplianceRecordAggregate) -> None:
        if compliance_record.hard_block_encountered:
            raise DomainError(f"Cannot proceed with application {self.stream_id}. Hard block encountered in compliance.")

    def approve(self, 
                event: ApplicationApproved, 
                credit_record: CreditRecordAggregate, 
                fraud_record: FraudScreeningAggregate, 
                compliance_record: ComplianceRecordAggregate) -> None:
        
        # Cross Aggregate Validations (Rule 6)
        self.assert_no_hard_block(compliance_record)
            
        if not credit_record.has_analysis:
            raise DomainError(f"Cannot approve application {self.stream_id}. Credit analysis is incomplete.")
            
        if not fraud_record.is_completed:
            raise DomainError(f"Cannot approve application {self.stream_id}. Fraud screening is incomplete.")
            
        self._assert_valid_transition(ApplicationState.APPROVED)
        self.append_event(event)

    def complete_human_review(self, event: HumanReviewCompleted) -> None:
        self._assert_valid_transition(ApplicationState.PENDING_HUMAN_REVIEW) # Or any state it targets
        self.append_event(event)

    def decline(self, event: ApplicationDeclined) -> None:
        self._assert_valid_transition(ApplicationState.DECLINED)
        self.append_event(event)
