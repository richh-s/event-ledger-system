import asyncio
import os
import uuid
import sys
from decimal import Decimal
from datetime import datetime

# --- LOAD .ENV NATIVELY ---
def load_env():
    env_path = os.path.join(os.getcwd(), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value

load_env()
# -------------------------

# Add current dir to sys.path
sys.path.append(os.getcwd())

from src.database import Database
from src.event_store import EventStore
from src.aggregates.repository import AggregateRepository

# Import all 7 aggregates
from src.aggregates.loan_application import LoanApplicationAggregate
from src.aggregates.document_package import DocumentPackageAggregate
from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.credit_record import CreditRecordAggregate
from src.aggregates.fraud_screening import FraudScreeningAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.aggregates.audit_ledger import AuditLedgerAggregate

# Import event models and enums
from src.models.events import (
    ApplicationSubmitted,
    LoanPurpose,
    PackageCreated,
    DocumentType,
    DocumentUploaded,
    DocumentUploadRequested,
    DocumentFormat,
    AgentSessionStarted,
    AgentType,
    AgentNodeExecuted,
    CreditRecordOpened,
    CreditDecision,
    RiskTier,
    CreditAnalysisRequested,
    CreditAnalysisCompleted,
    FraudScreeningRequested,
    FraudScreeningInitiated,
    FraudScreeningCompleted,
    ComplianceCheckRequested,
    ComplianceCheckInitiated,
    ComplianceRulePassed,
    ComplianceCheckCompleted,
    ComplianceVerdict,
    ApplicationApproved,
    AuditIntegrityCheckRun
)

async def test_phase2_workflow():
    # --- 1. Setup ---
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("[!] FATAL: DATABASE_URL not found in environment or .env file.")
        return

    # Enforce SSL for Supabase if not present
    if "supabase.co" in dsn and "sslmode=" not in dsn:
        dsn += "&sslmode=require" if "?" in dsn else "?sslmode=require"
    
    print(f"[*] Initializing Manual Phase 2 Test on: {dsn.split('@')[-1]}")
    
    db = Database(dsn)
    try:
        await db.connect()
    except Exception as e:
        print(f"[!] Connection failed: {e}")
        return

    # --- 2. Migration Check ---
    async with db.get_connection() as conn:
        events_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'events')"
        )
        if not events_exists:
            print("[*] Found missing tables. Applying src/schema.sql...")
            schema_path = os.path.join(os.getcwd(), "src", "schema.sql")
            await db.init_schema(schema_path)
            print("[✓] Schema updated.")
        else:
            print("[✓] Base tables present.")

    store = EventStore(db)
    repo = AggregateRepository(store, db)

    # Unique ID for this test session
    run_id = str(uuid.uuid4())[:8]
    loan_id = f"loan-{run_id}"
    agent_id = f"agent-credit-analyst"
    session_id = f"sess-{run_id}"
    
    now = datetime.now()
    
    print(f"\n[+] STARTING FULL WORKFLOW for Loan ID: {loan_id}")

    try:
        loan = LoanApplicationAggregate(loan_id)
        
        # --- 3. Loan Submission ---
        print("\n--- Phase 2.1: Loan Application Submission ---")
        loan.submit_application(ApplicationSubmitted(
            application_id=loan_id, applicant_id="user-123",
            requested_amount_usd=Decimal("450000.0"), loan_purpose=LoanPurpose.WORKING_CAPITAL,
            loan_term_months=36, submission_channel="WEB", contact_email="test@example.com",
            contact_name="Aman Test", submitted_at=now, application_reference=f"REF-{run_id}"
        ))
        await repo.save(loan, "LoanApplication")
        print(f"[✓] State: {loan.state.value}")

        # --- 4. Transition: DOCUMENTS_PENDING -> UPLOADED -> PROCESSED ---
        print("\n--- Phase 2.2: Document Processing Transitions ---")
        loan.request_document_upload(DocumentUploadRequested(
            application_id=loan_id, required_document_types=[DocumentType.INCOME_STATEMENT],
            deadline=now, requested_by="system"
        ))
        loan.add_document(DocumentUploaded(
            application_id=loan_id, document_id="doc-123", document_type=DocumentType.INCOME_STATEMENT,
            document_format=DocumentFormat.PDF, filename="inc.pdf", file_path="/s3/inc.pdf",
            file_size_bytes=1024, file_hash="abc", uploaded_at=now, uploaded_by="user-123"
        ))
        loan.record_documents_processed()
        await repo.save(loan, "LoanApplication")
        print(f"[✓] State: {loan.state.value}")

        # --- 5. Transition: CREDIT_REQUESTED -> CREDIT_COMPLETE ---
        print("\n--- Phase 2.3: Credit Transitions ---")
        loan.record_credit_request(CreditAnalysisRequested(
            application_id=loan_id, requested_at=now, requested_by="system"
        ))
        
        # Create separate aggregate for CreditRecord
        credit = CreditRecordAggregate(f"credit-{loan_id}")
        credit.open_record(CreditRecordOpened(
            application_id=loan_id, applicant_id="user-123", opened_at=now
        ))
        credit_event = CreditAnalysisCompleted(
            application_id=loan_id, session_id="sess-1",
            decision=CreditDecision(risk_tier=RiskTier.LOW, recommended_limit_usd=Decimal("500000.0"), confidence=0.95, rationale="Fine."),
            model_version="v1", model_deployment_id="d1", input_data_hash="h1", analysis_duration_ms=100, completed_at=now
        )
        credit.complete_analysis(credit_event)
        await repo.save(credit, "CreditRecord")
        
        loan.record_credit_analysis(credit_event)
        await repo.save(loan, "LoanApplication")
        print(f"[✓] State: {loan.state.value}")

        # --- 6. Transition: FRAUD_REQUESTED -> FRAUD_COMPLETE ---
        print("\n--- Phase 2.4: Fraud Transitions ---")
        loan.append_event(FraudScreeningRequested(application_id=loan_id, requested_at=now, triggered_by_event_id="e1"))
        
        fraud = FraudScreeningAggregate(f"fraud-{loan_id}")
        fraud.initiate_screening(FraudScreeningInitiated(
            application_id=loan_id, session_id="sess-1", screening_model_version="v1", initiated_at=now
        ))
        fraud_event = FraudScreeningCompleted(
            application_id=loan_id, session_id="sess-1", fraud_score=0.1, risk_level="LOW",
            anomalies_found=0, recommendation="CLEAR", screening_model_version="v1", input_data_hash="h1", completed_at=now
        )
        fraud.complete_screening(fraud_event)
        await repo.save(fraud, "FraudScreening")
        
        loan.record_fraud_screening(fraud_event)
        await repo.save(loan, "LoanApplication")
        print(f"[✓] State: {loan.state.value}")

        # --- 7. Transition: COMPLIANCE_REQUESTED -> COMPLIANCE_COMPLETE ---
        print("\n--- Phase 2.5: Compliance Transitions ---")
        loan.append_event(ComplianceCheckRequested(
            application_id=loan_id, requested_at=now, triggered_by_event_id="e2",
            regulation_set_version="v1", rules_to_evaluate=["r1"]
        ))
        
        compliance = ComplianceRecordAggregate(f"compliance-{loan_id}")
        compliance.initiate_check(ComplianceCheckInitiated(
            application_id=loan_id, session_id="sess-1", regulation_set_version="v1", rules_to_evaluate=["r1"], initiated_at=now
        ))
        compliance_event = ComplianceCheckCompleted(
            application_id=loan_id, session_id="sess-1", rules_evaluated=1, rules_passed=1, 
            rules_failed=0, rules_noted=0, has_hard_block=False, overall_verdict=ComplianceVerdict.CLEAR, completed_at=now
        )
        compliance.complete_check(compliance_event)
        await repo.save(compliance, "ComplianceRecord")
        
        loan.record_compliance_check(compliance_event)
        await repo.save(loan, "LoanApplication")
        print(f"[✓] State: {loan.state.value}")

        # --- 8. Final Approval ---
        print("\n--- Phase 2.6: Final Approval ---")
        loan.approve(
            ApplicationApproved(
                application_id=loan_id, approved_amount_usd=Decimal("450000.0"),
                interest_rate_pct=5.5, term_months=36, approved_by="officer-456",
                effective_date="2026-03-23", approved_at=now
            ),
            credit_record=credit, fraud_record=fraud, compliance_record=compliance
        )
        await repo.save(loan, "LoanApplication")
        print(f"[✓] Final State: {loan.state.value}")

        # --- 9. Audit ---
        audit = AuditLedgerAggregate("audit-finance")
        audit.append_integrity_check(AuditIntegrityCheckRun(
            entity_type="Finance", entity_id="f1", check_timestamp=now, events_verified_count=10,
            integrity_hash="h1", previous_hash=None, chain_valid=True, tamper_detected=False, recorded_at=now
        ))
        await repo.save(audit, "AuditLedger")
        print("\n[✓] Audit Ledger Recorded.")

        print(f"\n[★] PHASE 2 MANUAL TEST PASSED SUCCESSFULLY!")

    except Exception as e:
        print(f"\n[!] ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        await db.disconnect()
        print("\n[+] WORKFLOW COMPLETE.")

if __name__ == "__main__":
    asyncio.run(test_phase2_workflow())
