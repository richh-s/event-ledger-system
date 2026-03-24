"""
phase5_snapshot_test.py
========================
Phase 5 Ledger Architecture Integrity Snapshot Test.
This script demonstrates the end-to-end loan application lifecycle,
verifying causal integrity, agent tracing, and cryptographic audit.
"""
import asyncio
import os
import uuid
import json
from datetime import datetime
from dotenv import load_dotenv

from src.database import get_db
from src.event_store import EventStore
from src.agents.registry_client import ApplicantRegistryClient
from src.agents.document_processing_agent import DocumentProcessingAgent
from src.agents.credit_analysis_agent import CreditAnalysisAgent
from src.agents.fraud_detection_agent import FraudDetectionAgent
from src.agents.compliance_agent import ComplianceAgent
from src.agents.decision_orchestrator import DecisionOrchestratorAgent
from src.integrity.audit_chain import AuditChainVerifier
from src.models import ApplicationSubmitted

from anthropic import AsyncAnthropic

class MockAnthropic:
    """Mock for AsyncAnthropic for snapshot tests."""
    def __init__(self):
        class MockMessages:
            async def create(self, **kwargs):
                return MockResponse()
        self.messages = MockMessages()

class MockResponse:
    def __init__(self, content="Mock response content"):
        self.content = [MockContentBlock(text=content)]
        self.usage = MockUsage()

class MockUsage:
    def __init__(self):
        self.input_tokens = 100
        self.output_tokens = 50

class MockContentBlock:
    def __init__(self, text):
        self.text = text

async def run_snapshot():
    load_dotenv()
    db = get_db()
    await db.connect()
    store = EventStore(db)
    registry = ApplicantRegistryClient(db._pool)
    
    app_id = f"APEX-SNAP-{uuid.uuid4().hex[:6].upper()}"
    corr_id = f"corr-{app_id}"
    print(f"\n🚀 Phase 5 Snapshot Test: {app_id}")
    print("=" * 60)
    
    # --- 1. SEED REGISTRY (Optional, assuming seed_apex.py was run) ---
    # For now, we assume COMP-001 exists.
    
    # --- 2. START LIFECYCLE: Application Submitted ---
    sub_evt = ApplicationSubmitted(
        application_id=app_id,
        applicant_id="COMP-001",
        requested_amount_usd=250000.0,
        loan_purpose="working_capital",
        loan_term_months=12,
        submission_channel="WEB",
        contact_email="test@example.com",
        contact_name="James Smith",
        submitted_at=datetime.utcnow(),
        application_reference="REF-SNAP-001"
    )
    await store.append(
        stream_id=f"loan-{app_id}", 
        events=[sub_evt], 
        expected_version=-1, 
        correlation_id=corr_id, 
        aggregate_type="LoanApplication"
    )
    print(f"✔ Application '{app_id}' recorded in ledger.")
    
    # --- 3. RUN AGENT CHAIN ---
    # Override for test mode
    os.environ["ANTHROPIC_API_KEY"] = "sk-mock-123"
    client = MockAnthropic()
    
    agents = [
        ("DocProcessing", DocumentProcessingAgent("agent-doc-001", "document_processing", store, registry, client)),
        ("CreditAnalysis", CreditAnalysisAgent("agent-credit-001", "credit_analysis", store, registry, client)),
        ("FraudDetection", FraudDetectionAgent("agent-fraud-001", "fraud_detection", store, registry, client)),
        ("Compliance", ComplianceAgent("agent-comp-001", "compliance", store, registry, client)),
        ("Orchestrator", DecisionOrchestratorAgent("agent-orch-001", "decision_orchestrator", store, registry, client))
    ]
    
    last_session_id = None
    for name, agent in agents:
        print(f"🛠 Running {name} Agent...")
        # StepId for Gas Town recovery simulation (always start fresh here)
        result = await agent.process_application(app_id, correlation_id=corr_id)
        last_session_id = agent.session_id
        print(f"  └ Session {last_session_id[-8:]} completed successfully.")

    # --- 4. VERIFY PROJECTIONS ---
    print("\n🔍 Verifying Projections...")
    async with db.get_connection() as conn:
        # Check summary
        summary = await conn.fetchrow("SELECT * FROM application_summary WHERE application_id = $1", app_id)
        if summary:
            print(f"  ✔ application_summary: Status='{summary['state']}', Sessions={len(summary['agent_sessions_completed'])}")
        
        # Check trace
        traces = await conn.fetch("SELECT * FROM agent_decision_trace WHERE application_id = $1", app_id)
        print(f"  ✔ agent_decision_trace: Found {len(traces)} sessions recorded.")
        for r in traces:
             print(f"    - [{r['agent_type']}] Session {r['session_id'][-8:]} via Corr: {r['correlation_id'][-8:]}")

    # --- 5. CRYPTOGRAPHIC AUDIT ---
    print("\n🛡 Running Cryptographic Audit...")
    verifier = AuditChainVerifier(db, store)
    # verify main stream
    res = await verifier.verify_stream_integrity(f"loan-{app_id}")
    
    print(f"  ✔ Audit Result: {'VALID' if res.valid else 'INVALID'}")
    print(f"  ✔ Verified Events Count: {res.verified_count}")
    print(f"  ✔ Final Hash: {res.final_hash[:16]}...")

    if res.valid:
        print("\n✅ SNAPSHOT SUCCESS: LEDGER ARCHITECTURE INTEGRITY CONFIRMED")
    else:
        print("\n❌ SNAPSHOT FAILED: CAUSAL LINKAGE BROKEN")

    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(run_snapshot())
