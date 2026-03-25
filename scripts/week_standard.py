import asyncio
import os
import time
import uuid
import json
from dotenv import load_dotenv

from src.database import Database
from src.event_store import EventStore
from src.mcp.tools import (
    submit_application, record_credit_analysis, record_fraud_screening,
    record_compliance_check, generate_decision, record_human_review, start_agent_session
)
from src.integrity.audit_chain import AuditChainVerifier

load_dotenv()

async def run_week_standard():
    start_time = time.time()
    
    app_id = f"APP-MCP-{uuid.uuid4().hex[:6].upper()}"
    applicant = "CUST-WEEK-01"
    print(f"\n==================================================================")
    print(f"🚀 THE WEEK STANDARD: DECISION HISTORY FOR {app_id}")
    print(f"==================================================================\n")
    
    db = Database(os.getenv("DATABASE_URL"))
    await db.connect()
    store = EventStore(db)
    verifier = AuditChainVerifier(db, store)
    
    try:
        def check_res(res, step_name):
            if isinstance(res, str) and '{"error":' in res:
                print(f"❌ ERROR in {step_name}: {res}")
                raise Exception(f"{step_name} failed: {res}")
            return res

        # 1. Submit App
        print(f"📝 1. Submitting Application {app_id}...")
        res = await submit_application(
            application_id=app_id, applicant_id=applicant, requested_amount_usd=150000.0,
            loan_purpose="working_capital", loan_term_months=48
        )
        check_res(res, "Submit App")
        
        async with db.get_connection() as conn:
            sub_event_id = await conn.fetchval(
                "SELECT event_id FROM events WHERE stream_id = $1 AND event_type = 'ApplicationSubmitted'",
                f"loan-{app_id}"
            )

        # 2. Credit Analysis Agent
        print(f"🤖 2. Running Agent: Credit Analysis...")
        c_sess = await start_agent_session(
            agent_type="credit_analysis", model_version="gpt-4o",
            context_source="internal_db", application_id=app_id, causation_id=str(sub_event_id)
        )
        check_res(c_sess, "Start Credit Session")
        res = await record_credit_analysis(
            session_id=c_sess, application_id=app_id, recommendation="APPROVE",
            confidence=0.92, risk_tier="LOW", recommended_limit_usd=150000.0, rationale="Strong financials."
        )
        check_res(res, "Record Credit Analysis")

        # 3. Fraud Detection Agent
        print(f"🤖 3. Running Agent: Fraud Screening...")
        f_sess = await start_agent_session(
            agent_type="fraud_detection", model_version="gpt-4o",
            context_source="internal_db", application_id=app_id, causation_id=str(sub_event_id)
        )
        check_res(f_sess, "Start Fraud Session")
        res = await record_fraud_screening(
            session_id=f_sess, application_id=app_id, fraud_score=0.03, anomalies_found=0, verification_notes="Identity verified."
        )
        check_res(res, "Record Fraud Screening")

        # 4. Compliance Agent
        print(f"🤖 4. Running Agent: Compliance Check...")
        comp_sess = await start_agent_session(
            agent_type="compliance", model_version="gpt-4o",
            context_source="internal_db", application_id=app_id, causation_id=str(sub_event_id)
        )
        check_res(comp_sess, "Start Compliance Session")
        res = await record_compliance_check(
            session_id=comp_sess, application_id=app_id, overall_verdict="CLEAR", rules_evaluated=8,
            fail_count=0, hard_block=False, details="No sanctions. AML checks passed."
        )
        check_res(res, "Record Compliance Check")

        # 5. Decision Orchestrator Agent
        print(f"🤖 5. Running Agent: Decision Orchestration...")
        d_sess = await start_agent_session(
            agent_type="decision_orchestrator", model_version="gpt-4o",
            context_source="internal_db", application_id=app_id, causation_id=str(sub_event_id)
        )
        check_res(d_sess, "Start Decision Session")
        res = await generate_decision(
            session_id=d_sess, application_id=app_id, recommendation="APPROVE", confidence=0.95,
            rationale="All criteria met and compliance checks cleared.", key_concerns="None"
        )
        check_res(res, "Generate Decision")

        # 6. Human Review
        print(f"👨‍⚖️ 6. Recording Human Review...")
        res = await record_human_review(
            application_id=app_id, reviewer_id="MANAGER-X", decision="APPROVED", notes="Final sign-off authorized."
        )
        check_res(res, "Record Human Review")

        # 7. Complete Event Tree & Causal Links
        print(f"\n==================================================================")
        print(f"🔗 CAUSAL LEDGER STREAM (Application ID: {app_id})")
        print(f"==================================================================")
        
        events = await store.load_stream(f"loan-{app_id}")
        events.sort(key=lambda x: x.global_position)
        for evt in events:
            corr = evt.metadata.get('correlation_id', 'N/A')
            caus = evt.metadata.get('causation_id', 'N/A')
            print(f"  [Pos: {evt.global_position}] {evt.event_type:<30}")
            print(f"      ↳ CorrID: {corr} | CausID: {caus}")

        # 8. Integrity Verification
        print(f"\n==================================================================")
        print(f"🔐 CRYPTOGRAPHIC INTEGRITY VERIFICATION")
        print(f"==================================================================")
        result = await verifier.verify_stream_integrity(f"loan-{app_id}")
        print(f"  - Events Verified: {result['events_verified']}")
        print(f"  - Chain Valid:     {'✅ YES' if result['chain_valid'] else '❌ NO'}")
        print(f"  - Tamper Detected: {'❌ YES' if result['tamper_detected'] else '✅ NO'}")
        print(f"  - Integrity Hash:  {result['integrity_hash']}")

    finally:
        await db.disconnect()
        end_time = time.time()
        elapsed = end_time - start_time
        print(f"\n⏱️  Execution Time: {elapsed:.2f} seconds")
        if elapsed < 60.0:
            print(f"✅ PASSED THE WEEK STANDARD (< 60s)")
        else:
            print(f"❌ FAILED THE WEEK STANDARD (>= 60s)")

if __name__ == "__main__":
    asyncio.run(run_week_standard())
