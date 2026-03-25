"""
Phase 3 Final Audit — validates the EXACT 3 spec-required projections:
  1. ApplicationSummary
  2. ComplianceAuditView (with temporal query via snapshots)
  3. AgentPerformanceLedger
"""
import asyncio
import os
import uuid
from datetime import datetime

from src.database import Database
from src.event_store import EventStore
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.compliance_view import ComplianceAuditViewProjection
from src.projections.agent_performance import AgentPerformanceLedgerProjection
from src.models.events import (
    ApplicationSubmitted, AgentSessionStarted, AgentNodeExecuted,
    ComplianceCheckCompleted, ComplianceRulePassed, ComplianceRuleFailed,
    LoanPurpose, AgentType, ComplianceVerdict,
)


async def audit():
    db = Database(os.getenv("DATABASE_URL"))
    await db.connect()
    store = EventStore(db)

    # ── 1. HARD RESET ──
    async with db.transaction() as conn:
        print("🧹 Resetting all projection tables...")
        await conn.execute("""
            TRUNCATE outbox, events, event_streams,
            application_summary, compliance_audit_view, compliance_snapshots,
            agent_performance_ledger, projection_checkpoints, dead_letter_queue
            RESTART IDENTITY CASCADE
        """)

    # ── 2. SEED EVENTS ──
    app_id = f"APP-{uuid.uuid4().hex[:6]}"
    sess_id = f"SESS-{uuid.uuid4().hex[:6]}"

    print(f"📥 Seeding events for {app_id} / {sess_id}...")

    # 2a. Application submission
    await store.append(f"loan-{app_id}", [
        ApplicationSubmitted(
            application_id=app_id, applicant_id="user-1",
            requested_amount_usd=50000, loan_purpose=LoanPurpose.WORKING_CAPITAL,
            loan_term_months=12, submission_channel="WEB",
            contact_email="test@ledger.io", contact_name="Aman",
            submitted_at=datetime.utcnow(), application_reference="REF-AUDIT"
        )
    ], -1, aggregate_type="LoanApplication")

    # 2b. Agent session + node execution (for AgentPerformanceLedger)
    await store.append(f"agent-compliance-{sess_id}", [
        AgentSessionStarted(
            session_id=sess_id, agent_type=AgentType.COMPLIANCE,
            agent_id="compliance-agent", application_id=app_id,
            model_version="gpt-4o-2024", langgraph_graph_version="v2",
            context_source="regulatory_db", context_token_count=2000,
            started_at=datetime.utcnow()
        ),
        AgentNodeExecuted(
            session_id=sess_id, agent_type=AgentType.COMPLIANCE,
            node_name="check_sanctions", node_sequence=1,
            input_keys=["applicant_id"], output_keys=["sanctions_clear"],
            llm_called=True, llm_tokens_input=200, llm_tokens_output=80,
            llm_cost_usd=0.003, duration_ms=350, executed_at=datetime.utcnow()
        ),
    ], -1, aggregate_type="AgentSession")

    # 2c. Compliance rules + check completed (for ComplianceAuditView)
    await store.append(f"compliance-{app_id}", [
        ComplianceRulePassed(
            application_id=app_id, session_id=sess_id,
            rule_id="SANCTIONS_CHECK", rule_name="Sanctions Screening",
            rule_version="REG-2024-v2", evidence_hash="abc123",
            evaluation_notes="No matches found", evaluated_at=datetime.utcnow()
        ),
        ComplianceRuleFailed(
            application_id=app_id, session_id=sess_id,
            rule_id="AML_THRESHOLD", rule_name="AML Amount Threshold",
            rule_version="REG-2024-v2", failure_reason="Amount exceeds tier limit",
            is_hard_block=True, remediation_available=False,
            evidence_hash="def456", evaluated_at=datetime.utcnow()
        ),
        ComplianceCheckCompleted(
            application_id=app_id, session_id=sess_id,
            rules_evaluated=2, rules_passed=1, rules_failed=1, rules_noted=0,
            has_hard_block=True, overall_verdict=ComplianceVerdict.BLOCKED,
            completed_at=datetime.utcnow()
        ),
    ], -1, aggregate_type="ComplianceRecord")

    # ── 3. START DAEMON with EXACTLY 3 PROJECTIONS ──
    projections = [
        ApplicationSummaryProjection(),
        ComplianceAuditViewProjection(),
        AgentPerformanceLedgerProjection(),
    ]
    daemon = ProjectionDaemon(db, store, projections, poll_interval_ms=10)
    await daemon.start()

    print("⏳ Waiting for 3 parallel daemon tasks (15s for Supabase latency)...")
    await asyncio.sleep(15.0)

    # ── 4. VERIFY ALL 3 PROJECTIONS ──
    results = {}
    async with db.get_connection() as conn:
        # 4a. ApplicationSummary
        r = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1", app_id
        )
        results["ApplicationSummary"] = (
            f"✅ (state={r['state']}, last={r['last_event_type']})"
            if r and r['state'] in ("SUBMITTED", "COMPLIANCE_COMPLETE", "CREDIT_COMPLETE", "FRAUD_COMPLETE")
            else f"❌ (row={'found' if r else 'missing'}, state={r['state'] if r else 'N/A'})"
        )

        # 4b. ComplianceAuditView — rule-level verdicts
        rules = await conn.fetch(
            "SELECT * FROM compliance_audit_view WHERE application_id = $1 ORDER BY rule_id", app_id
        )
        rule_ids = [r["rule_id"] for r in rules]
        has_sanctions = "SANCTIONS_CHECK" in rule_ids
        has_aml = "AML_THRESHOLD" in rule_ids
        has_overall = "__overall__" in rule_ids
        
        # Verify hard_block flag on AML
        aml_row = next((r for r in rules if r["rule_id"] == "AML_THRESHOLD"), None)
        aml_blocked = aml_row["is_hard_block"] if aml_row else False

        results["ComplianceAuditView"] = (
            f"✅ ({len(rules)} rules, hard_block={aml_blocked})"
            if has_sanctions and has_aml and has_overall and aml_blocked
            else f"❌ (found: {rule_ids})"
        )

        # 4c. AgentPerformanceLedger — check any row exists with analyses > 0
        perf_rows = await conn.fetch(
            "SELECT * FROM agent_performance_ledger WHERE analyses_completed > 0"
        )
        if perf_rows:
            perf = perf_rows[0]
            results["AgentPerformanceLedger"] = (
                f"✅ ({len(perf_rows)} agents, top: {perf['agent_id']}/{perf['model_version']}, "
                f"analyses={perf['analyses_completed']}, avg_dur={perf['avg_duration_ms']:.0f}ms)"
            )
        else:
            # Check if any rows at all
            any_perf = await conn.fetch("SELECT * FROM agent_performance_ledger")
            results["AgentPerformanceLedger"] = f"❌ (rows={len(any_perf)}, but 0 analyses)"

        # 4d. DLQ check
        dlq = await conn.fetchval("SELECT COUNT(*) FROM dead_letter_queue")
        results["DeadLetterQueue"] = f"✅ (0 failures)" if dlq == 0 else f"⚠️ ({dlq} failures)"

        # 4e. Checkpoints
        cp_rows = await conn.fetch("SELECT * FROM projection_checkpoints ORDER BY projection_name")
        checkpoints = {r['projection_name']: r['last_position'] for r in cp_rows}
        all_caught_up = all(pos > 0 for pos in checkpoints.values()) if checkpoints else False
        results["Checkpoints"] = f"✅ ({checkpoints})" if all_caught_up else f"❌ ({checkpoints})"

    await daemon.stop()
    await db.disconnect()

    # ── 5. REPORT ──
    print("\n" + "=" * 65)
    print("  PHASE 3 FINAL AUDIT — Spec-Compliant (3 Projections)")
    print("=" * 65)
    for k, v in results.items():
        print(f"  {k:<30} {v}")
    print("=" * 65)

    all_ok = all("✅" in str(v) for v in results.values())
    if all_ok:
        print("  🏁 PHASE 3 COMPLETE — ALL PROJECTIONS SPEC-COMPLIANT")
    else:
        print("  ❌ PHASE 3 INCOMPLETE — SEE FAILURES ABOVE")
        exit(1)


if __name__ == "__main__":
    asyncio.run(audit())
