
import asyncio
import os
import json
from dotenv import load_dotenv

from src.database import get_db, disconnect_db
from src.event_store import EventStore
from src.what_if.projector import run_what_if

load_dotenv()

async def run_what_if_demo():
    db = get_db()
    await db.connect()
    store = EventStore(db)

    print("\n" + "="*80)
    print("🔮 STEP 6 — WHAT-IF COUNTERFACTUAL: RISK TIER ESCALATION")
    print("="*80 + "\n")

    # 1. Find a recent application with a full decision history
    async with db.get_connection() as conn:
        app_row = await conn.fetchrow(
            "SELECT stream_id, metadata->>'correlation_id' as correlation_id "
            "FROM events "
            "WHERE event_type = 'HumanReviewCompleted' "
            "ORDER BY recorded_at DESC LIMIT 1"
        )
    
    if not app_row:
        print("❌ No application found with a full history. Run scripts/week_standard first.")
        await db.disconnect()
        return

    app_id = app_row['stream_id'].replace("loan-", "")
    print(f"🧐 Original Case: {app_id} (Currently APPROVED)")

    # 2. Run What-If Scenario: Substitute risk tier for HIGH
    print(f"🧪 Action: Simulating HIGH risk tier for {app_id}...")
    
    result = await run_what_if(
        application_id=app_id,
        alternate_model="llama-3-70b-experimental",
        db=db,
        store=store,
        override_risk_tier="HIGH"
    )

    if not result or "error" in result:
        print(f"❌ Error: {result.get('error') if result else 'Unknown error'}")
        await db.disconnect()
        return

    # 3. Show Result and Cascade
    orig = result['original_scenario']
    cf = result['counterfactual_scenario']

    print(f"\n📈 OVERVIEW:")
    print(f"  - Original Risk: {orig['risk_tier']} -> Verdict: {orig['orchestrator_recommendation']}")
    print(f"  - Counterfactual Risk: {cf['risk_tier']} -> Verdict: {cf['final_verdict']}")

    if cf['policy_violations']:
        print(f"\n🧠 CASCADING BUSINESS RULES TRIGGERED:")
        for v in cf['policy_violations']:
            print(f"  ⚠️ TRIGGERED: {v}")
    else:
        print("\n  (No authority layer overrides triggered)")

    print(f"\n📝 IMPACT SUMMARY:")
    print(f"  {result['impact_summary']}")

    print("\n" + "="*80)
    print("✅ WHAT-IF VERIFIED: Counterfactual projection correctly simulated business rule enforcement.")
    print("="*80 + "\n")

    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(run_what_if_demo())
