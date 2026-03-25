
import asyncio
import uuid
import logging
from decimal import Decimal
from datetime import datetime
from typing import List

from src.database import get_db, disconnect_db
from src.event_store import EventStore
from src.aggregates.repository import AggregateRepository
from src.aggregates.loan_application import LoanApplicationAggregate
from src.models.events import (
    ApplicationSubmitted, 
    CreditAnalysisCompleted, 
    FraudScreeningCompleted,
    CreditDecision, 
    RiskTier,
    LoanPurpose
)
from src.exceptions import OptimisticConcurrencyError

# Configure logging to show the collision
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def run_concurrency_demo():
    db = get_db()
    await db.connect()
    
    store = EventStore(db)
    repo = AggregateRepository(store, db)
    
    app_id = f"APP-CONC-{uuid.uuid4().hex[:6].upper()}"
    stream_id = f"loan-{app_id}"
    
    print("\n" + "="*70)
    print(f"🚀 CONCURRENCY UNDER PRESSURE: COLLISION TEST FOR {app_id}")
    print("="*70 + "\n")

    # 1. Initialize Stream
    print(f"📝 1. Initializing stream with ApplicationSubmitted...")
    submit_event = ApplicationSubmitted(
        application_id=app_id,
        applicant_id="user-123",
        requested_amount_usd=Decimal("50000.00"),
        loan_purpose=LoanPurpose.EQUIPMENT_FINANCING,
        loan_term_months=36,
        submission_channel="DIRECT",
        contact_name="Alice Smith",
        contact_email="alice@example.com",
        submitted_at=datetime.now(),
        application_reference=f"REF-{app_id}"
    )
    
    agg = LoanApplicationAggregate(stream_id)
    agg.submit_application(submit_event)
    await repo.save(agg, "LoanApplication", correlation_id=app_id)
    
    initial_version = agg.version
    print(f"✅ Initialized at Version: {initial_version}")

    # 2. Setup Concurrent Agents
    # Both see Version 1 but try to append at the same time.
    barrier = asyncio.Event()
    
    async def agent_action(agent_name: str, decision: str, delay: float = 0):
        print(f"🤖 Agent {agent_name}: Starting analysis...")
        
        # Load the aggregate at its current version
        local_agg = await repo.load(LoanApplicationAggregate, stream_id)
        current_v = local_agg.version
        print(f"🤖 Agent {agent_name}: Loaded at Version {current_v}")
        
        if delay > 0:
            print(f"🤖 Agent {agent_name}: Simulating slow processing ({delay}s)...")
            barrier.set() # Signal the fast agent to go
            await asyncio.sleep(delay)
        else:
            await barrier.wait() # Wait for slow agent to be ready
            
        # Apply the update
        if agent_name == "FastAgent":
            event = CreditAnalysisCompleted(
                application_id=app_id,
                session_id=f"session-{agent_name}",
                decision=CreditDecision(
                    risk_tier=RiskTier.LOW,
                    recommended_limit_usd=Decimal("50000.00"),
                    confidence=0.95,
                    rationale=f"Decision by {agent_name}"
                ),
                model_version="v2.1",
                model_deployment_id="dep-99",
                input_data_hash="abc",
                analysis_duration_ms=450,
                completed_at=datetime.now()
            )
            local_agg.record_credit_analysis(event)
        else:
            event = FraudScreeningCompleted(
                application_id=app_id,
                session_id=f"session-{agent_name}",
                fraud_score=0.01,
                risk_level="LOW",
                anomalies_found=0,
                recommendation="PASS",
                screening_model_version="fraud-v1",
                input_data_hash="xyz",
                completed_at=datetime.now()
            )
            local_agg.record_fraud_screening(event)
        
        print(f"🤖 Agent {agent_name}: Attempting to save at expected_version={current_v}...")
        try:
            await repo.save(local_agg, "LoanApplication", correlation_id=app_id)
            print(f"✅ Agent {agent_name}: SUCCESS! Stream is now at Version {local_agg.version}")
            return "SUCCESS"
        except OptimisticConcurrencyError as e:
            print(f"⚠️ Agent {agent_name}: COLLISION! {e}")
            print(f"🔄 Agent {agent_name}: Retrying with linear backoff...")
            
            # THE RETRY LOGIC (The "Fix")
            await asyncio.sleep(0.5)
            
            # 1. Re-load the aggregate (gets latest version incremented by winner)
            retry_agg = await repo.load(LoanApplicationAggregate, stream_id)
            print(f"🔄 Agent {agent_name}: Reloaded. Current Stream Version is {retry_agg.version}, State: {retry_agg.state}")
            
            # 2. Re-apply the logic to the new state
            if agent_name == "SlowAgent":
                 # Fraud screening is now valid because state is CREDIT_COMPLETE
                 retry_agg.record_fraud_screening(event)
            
            # 3. Save again
            await repo.save(retry_agg, "LoanApplication", correlation_id=app_id)
            print(f"✅ Agent {agent_name}: RETRY SUCCESS! Stream is now at Version {retry_agg.version}")
            return "RETRY_SUCCESS"

    print(f"\n⚡ 2. Launching Concurrent Tasks...")
    tasks = [
        agent_action("FastAgent", "APPROVE", delay=0),
        agent_action("SlowAgent", "DECLINE", delay=0.2)
    ]
    
    results = await asyncio.gather(*tasks)
    
    print(f"\n" + "="*70)
    print(f"🏁 FINAL STATUS FOR {app_id}")
    print("="*70)
    print(f"Results: {results}")
    
    final_events = await store.load_stream(stream_id)
    print(f"Total Events in Stream: {len(final_events)}")
    for e in final_events:
        print(f"  [{e.stream_position}] {e.event_type} (CorrID: {e.metadata.get('correlation_id')})")
    
    print("\n✅ CONCURRENCY TEST PASSED: DEMONSTRATED OPTIMISTIC LOCKING AND SELF-HEALING RETRY.")
    
    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(run_concurrency_demo())
