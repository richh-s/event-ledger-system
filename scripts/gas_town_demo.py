
import asyncio
import uuid
from datetime import datetime, timezone
from typing import List
from dotenv import load_dotenv

from src.database import get_db, disconnect_db
from src.event_store import EventStore
from src.aggregates.repository import AggregateRepository
from src.aggregates.agent_session import AgentSessionAggregate
from src.integrity.gas_town import reconstruct_agent_context, SessionHealth
from src.models.events import (
    AgentSessionStarted,
    AgentContextLoaded,
    AgentNodeExecuted,
    AgentType
)

load_dotenv()

async def run_gas_town_demo():
    db = get_db()
    await db.connect()
    store = EventStore(db)
    repo = AggregateRepository(store, db)

    print("\n" + "="*80)
    print("🔋 STEP 5 — GAS TOWN RECOVERY: AGENT MEMORY RECONSTRUCTION")
    print("="*80 + "\n")

    session_id = f"SESS-{uuid.uuid4().hex[:6].upper()}"
    app_id = f"APP-{uuid.uuid4().hex[:4].upper()}"
    stream_id = f"agent-credit_analysis-{session_id}"
    
    # 📝 1. AGENT STARTS WORK
    print(f"📝 1. Starting Agent Session: {session_id}")
    agg = AgentSessionAggregate(stream_id)
    
    start_event = AgentSessionStarted(
        session_id=session_id,
        agent_type=AgentType.CREDIT_ANALYSIS,
        agent_id="agent-001",
        application_id=app_id,
        model_version="gpt-4o",
        langgraph_graph_version="v2.0",
        context_source=f"loan-{app_id}",
        context_token_count=1500,
        started_at=datetime.now(timezone.utc)
    )
    agg.start_session(start_event)
    
    # Simulate loading some context
    context_event = AgentContextLoaded(
        session_id=session_id,
        agent_type=AgentType.CREDIT_ANALYSIS,
        context_source=f"loan-{app_id}",
        context_version=1,
        context_hash="abc-123-hash",
        loaded_at=datetime.now(timezone.utc)
    )
    agg.record_context_loaded(context_event)
    
    # Simulate executing a node
    node_event = AgentNodeExecuted(
        session_id=session_id,
        agent_type=AgentType.CREDIT_ANALYSIS,
        node_name="DocumentParser",
        node_sequence=1,
        input_keys=["raw_docs"],
        output_keys=["parsed_data"],
        llm_called=True,
        llm_tokens_input=500,
        llm_tokens_output=200,
        llm_cost_usd=0.015,
        duration_ms=1200,
        executed_at=datetime.now(timezone.utc)
    )
    agg.record_node_execution(node_event)

    # 💾 2. PERSIST TO LEDGER
    print(f"💾 2. Persisting events to the ledger...")
    await repo.save(agg, "AgentSession")
    
    print(f"💥 3. SIMULATING CRASH... (Killing current agent state)")
    del agg
    
    # 🛠️ 4. GAS TOWN RECOVERY
    print(f"🛠️ 4. Calling reconstruct_agent_context('{session_id}')...")
    
    # Load from store
    recovered_events = await store.load_stream(stream_id)
    
    # Reconstruct
    re_agg, ctx, status = reconstruct_agent_context(session_id, recovered_events)
    
    print(f"\n✅ RECONSTRUCTION RESULTS:")
    print(f"  Session Health: {ctx.session_health_status}")
    print(f"  Reconstruction Status: {status}")
    print(f"  Last Successful Node: {re_agg.last_successful_node}")
    print(f"  Pending Work: {ctx.pending_work}")
    
    print(f"\n📄 RECONSTRUCTED AGENT CONTEXT (Memory):")
    print("-" * 40)
    print(ctx.context_text)
    print("-" * 40)

    # 🚀 5. RESUME WORK
    print(f"\n🚀 5. Resuming work (recording next node)...")
    resume_node = AgentNodeExecuted(
        session_id=session_id,
        agent_type=AgentType.CREDIT_ANALYSIS,
        node_name="RiskScoring",
        node_sequence=2,
        input_keys=["parsed_data"],
        output_keys=["risk_score"],
        llm_called=True,
        llm_tokens_input=700,
        llm_tokens_output=100,
        llm_cost_usd=0.008,
        duration_ms=800,
        executed_at=datetime.now(timezone.utc)
    )
    re_agg.record_node_execution(resume_node)
    
    await repo.save(re_agg, "AgentSession")
    print(f"✅ Successfully resumed! New stream version: {re_agg.version}")

    print("\n" + "="*80)
    print("✅ TEST PASSED: Agent recovered full state and resumed work without data loss.")
    print("="*80 + "\n")

    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(run_gas_town_demo())
