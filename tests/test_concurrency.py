import pytest
import asyncio
from decimal import Decimal
from datetime import datetime

from src.database import Database
from src.event_store import EventStore
from src.aggregates.credit_record import CreditRecordAggregate
from src.aggregates.repository import AggregateRepository
from src.exceptions import OptimisticConcurrencyError
from src.models.events import CreditRecordOpened, CreditAnalysisCompleted, CreditDecision, RiskTier

@pytest.mark.asyncio
async def test_interim_double_decision_concurrency(db: Database):
    """
    Interim Sunday Requirement:
    - two concurrent asyncio tasks
    - appending to same stream at expected_version=3
    - exactly one succeeds, one raises OptimisticConcurrencyError
    - total stream length = 4
    """
    store = EventStore(db)
    repo = AggregateRepository(store, db)
    stream_id = "credit-interim-test"
    
    from src.models.events import HistoricalProfileConsumed, ExtractedFactsConsumed
    
    # 1. Initialize to version 3
    agg = CreditRecordAggregate(stream_id)
    agg.append_event(CreditRecordOpened.model_construct(
        application_id="interim", applicant_id="u1", opened_at=datetime.now(),
        event_type="CreditRecordOpened", event_version=1
    ))
    agg.append_event(HistoricalProfileConsumed.model_construct(
        application_id="interim", session_id="s_hist", fiscal_years_loaded=[2023],
        has_prior_loans=False, has_defaults=False, revenue_trajectory="UP", data_hash="h1",
        consumed_at=datetime.now(), event_type="HistoricalProfileConsumed", event_version=1
    ))
    agg.append_event(ExtractedFactsConsumed.model_construct(
        application_id="interim", session_id="s_facts", document_ids_consumed=["d1"],
        facts_summary="ok", quality_flags_present=False, consumed_at=datetime.now(),
        event_type="ExtractedFactsConsumed", event_version=1
    ))
    
    assert agg.version == 0 # Version stays at 0 for uncommitted
    await repo.save(agg, "CreditRecord")
    assert agg.version == 3 # Now version is 3
    
    # 2. Setup concurrent tasks
    dec1 = CreditDecision(risk_tier=RiskTier.LOW, recommended_limit_usd=Decimal("100"), confidence=0.9, rationale="ok")
    dec2 = CreditDecision(risk_tier=RiskTier.MEDIUM, recommended_limit_usd=Decimal("50"), confidence=0.8, rationale="ok")
    
    analysis1 = CreditAnalysisCompleted.model_construct(
        application_id="interim", session_id="s1", decision=dec1, model_version="v1",
        model_deployment_id="d1", input_data_hash="h", analysis_duration_ms=10, 
        completed_at=datetime.now(), event_type="CreditAnalysisCompleted", event_version=2
    )
    analysis2 = CreditAnalysisCompleted.model_construct(
        application_id="interim", session_id="s2", decision=dec2, model_version="v1",
        model_deployment_id="d1", input_data_hash="h", analysis_duration_ms=10, 
        completed_at=datetime.now(), event_type="CreditAnalysisCompleted", event_version=2
    )

    barrier = asyncio.Event()

    async def agent_task(analysis_event, is_slow=False):
        # Step 1: Load at version 3
        agent_agg = await repo.load(CreditRecordAggregate, stream_id)
        assert agent_agg.version == 3 # Both load v3
        
        if is_slow:
            barrier.set()
            await asyncio.sleep(0.1) # Wait for fast one to win
        else:
            await barrier.wait()
            
        # Step 2: Apply logic
        agent_agg.complete_analysis(analysis_event)
        
        # Step 3: Save at expected_version=3
        await repo.save(agent_agg, "CreditRecord")
        return "SUCCESS"

    # 3. Execute
    tasks = [
        agent_task(analysis1, is_slow=False),
        agent_task(analysis2, is_slow=True)
    ]
    
    # We expect one SUCCESS and one OptimisticConcurrencyError
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    success_count = sum(1 for r in results if r == "SUCCESS")
    occ_count = sum(1 for r in results if isinstance(r, OptimisticConcurrencyError))
    
    assert success_count == 1
    assert occ_count == 1
    
    # 4. Assert total stream length = 4
    events = await store.load_stream(stream_id)
    assert len(events) == 4
    assert events[-1].stream_position == 4
