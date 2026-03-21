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
async def test_concurrency_credit_analysis_duplicate_prevention():
    # Setup test DB (Assuming fixtures or similar pattern, but we can do inline setup)
    # Using an isolated event store instance if possible or mock, but let's test against actual event_store + DB
    # The application tests typically expect postgres, we assume `event_store` is injected via a fixture.
    pass

# We will write the test using a fixture.
@pytest.mark.asyncio
async def test_duplicate_analysis_resolution(db: Database):
    store = EventStore(db)
    repo = AggregateRepository(store, db)
    
    stream_id = "credit-conc-test-1"
    
    # 1. Initialize CreditRecord with CreditRecordOpened
    agg = CreditRecordAggregate(stream_id)
    opened = CreditRecordOpened(
        application_id="conc-test-1", applicant_id="u1", opened_at=datetime.now()
    )
    agg.open_record(opened)
    await repo.save(agg, "CreditRecord")
    
    # version is now 1.
    
    analysis1 = CreditAnalysisCompleted(
        application_id="conc-test-1", session_id="s1",
        decision=CreditDecision(risk_tier=RiskTier.LOW, recommended_limit_usd=Decimal("100"), confidence=0.9, rationale="ok"),
        model_version="v1", model_deployment_id="d1", input_data_hash="h", analysis_duration_ms=10, completed_at=datetime.now()
    )
    
    analysis2 = CreditAnalysisCompleted(
        application_id="conc-test-1", session_id="s2",
        decision=CreditDecision(risk_tier=RiskTier.MEDIUM, recommended_limit_usd=Decimal("50"), confidence=0.8, rationale="ok"),
        model_version="v1", model_deployment_id="d1", input_data_hash="h", analysis_duration_ms=10, completed_at=datetime.now()
    )
    load_event = asyncio.Event()

    async def agent_task(analysis_event, is_slow=False):
        try:
            # Load agg
            agent_agg = await repo.load(CreditRecordAggregate, stream_id)
            
            if is_slow:
                load_event.set()
                await asyncio.sleep(0.1) # Let fast task save
            else:
                await load_event.wait()
                
            agent_agg.complete_analysis(analysis_event)
            await repo.save(agent_agg, "CreditRecord")
            return "SUCCESS"
        except OptimisticConcurrencyError:
            # Losing agent behavior:
            # Caught OCC, reloads stream
            reloaded_agg = await repo.load(CreditRecordAggregate, stream_id)
            # Detects existing analysis
            if reloaded_agg.has_analysis:
                return "OCC_DETECTED_EXIT"
            return "OCC_BUT_NO_ANALYSIS"
        except Exception as e:
            return f"OTHER_ERROR: {str(e)}"
            
    # Run them simultaneously
    results = await asyncio.gather(
        agent_task(analysis1, is_slow=False), 
        agent_task(analysis2, is_slow=True)
    )
    
    assert "SUCCESS" in results
    assert "OCC_DETECTED_EXIT" in results

    # Final stream has exactly one analysis
    final_agg = await repo.load(CreditRecordAggregate, stream_id)
    events = await store.load_stream(stream_id)
    
    analysis_events = [e for e in events if e.event_type == "CreditAnalysisCompleted"]
    assert len(analysis_events) == 1
