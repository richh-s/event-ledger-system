import asyncio
import os
import time
import uuid
import pytest
from src.database import Database
from src.event_store import EventStore
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.models.events import ApplicationSubmitted, LoanPurpose
from datetime import datetime

# Performance Test Config - Adjusted for pool stability
NUM_CONCURRENT_HANDLERS = 10 
EVENTS_PER_HANDLER = 50 # Total 500 events

@pytest.mark.asyncio
async def test_lag_under_load():
    db = Database(os.getenv("DATABASE_URL"))
    await db.connect()
    store = EventStore(db)
    
    # Clean up with identity reset
    async with db.transaction() as conn:
        print("Scrubbing DB for performance test...")
        await conn.execute("TRUNCATE events, event_streams, application_summary, projection_checkpoints RESTART IDENTITY CASCADE")

    # Start projection daemon
    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(db, store, [proj], batch_size=5000, poll_interval_ms=10)
    await daemon.start()

    # Parallel Command Handlers (10 concurrent streams)
    async def run_handler(h_id):
        for i in range(EVENTS_PER_HANDLER):
            app_id = f"APP-PERF-{h_id}-{i}-{uuid.uuid4().hex[:6]}"
            event = ApplicationSubmitted(
                application_id=app_id,
                applicant_id=f"user-{h_id}",
                requested_amount_usd=1000,
                loan_purpose=LoanPurpose.WORKING_CAPITAL,
                loan_term_months=12,
                submission_channel="WEB",
                contact_email="perf@example.com",
                contact_name="Perf Bot",
                submitted_at=datetime.utcnow(),
                application_reference=f"REF-{app_id}"
            )
            await store.append(app_id, [event], -1, aggregate_type="Application")

    print(f"Beginning ingest of {NUM_CONCURRENT_HANDLERS * EVENTS_PER_HANDLER} events...")
    start_time = time.time()
    await asyncio.gather(*(run_handler(i) for i in range(NUM_CONCURRENT_HANDLERS)))
    ingest_duration = time.time() - start_time
    total_expected = NUM_CONCURRENT_HANDLERS * EVENTS_PER_HANDLER
    print(f"Ingested {total_expected} events in {ingest_duration:.2f}s")
    
    # SLO Lag Verification: Check if projections keep pace
    caught_up = False
    for i in range(120): # Up to 60s wait
        lag = await daemon.get_lag("application_summary")
        async with db.get_connection() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM application_summary")
            
        if i % 10 == 0:
            print(f"Lag Snapshot: {lag} | Database Projected Rows: {count}/{total_expected}")
            
        if lag == 0 and count >= total_expected:
            caught_up = True
            break
        await asyncio.sleep(0.5)

    duration = time.time() - start_time
    print(f"Total end-to-end duration: {duration:.2f}s")
    
    assert caught_up, f"Daemon failed SLO! Final Lag: {await daemon.get_lag('application_summary')}, Rows: {count}"
    
    await daemon.stop()
    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(test_lag_under_load())
