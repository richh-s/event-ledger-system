import asyncio
import os
import time
import uuid
from src.database import Database
from src.event_store import EventStore
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.models.events import ApplicationSubmitted, LoanPurpose
from datetime import datetime

# Performance Test Config - Target 500 events
NUM_CONCURRENT_HANDLERS = 10
EVENTS_PER_HANDLER = 50 

async def run_validation():
    db = Database(os.getenv("DATABASE_URL"))
    await db.connect()
    store = EventStore(db)
    
    async with db.transaction() as conn:
        print("Cleaning system for final Phase 3 validation...")
        await conn.execute("DELETE FROM outbox")
        await conn.execute("DELETE FROM events")
        await conn.execute("DELETE FROM event_streams")
        await conn.execute("DELETE FROM application_summary")
        await conn.execute("DELETE FROM projection_checkpoints")
        await conn.execute("INSERT INTO projection_checkpoints (projection_name, last_position) VALUES ('application_summary', 0)")

    # 2. Start projection daemon
    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(db, store, [proj], batch_size=5000, poll_interval_ms=10)
    await daemon.start()

    # 3. Parallel Ingest (simulating high write load)
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

    print(f"Ingesting 500 events from 10 concurrent streams...")
    start_all = time.time()
    await asyncio.gather(*(run_handler(i) for i in range(NUM_CONCURRENT_HANDLERS)))
    ingest_done = time.time()
    
    # 4. Wait for Catch-up (extended for remote DB latency)
    total_expected = NUM_CONCURRENT_HANDLERS * EVENTS_PER_HANDLER
    caught_up = False
    for i in range(240): # Wait up to 120s AFTER ingestion
        lag = await daemon.get_lag("application_summary")
        async with db.get_connection() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM application_summary")
        
        if i % 20 == 0:
            print(f"T+{int(time.time() - start_all)}s | Current Lag: {lag} | Projected Rows: {count}/{total_expected}")
            
        if lag == 0 and count >= total_expected:
            caught_up = True
            break
        await asyncio.sleep(0.5)

    duration = time.time() - start_all
    print(f"✅ Final Summary: {count}/{total_expected} projected in {duration:.2f}s total.")
    
    if not caught_up:
        print(f"❌ SLO ALERT: Lag remained at {lag}")
        exit(1)
    
    await daemon.stop()
    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(run_validation())
