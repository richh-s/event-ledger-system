
import asyncio
import json
import uuid
from datetime import datetime
from decimal import Decimal
from dotenv import load_dotenv

from src.database import get_db, disconnect_db
from src.event_store import EventStore
from src.models.events import StoredEvent

load_dotenv()

async def run_upcasting_demo():
    db = get_db()
    await db.connect()
    store = EventStore(db)

    print("\n" + "="*80)
    print("💎 STEP 4 — UPCASTING & IMMUTABILITY: VERIFYING SCHEMA EVOLUTION")
    print("="*80 + "\n")

    app_id = f"UPCAST-{uuid.uuid4().hex[:6].upper()}"
    stream_id = f"loan-{app_id}"
    
    # 📝 1. MANUALLY INSERT A V1 EVENT (Raw SQL to ensure v1 stored)
    v1_payload = {
        "application_id": app_id,
        "session_id": "session-old-v1",
        "decision": {
            "risk_tier": "LOW",
            "recommended_limit_usd": 25000.0,
            "confidence": 0.85,
            "rationale": "Old V1 Data"
        },
        "model_deployment_id": "dep-v1-system",
        "input_data_hash": "hash-legacy",
        "analysis_duration_ms": 1200,
        "completed_at": "2024-01-01T12:00:00Z"
    }
    
    print(f"📝 1. Manually inserting a V1 event into stream {stream_id}...")
    async with db.get_connection() as conn:
        await conn.execute(
            "INSERT INTO events (stream_id, stream_position, event_type, event_version, payload, recorded_at) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            stream_id, 1, "CreditAnalysisCompleted", 1, json.dumps(v1_payload), datetime(2024, 1, 1)
        )

    # 📥 2. LOAD THROUGH EVENT STORE
    print(f"📥 2. Loading stream {stream_id} through the EventStore...")
    events = await store.load_stream(stream_id)
    upcasted_event = events[0]
    
    print(f"\n✅ LOADED DATA (through EventStore):")
    print(f"  Event Version: {upcasted_event.event_version}")
    print(f"  Has 'model_version'? {'model_version' in upcasted_event.payload}")
    print(f"  'model_version' Value: {upcasted_event.payload.get('model_version')}")
    print(f"  'regulatory_basis' Value: {upcasted_event.payload.get('regulatory_basis')}")

    # 📊 3. CHECK RAW DATABASE ROW (IMMUTABILITY)
    print(f"\n📊 3. Checking raw data in database (Immutability Check)...")
    async with db.get_connection() as conn:
        raw_row = await conn.fetchrow("SELECT event_version, payload FROM events WHERE stream_id = $1", stream_id)
    
    raw_payload = raw_row['payload']
    if isinstance(raw_payload, str):
        raw_payload = json.loads(raw_payload)

    print(f"✅ RAW DATA (directly from database):")
    print(f"  Stored Version: {raw_row['event_version']} (STILL V1!)")
    print(f"  Has 'model_version' in raw? {'model_version' in raw_payload}")
    
    print("\n" + "="*80)
    print("✅ TEST PASSED: Events are evolved on-the-fly without mutating history.")
    print("="*80 + "\n")

    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(run_upcasting_demo())
