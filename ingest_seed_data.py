import asyncio
import os
import json
import sys
from datetime import datetime

# --- LOAD .ENV NATIVELY ---
def load_env():
    env_path = os.path.join(os.getcwd(), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    line = line.strip()
                    if not line: continue
                    key, value = line.split("=", 1)
                    os.environ[key] = value.strip('"').strip("'")

load_env()
sys.path.append(os.getcwd())
from src.database import Database

async def ingest_data():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("[!] DATABASE_URL not found.")
        return

    db = Database(dsn)
    await db.connect()

    seed_file = "starter/data/seed_events.jsonl"
    if not os.path.exists(seed_file):
        print(f"[!] Seed file not found at {seed_file}")
        return

    print(f"[*] Starting FAST-BATCH ingestion from {seed_file}...")
    
    with open(seed_file, "r") as f:
        lines = f.readlines()

    count = 0
    batch_size = 50
    m = {
        "loan": "LoanApplication", "docpkg": "DocumentPackage", 
        "agent": "AgentSession", "credit": "CreditRecord", 
        "fraud": "FraudScreening", "compliance": "ComplianceRecord"
    }

    async with db.get_connection() as conn:
        tx = None
        for line in lines:
            if not line.strip(): continue
            if count % batch_size == 0:
                tx = conn.transaction()
                await tx.start()

            data = json.loads(line)
            stream_id = data["stream_id"]
            event_type = data["event_type"]
            payload = data["payload"]
            recorded_at = datetime.fromisoformat(data["recorded_at"])
            
            prefix = stream_id.split("-")[0]
            agg_type = m.get(prefix, prefix.capitalize())
            
            await conn.execute(
                "INSERT INTO event_streams (stream_id, aggregate_type, current_version) VALUES ($1, $2, 0) ON CONFLICT (stream_id) DO UPDATE SET aggregate_type = $2",
                stream_id, agg_type
            )
            curr_v = await conn.fetchval("SELECT current_version FROM event_streams WHERE stream_id = $1", stream_id)
            new_v = (curr_v or 0) + 1
            await conn.execute(
                "INSERT INTO events (stream_id, stream_position, event_type, payload, recorded_at) VALUES ($1, $2, $3, $4, $5)",
                stream_id, new_v, event_type, json.dumps(payload), recorded_at
            )
            await conn.execute("UPDATE event_streams SET current_version = $1 WHERE stream_id = $2", new_v, stream_id)
            
            count += 1
            if count % batch_size == 0:
                if tx:
                    await tx.commit()
                    tx = None
                print(f"[*] Ingested {count} / {len(lines)} events...", flush=True)

        if tx:
            await tx.commit()

    await db.disconnect()
    print(f"[✓] Successfully ingested {count} events.")

if __name__ == "__main__":
    asyncio.run(ingest_data())
