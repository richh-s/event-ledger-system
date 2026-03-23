import asyncio
import os
import json
import sys
from datetime import datetime
from decimal import Decimal

# --- LOAD .ENV NATIVELY ---
def load_env():
    env_path = os.path.join(os.getcwd(), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value

load_env()
# -------------------------

sys.path.append(os.getcwd())

from src.database import Database

async def ingest_data():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("[!] DATABASE_URL not found.")
        return

    if "supabase.co" in dsn and "sslmode=" not in dsn:
        dsn += "&sslmode=require" if "?" in dsn else "?sslmode=require"

    db = Database(dsn)
    await db.connect()

    seed_file = "starter/data/seed_events.jsonl"
    if not os.path.exists(seed_file):
        print(f"[!] Seed file not found at {seed_file}")
        return

    print(f"[*] Starting ingestion from {seed_file}...")

    # We'll use raw SQL to preserve the exact recorded_at and positions if possible, 
    # but the seed data might not have global_position.
    # Let's check the schema again. 
    # Actually, we should just append them normally to ensure consistency.

    count = 0
    async with db.get_connection() as conn:
        async with conn.transaction():
            # Clear existing data if you want a clean seed? 
            # No, let's just append. 
            
            with open(seed_file, "r") as f:
                for line in f:
                    if not line.strip(): continue
                    data = json.loads(line)
                    
                    stream_id = data["stream_id"]
                    event_type = data["event_type"]
                    payload = data["payload"]
                    recorded_at_str = data["recorded_at"]
                    recorded_at = datetime.fromisoformat(recorded_at_str)
                    
                    # 1. Map prefix to standard aggregate type
                    prefix = stream_id.split("-")[0]
                    map = {
                        "loan": "LoanApplication",
                        "docpkg": "DocumentPackage",
                        "agent": "AgentSession",
                        "credit": "CreditRecord",
                        "fraud": "FraudScreening",
                        "compliance": "ComplianceRecord"
                    }
                    agg_type = map.get(prefix, prefix.capitalize())
                    
                    await conn.execute(
                        """
                        INSERT INTO event_streams (stream_id, aggregate_type, current_version)
                        VALUES ($1, $2, 0)
                        ON CONFLICT (stream_id) DO UPDATE SET aggregate_type = $2
                        """,
                        stream_id, agg_type
                    )
                    
                    # 2. Get current version for this stream
                    current_v = await conn.fetchval(
                        "SELECT current_version FROM event_streams WHERE stream_id = $1",
                        stream_id
                    )
                    
                    new_v = current_v + 1
                    
                    # 3. Insert event
                    await conn.execute(
                        """
                        INSERT INTO events (stream_id, stream_position, event_type, event_version, payload, recorded_at)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        """,
                        stream_id, new_v, event_type, data.get("event_version", 1), json.dumps(payload), recorded_at
                    )
                    
                    # 4. Update stream version
                    await conn.execute(
                        "UPDATE event_streams SET current_version = $1 WHERE stream_id = $2",
                        new_v, stream_id
                    )
                    
                    count += 1
                    if count % 100 == 0:
                        print(f"[*] Ingested {count} events...")

    await db.disconnect()
    print(f"[✓] Successfully ingested {count} events into Supabase.")

if __name__ == "__main__":
    asyncio.run(ingest_data())
