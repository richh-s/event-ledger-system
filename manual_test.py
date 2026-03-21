import asyncio
import os
import urllib.parse
from src.database import Database
from src.event_store import EventStore
from src.models import BaseEvent
from src.outbox_publisher import OutboxPublisher
from src.exceptions import OptimisticConcurrencyError

DB_DSN = os.getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres")

class DummyEvent(BaseEvent):
    event_type: str = "Dummy"
    payload_data: dict = {}
 
    def to_payload(self) -> dict:
        return self.payload_data

async def run_occ_test(store: EventStore):
    print("\n==============================================")
    print("[*] TEST 1: Optimistic Concurrency Race Condition")
    print("==============================================")
    stream_id = "occ-race-stream"
    
    # Setup base stream natively
    try:
        await store.append(stream_id, [], expected_version=-1, aggregate_type="TestAgg")
    except OptimisticConcurrencyError:
        pass # Already exists from previous runs
        
    current_v = await store.stream_version(stream_id)
    
    print(f"    Current Version: {current_v}")
    print(f"    Spawning Agent A (expected={current_v}) and Agent B (expected={current_v}) simultaneously...")

    async def agent_task(agent_name):
        events = [DummyEvent(event_type=f"{agent_name}Action", payload_data={"data": "test"})]
        try:
            new_v = await store.append(stream_id, events, expected_version=current_v, aggregate_type="TestAgg")
            print(f"    -> [SUCCESS]  {agent_name} appended successfully! New Version: {new_v}")
        except OptimisticConcurrencyError as e:
            print(f"    -> [REJECTED] {agent_name} hit OCC collision! DB refused write. Error: {e}")

    # Launch both exact same millisecond
    await asyncio.gather(
        agent_task("Agent_A"),
        agent_task("Agent_B")
    )

async def test_connection(dsn: str) -> bool:
    parsed = urllib.parse.urlparse(dsn)
    pwd = parsed.password
    safe_dsn = dsn.replace(pwd, "*****") if pwd else dsn
    print(f"[*] Target DB: {safe_dsn}")
    db = Database(dsn)
    try:
        await db.connect()
        async with db.get_connection() as conn:
            version = await conn.fetchval("SELECT version();")
            print(f"[+] Connected to PostgreSQL {version.split()[1]}")
        await db.disconnect()
        return True
    except Exception as e:
        print(f"[!] Connection Validation Failed: {e}")
        return False

async def main():
    if not await test_connection(DB_DSN): return
    db = Database(DB_DSN)
    await db.connect()
    
    store = EventStore(db)
    
    # 1. OCC Race Test
    await run_occ_test(store)
    
    # 2. Outbox Publisher Demo
    print("\n==============================================")
    print("[*] TEST 2: Outbox Background Publisher Daemon")
    print("==============================================")
    publisher = OutboxPublisher(db)
    
    # Flush the backlog of dual-written events from our previous test runs!
    processed = await publisher.publish_pending(batch_size=10)
    print(f"    [+] Successfully dispatched and flagged {processed} pending dual-written events!")
    
    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
