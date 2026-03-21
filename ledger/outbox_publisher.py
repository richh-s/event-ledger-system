import asyncio
from ledger.database import Database

class OutboxPublisher:
    """
    Background daemon responsible for claiming outbox rows transactionally
    and flushing them to external infrastructure reliably (At-Least-Once delivery).
    """

    def __init__(self, db: Database):
        self.db = db
        
    async def publish_pending(self, batch_size: int = 50) -> int:
        """Claims a batch of outbox messages using SKIP LOCKED and marks them published."""
        async with self.db.transaction() as conn:
            # 1. Claim rows exclusively skipping locked rows (safe for horizontal scaling)
            rows = await conn.fetch(
                """
                SELECT id, payload, destination 
                FROM outbox 
                WHERE published_at IS NULL 
                ORDER BY created_at ASC 
                FOR UPDATE SKIP LOCKED 
                LIMIT $1
                """, batch_size
            )
            
            for row in rows:
                # 2. Simulate external dispatch (e.g. producing to Kafka/RabbitMQ)
                msg_id = row['id']
                dest = row['destination']
                event_type = row['payload'].get("event_type", "Unknown")
                
                print(f"    [Outbox Daemon] Dispatching {event_type} to {dest} (msg_id: {msg_id})")
                await asyncio.sleep(0.01) # Simulating network IO
                
                # 3. Mark as successfully published to prevent duplicate dispatch
                await conn.execute(
                    "UPDATE outbox SET published_at = NOW() WHERE id = $1",
                    msg_id
                )
                
            return len(rows)
            
    async def start_polling(self, poll_interval: float = 1.0):
        """Infinite loop polling the active outbox queue."""
        print(f"[*] Outbox Publisher Daemon Started (Polling every {poll_interval}s)")
        try:
            while True:
                processed = await self.publish_pending()
                if processed == 0:
                    await asyncio.sleep(poll_interval)
        except asyncio.CancelledError:
            print("[*] Outbox Publisher Daemon Stopped cleanly.")
