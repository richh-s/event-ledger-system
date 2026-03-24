from __future__ import annotations
from datetime import datetime
from src.schema.events import StoredEvent

class DeadLetterQueue:
    """Handles logging of failed event processing within a projection."""

    def __init__(self, db):
        self.db = db

    @staticmethod
    async def log_failure(
        conn,
        projection_name: str,
        event: StoredEvent,
        error_message: str,
        retry_count: int = 0
    ) -> int:
        """Logs a failure to the dead_letter_queue table using an existing connection."""
        import json
        dead_letter_id = await conn.fetchval(
            """
            INSERT INTO dead_letter_queue 
            (projection_name, event_id, global_position, event_type, payload, error_message, retry_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            """,
            projection_name,
            event.event_id,
            event.global_position,
            event.event_type,
            json.dumps(event.payload),
            error_message,
            retry_count
        )
        return int(dead_letter_id)

    async def record_failure(
        self,
        projection_name: str,
        event: StoredEvent,
        error_message: str,
        retry_count: int = 0
    ) -> int:
        """Logs a failure to the dead_letter_queue table (compatible instance method)."""
        async with self.db.get_connection() as conn:
            return await self.log_failure(conn, projection_name, event, error_message, retry_count)

    async def get_failures(self, projection_name: str | None = None) -> list[dict]:
        """Retrieves failures for inspection."""
        query = "SELECT * FROM dead_letter_queue"
        args = []
        if projection_name:
            query += " WHERE projection_name = $1"
            args.append(projection_name)
        
        query += " ORDER BY failed_at DESC"
        
        async with self.db.get_connection() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(r) for r in rows]

    async def get_count(self) -> int:
        """Returns total count of dead letters for health monitoring."""
        async with self.db.get_connection() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM dead_letter_queue")
            return int(count or 0)
