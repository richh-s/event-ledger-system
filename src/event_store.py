import uuid
from typing import AsyncIterator, Callable

import asyncpg

from .models import BaseEvent, StoredEvent, StreamMetadata
from .database import Database
from .exceptions import OptimisticConcurrencyError, StreamArchivedError


from src.upcasting.registry import registry
from src.upcasting.upcasters import initialize_upcasters # Ensures upcasters register

# Auto-initialize on import
initialize_upcasters()


class EventStore:
    """
    The sole source of truth; all system state must be derived from events.
    This EventStore forms the write model in a CQRS architecture.
    """

    def __init__(self, db: Database):
        self.db = db

    def _apply_upcasters(self, event: StoredEvent) -> StoredEvent:
        """Phase 4: Uses the centralized UpcasterRegistry to evolve event schemas."""
        return registry.upcast(event)

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        aggregate_type: str | None = None,  # Required when creating a new stream
    ) -> list[StoredEvent]:
        """
        Atomically appends events to stream_id natively supporting batched insertion
        for high-frequency streams like AgentSession.
        Raises OptimisticConcurrencyError if stream version != expected_version.
        Writes to outbox in same transaction.
        """
        if not events:
            return []

        inserted_events: list[StoredEvent] = []
        async with self.db.transaction() as conn:
            # 1. Check stream version
            row = await conn.fetchrow(
                "SELECT current_version, archived_at, aggregate_type FROM event_streams WHERE stream_id = $1 FOR UPDATE",
                stream_id
            )

            current_version = 0
            if row:
                if expected_version == -1:
                    raise OptimisticConcurrencyError(stream_id, expected_version, row['current_version'])
                if row['archived_at'] is not None:
                    raise StreamArchivedError(stream_id)
                current_version = row['current_version']
            else:
                if expected_version != -1:
                    raise OptimisticConcurrencyError(stream_id, expected_version, 0)

            if expected_version != -1 and current_version != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current_version)

            new_version = current_version
            if not row:
                if aggregate_type is None:
                    raise ValueError("aggregate_type must be provided explicitly when creating a new stream")
                try:
                    await conn.execute(
                        "INSERT INTO event_streams (stream_id, aggregate_type, current_version) VALUES ($1, $2, $3)",
                        stream_id, aggregate_type, new_version
                    )
                except asyncpg.exceptions.UniqueViolationError:
                    raise OptimisticConcurrencyError(stream_id, expected_version, -1)
            
            # 2. Insert all events
            for base_event in events:
                new_version += 1
                
                metadata = dict(base_event.metadata or {})
                if correlation_id:
                    metadata["correlation_id"] = correlation_id
                if causation_id:
                    metadata["causation_id"] = causation_id

                event_id = uuid.uuid4()
                
                # Insert and get global_position
                row = await conn.fetchrow(
                    """
                    INSERT INTO events 
                    (event_id, stream_id, stream_position, event_type, event_version, payload, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING global_position, recorded_at
                    """,
                    event_id, stream_id, new_version, base_event.event_type, 
                    base_event.event_version, base_event.to_payload(), metadata
                )

                # 3. Create StoredEvent for return
                stored_event = StoredEvent(
                    event_id=event_id,
                    stream_id=stream_id,
                    stream_position=new_version,
                    global_position=row['global_position'],
                    event_type=base_event.event_type,
                    event_version=base_event.event_version,
                    payload=base_event.to_payload(),
                    metadata=metadata,
                    recorded_at=row['recorded_at']
                )
                inserted_events.append(stored_event)

                # 4. Insert outbox rows
                outbox_payload = {
                    "stream_id": stream_id,
                    "event_type": base_event.event_type,
                    "event_version": base_event.event_version,
                    "payload": base_event.to_payload(),
                    "metadata": metadata
                }
                await conn.execute(
                    """
                    INSERT INTO outbox (event_id, destination, payload)
                    VALUES ($1, $2, $3)
                    """,
                    event_id, "event_bus", outbox_payload
                )

            # 5. Update stream version (once after batch)
            await conn.execute(
                "UPDATE event_streams SET current_version = $1 WHERE stream_id = $2",
                new_version, stream_id
            )

        return inserted_events

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        query = "SELECT * FROM events WHERE stream_id = $1 AND stream_position >= $2"
        args = [stream_id, from_position]
        
        if to_position is not None:
            query += " AND stream_position <= $3"
            args.append(to_position)
            
        query += " ORDER BY stream_position ASC"
        
        async with self.db.get_connection() as conn:
            rows = await conn.fetch(query, *args)
            
        events: list[StoredEvent] = []
        for row in rows:
            event = StoredEvent(
                event_id=row['event_id'],
                stream_id=row['stream_id'],
                stream_position=row['stream_position'],
                global_position=row['global_position'],
                event_type=row['event_type'],
                event_version=row['event_version'],
                payload=row['payload'],
                metadata=row['metadata'],
                recorded_at=row['recorded_at']
            )
            events.append(self._apply_upcasters(event))
            
        return events

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
    ) -> AsyncIterator[StoredEvent]:
        next_global_position = from_global_position
        
        while True:
            # Reacquire connection per batch to avoid holding tight over yields
            async with self.db.get_connection() as conn:
                if event_types:
                    batch_query = (
                        "SELECT * FROM events "
                        "WHERE global_position >= $1 "
                        "AND event_type = ANY($2) "
                        "ORDER BY global_position ASC "
                        f"LIMIT {batch_size}"
                    )
                    rows = await conn.fetch(batch_query, next_global_position, event_types)
                else:
                    batch_query = (
                        "SELECT * FROM events "
                        "WHERE global_position >= $1 "
                        "ORDER BY global_position ASC "
                        f"LIMIT {batch_size}"
                    )
                    rows = await conn.fetch(batch_query, next_global_position)
                    
                if not rows:
                    break
                    
                batch = []
                import json
                for row in rows:
                    p = row['payload']
                    if isinstance(p, (str, bytes)):
                        p = json.loads(p)
                    
                    m = row['metadata']
                    if isinstance(m, (str, bytes)):
                        m = json.loads(m)

                    event = StoredEvent(
                        event_id=row['event_id'],
                        stream_id=row['stream_id'],
                        stream_position=row['stream_position'],
                        global_position=row['global_position'],
                        event_type=row['event_type'],
                        event_version=row['event_version'],
                        payload=p,
                        metadata=m,
                        recorded_at=row['recorded_at']
                    )
                    batch.append(self._apply_upcasters(event))
                    next_global_position = (event.global_position or 0) + 1
            
            # Yield outside the connection block
            for event in batch:
                yield event

    async def stream_version(self, stream_id: str) -> int:
        async with self.db.get_connection() as conn:
            val = await conn.fetchval("SELECT current_version FROM event_streams WHERE stream_id = $1", stream_id)
            if val is None:
                return -1
            return int(val)  # type: ignore
        return -1  # unreachable; satisfies type checker

    async def archive_stream(self, stream_id: str) -> None:
        async with self.db.get_connection() as conn:
            await conn.execute("UPDATE event_streams SET archived_at = NOW() WHERE stream_id = $1", stream_id)

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        async with self.db.get_connection() as conn:
            row = await conn.fetchrow("SELECT * FROM event_streams WHERE stream_id = $1", stream_id)
            if not row:
                raise ValueError(f"Stream {stream_id} not found")
            return StreamMetadata(
                stream_id=row['stream_id'],
                aggregate_type=row['aggregate_type'],
                current_version=row['current_version'],
                created_at=row['created_at'],
                archived_at=row['archived_at'],
                metadata=row['metadata']
            )
