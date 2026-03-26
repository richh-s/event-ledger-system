import pytest
import asyncio
from uuid import uuid4
from datetime import datetime
from src.database import Database
from src.event_store import EventStore
from src.models.events import StoredEvent
from src.projections.daemon import ProjectionDaemon
from src.projections.base import BaseProjection

class MockPassingProjection(BaseProjection):
    def __init__(self, name=None):
        self._name = name or f"mock_pass_{uuid4().hex[:6]}"
    @property
    def projection_name(self) -> str:
        return self._name
    async def handle_event(self, conn, event: StoredEvent) -> None:
        pass

class MockFailingProjection(BaseProjection):
    def __init__(self, name=None):
        self._name = name or f"mock_fail_{uuid4().hex[:6]}"
    @property
    def projection_name(self) -> str:
        return self._name
    async def handle_event(self, conn, event: StoredEvent) -> None:
        raise ValueError("Simulated failure in projection logic!")

@pytest.mark.asyncio
async def test_daemon_batch_processing_and_checkpoints(db):
    store = EventStore(db)
    proj = MockPassingProjection()
    # High batch size and low poll interval to catch up quickly
    daemon = ProjectionDaemon(db, store, [proj], batch_size=1000, poll_interval_ms=10)
    
    # Pre-populate with events and get their positions
    async with db.transaction() as conn:
        for i in range(1, 4):
            # direct insert for speed
            await conn.execute(
                """
                INSERT INTO events (event_id, stream_id, stream_position, event_type, payload, metadata)
                VALUES ($1, $2, $3, $4, '{}'::jsonb, '{}'::jsonb)
                """,
                uuid4(), f"stream-{uuid4().hex[:4]}", 1, "MockEvent"
            )
        max_pos = await conn.fetchval("SELECT MAX(global_position) FROM events")
            
    # Run daemon for a bit longer to ensure it picks up events
    await daemon.start()
    await asyncio.sleep(1.0) 
    await daemon.stop()
    
    async with db.get_connection() as conn:
        pos = await proj.get_last_position(conn)
        # It should have caught up to at least max_pos
        assert pos >= max_pos

@pytest.mark.asyncio
async def test_daemon_fault_isolation_and_dlq(db):
    store = EventStore(db)
    fail_proj = MockFailingProjection()
    pass_proj = MockPassingProjection()
    daemon = ProjectionDaemon(db, store, [fail_proj, pass_proj], batch_size=1000, poll_interval_ms=10)
    
    # Append failing event
    async with db.transaction() as conn:
        await conn.execute(
            """
            INSERT INTO events (event_id, stream_id, stream_position, event_type, payload, metadata)
            VALUES ($1, $2, $3, $4, '{}'::jsonb, '{}'::jsonb)
            """,
            uuid4(), f"fail-{uuid4().hex[:4]}", 1, "FailEvent"
        )
        max_pos = await conn.fetchval("SELECT MAX(global_position) FROM events")
    
    await daemon.start()
    await asyncio.sleep(1.0)
    await daemon.stop()
    
    async with db.get_connection() as conn:
        # Pass proj should advance its own checkpoint, fail proj should too after logging to DLQ
        pass_pos = await pass_proj.get_last_position(conn)
        fail_pos = await fail_proj.get_last_position(conn)
        
        assert pass_pos >= max_pos
        assert fail_pos >= max_pos
        
        # Dead Letter Queue should have an entry for this specific failing projection
        dlq_rows = await conn.fetch(
            "SELECT * FROM dead_letter_queue WHERE projection_name = $1", 
            fail_proj.projection_name
        )
        assert len(dlq_rows) >= 1
        assert "Simulated failure" in dlq_rows[0]['error_message']
