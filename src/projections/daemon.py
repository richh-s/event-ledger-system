from __future__ import annotations
import asyncio
import logging
import typing
import uuid
from datetime import datetime, timedelta, timezone
from src.database import Database
from src.event_store import EventStore
from src.models.events import StoredEvent
from src.projections.base import BaseProjection
from src.projections.dead_letter import DeadLetterQueue

logger = logging.getLogger(__name__)

class ProjectionDaemon:
    def __init__(
        self, 
        db: Database, 
        store: EventStore, 
        projections: list[BaseProjection], 
        batch_size: int = 500,
        poll_interval_ms: int = 100
    ):
        self._db = db
        self._store = store
        self._projections = {p.projection_name: p for p in projections}
        self._batch_size = batch_size
        self._poll_interval = poll_interval_ms / 1000.0
        self._running = False
        self._tasks: list[asyncio.Task] = []

    async def start(self):
        self._running = True
        for proj in self._projections.values():
            task = asyncio.create_task(self._run_projection_loop(proj))
            self._tasks.append(task)
        logger.info(f"Daemon started with {len(self._tasks)} tasks.")

    async def stop(self):
        """Graceful shutdown: signal loops to stop, then wait for completion."""
        self._running = False
        # Wait for all tasks to finish their current batch (no cancel!)
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []

    async def _run_projection_loop(self, proj: BaseProjection) -> None:
        name = proj.projection_name
        node_id = f"node-{uuid.uuid4().hex[:8]}"
        
        while self._running:
            try:
                # 1. Attempt to acquire advisory lock and lease
                acquired = await self._try_acquire_lease(proj, node_id)
                if not acquired:
                    await asyncio.sleep(2.0) # Check later
                    continue

                # 2. Process batch
                await self._process_projection_batch(proj, node_id)
                
                # Use short sleeps so we detect _running=False quickly
                for _ in range(max(1, int(self._poll_interval * 20))):
                    if not self._running:
                        break
                    await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{name}] Daemon loop error: {str(e)}", exc_info=True)
                await asyncio.sleep(2.0)

    async def _try_acquire_lease(self, proj: BaseProjection, node_id: str) -> bool:
        """Advisory lock + Lease ownership logic."""
        projection_name = proj.projection_name
        # Simple hash-based lock key from name
        lock_key = hash(projection_name) & 0x7FFFFFFF
        
        async with self._db.transaction() as conn:
            # Try acquire transaction-level advisory lock
            got_lock = await conn.fetchval("SELECT pg_try_advisory_xact_lock($1)", lock_key)
            if not got_lock:
                return False
            
            now = datetime.now(timezone.utc)
            lease_duration = timedelta(seconds=30)
            
            # Use UPSERT for atomic lease acquisition/renewal
            # We take the lease if it's expired OR if we already own it
            await conn.execute(
                """
                INSERT INTO projection_checkpoints 
                    (projection_name, last_position, owner_node_id, lease_expires_at, updated_at)
                VALUES 
                    ($1, 0, $2, NOW() + interval '30 seconds', NOW())
                ON CONFLICT (projection_name) 
                DO UPDATE SET 
                    owner_node_id = EXCLUDED.owner_node_id,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    updated_at = EXCLUDED.updated_at
                WHERE 
                    projection_checkpoints.owner_node_id IS NULL OR 
                    projection_checkpoints.owner_node_id = $2 OR 
                    projection_checkpoints.lease_expires_at < NOW()
                """,
                projection_name, node_id
            )
            
            # Verify we actually got the lease (the WHERE clause might have filtered the update)
            is_owner = await conn.fetchval(
                "SELECT 1 FROM projection_checkpoints WHERE projection_name = $1 AND owner_node_id = $2 AND lease_expires_at > NOW()",
                projection_name, node_id
            )
            return bool(is_owner)
            
            return True

    async def _process_projection_batch(self, proj: BaseProjection, node_id: str) -> None:
        async with self._db.get_connection() as conn:
            last_pos = await proj.get_last_position(conn)

        batch = []
        async for event in self._store.load_all(
            from_global_position=last_pos + 1,
            batch_size=self._batch_size
        ):
            batch.append(event)
            if len(batch) >= self._batch_size:
                break
        
        if not batch:
            return

        # Simple lock check or re-acquire before transaction
        async with self._db.transaction() as conn:
            # Re-verify lease ownership within the transaction
            lock_key = hash(proj.projection_name) & 0x7FFFFFFF
            got_lock = await conn.fetchval("SELECT pg_try_advisory_xact_lock($1)", lock_key)
            if not got_lock:
                logger.warning(f"[{proj.projection_name}] Lost lock during batch, skipping.")
                return
            
            is_owner = await conn.fetchval(
                "SELECT 1 FROM projection_checkpoints WHERE projection_name = $1 AND owner_node_id = $2 AND lease_expires_at > NOW()",
                proj.projection_name, node_id
            )
            if not is_owner:
                logger.warning(f"[{proj.projection_name}] Lease expired or stolen, skipping batch.")
                return

            current_checkpoint = last_pos
            for event in batch:
                failed = False
                error_msg = ""
                
                # 1. Attempt handle_event
                try:
                    await proj.handle_event(conn, event)
                    current_checkpoint = event.global_position
                except Exception as e:
                    failed = True
                    error_msg = str(e)
                    logger.error(f"[{proj.projection_name}] Handler failure at {event.global_position}: {error_msg}")

                # 2. If failed, log to DLQ
                if failed:
                    try:
                        await DeadLetterQueue.log_failure(conn, proj.projection_name, event, error_msg)
                        current_checkpoint = event.global_position
                    except Exception as dlq_e:
                        logger.critical(f"[{proj.projection_name}] FAILED TO LOG DLQ: {str(dlq_e)}")
            
            # Atomic checkpoint commit + lease renewal using DB time
            await conn.execute(
                "UPDATE projection_checkpoints "
                "SET last_position = $1, lease_expires_at = NOW() + interval '30 seconds', updated_at = NOW() "
                "WHERE projection_name = $2 AND owner_node_id = $3",
                current_checkpoint, proj.projection_name, node_id
            )

    async def get_lag(self, projection_name: str) -> int:
        async with self._db.get_connection() as conn:
            latest_pos = await conn.fetchval("SELECT MAX(global_position) FROM events") or 0
            current_pos = await self._projections[projection_name].get_last_position(conn)
            return max(0, latest_pos - current_pos)
