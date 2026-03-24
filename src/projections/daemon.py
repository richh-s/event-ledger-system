from __future__ import annotations
import asyncio
import logging
import typing
import uuid
from datetime import datetime, timedelta, timezone
from src.database import Database
from src.event_store import EventStore
from src.schema.events import StoredEvent
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
            
            # Check lease in DB
            now = datetime.now(timezone.utc)
            lease_duration = timedelta(seconds=30)
            
            row = await conn.fetchrow(
                "SELECT owner_node_id, lease_expires_at FROM projection_checkpoints WHERE projection_name = $1 FOR UPDATE",
                projection_name
            )
            
            if row:
                owner = row['owner_node_id']
                expires = row['lease_expires_at']
                
                if owner and owner != node_id and expires and expires > now:
                    # Owned by someone else and active
                    return False
                
                # Take or renew lease
                await conn.execute(
                    "UPDATE projection_checkpoints SET owner_node_id = $1, lease_expires_at = $2, updated_at = $3 WHERE projection_name = $4",
                    node_id, now + lease_duration, now, projection_name
                )
            else:
                # Create initial checkpoint with lease
                await conn.execute(
                    "INSERT INTO projection_checkpoints (projection_name, last_position, owner_node_id, lease_expires_at, updated_at) VALUES ($1, 0, $2, $3, $4)",
                    projection_name, node_id, now + lease_duration, now
                )
            
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
            
            # Atomic checkpoint commit + lease renewal
            now = datetime.now(timezone.utc)
            await conn.execute(
                "UPDATE projection_checkpoints SET last_position = $1, lease_expires_at = $2, updated_at = $3 WHERE projection_name = $4 AND owner_node_id = $5",
                current_checkpoint, now + timedelta(seconds=30), now, proj.projection_name, node_id
            )

    async def get_lag(self, projection_name: str) -> int:
        async with self._db.get_connection() as conn:
            latest_pos = await conn.fetchval("SELECT MAX(global_position) FROM events") or 0
            current_pos = await self._projections[projection_name].get_last_position(conn)
            return max(0, latest_pos - current_pos)
