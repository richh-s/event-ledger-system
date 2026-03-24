from __future__ import annotations
import asyncio
import logging
import typing
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
        while self._running:
            try:
                await self._process_projection_batch(proj)
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

    async def _process_projection_batch(self, proj: BaseProjection) -> None:
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

        async with self._db.transaction() as conn:
            current_checkpoint = last_pos
            for event in batch:
                failed = False
                error_msg = ""
                
                # 1. Attempt handle_event within a SAVEPOINT
                try:
                    async with conn.transaction():
                        await proj.handle_event(conn, event)
                        current_checkpoint = event.global_position
                except Exception as e:
                    failed = True
                    error_msg = str(e)
                    logger.error(f"[{proj.projection_name}] Handler failure at {event.global_position}: {error_msg}")

                # 2. If failed, log to DLQ within a FRESH savepoint
                if failed:
                    try:
                        async with conn.transaction():
                            await DeadLetterQueue.log_failure(conn, proj.projection_name, event, error_msg)
                            current_checkpoint = event.global_position
                    except Exception as dlq_e:
                        logger.critical(f"[{proj.projection_name}] FAILED TO LOG DLQ: {str(dlq_e)}")
            
            # Atomic checkpoint commit
            await proj.update_checkpoint(conn, current_checkpoint)

    async def get_lag(self, projection_name: str) -> int:
        async with self._db.get_connection() as conn:
            latest_pos = await conn.fetchval("SELECT MAX(global_position) FROM events") or 0
            current_pos = await self._projections[projection_name].get_last_position(conn)
            return max(0, latest_pos - current_pos)
