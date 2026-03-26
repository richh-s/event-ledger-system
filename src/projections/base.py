from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any
from src.models.events import StoredEvent

class BaseProjection(ABC):
    """
    Abstract Base Class for all projections.
    Each projection implements its own idempotent handler and checkpoint management.
    """

    @property
    @abstractmethod
    def projection_name(self) -> str:
        """Unique identifier for this projection in the checkpoints table."""
        pass

    @abstractmethod
    async def handle_event(self, conn, event: StoredEvent) -> None:
        """
        Processes a single event within an existing database transaction.
        Implementation must be idempotent.
        """
        pass

    async def get_last_position(self, conn) -> int:
        """Retrieves the last processed global_position for this projection."""
        row = await conn.fetchrow(
            "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
            self.projection_name
        )
        if not row:
            # Initialize checkpoint if missing — ON CONFLICT handles concurrent tasks
            await conn.execute(
                "INSERT INTO projection_checkpoints (projection_name, last_position) VALUES ($1, 0) ON CONFLICT DO NOTHING",
                self.projection_name
            )
            return 0
        return row['last_position']

    async def update_checkpoint(self, conn, position: int) -> None:
        """Atomically updates the checkpoint for this projection."""
        await conn.execute(
            "UPDATE projection_checkpoints SET last_position = $1, updated_at = NOW() WHERE projection_name = $2",
            position, self.projection_name
        )
