import json
from typing import TypeVar, Type

from src.event_store import EventStore
from src.aggregates.base import BaseAggregate
from src.database import Database

T = TypeVar('T', bound=BaseAggregate)

class AggregateRepository:
    def __init__(self, event_store: EventStore, db: Database, snapshot_threshold: int = 100):
        self.event_store = event_store
        self.db = db
        self.snapshot_threshold = snapshot_threshold

    async def load(self, aggregate_cls: Type[T], stream_id: str) -> T:
        """
        Loads an aggregate, applying snapshot if available, then remaining events.
        """
        aggregate = aggregate_cls(stream_id)
        
        # 1. Try to load latest snapshot from DB
        snapshot_version = 0
        async with self.db.get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT version, state FROM snapshots WHERE stream_id = $1 ORDER BY version DESC LIMIT 1",
                stream_id
            )
            if row:
                snapshot_version = row["version"]
                aggregate.restore_state(json.loads(row["state"]))
                aggregate.version = snapshot_version

        # 2. Load events after snapshot
        events = await self.event_store.load_stream(stream_id, from_position=snapshot_version + 1)
        
        # 3. Apply to aggregate
        aggregate.load_from_history(events)

        return aggregate

    async def save(
        self, 
        aggregate: BaseAggregate, 
        aggregate_type: str,
        correlation_id: str | None = None,
        causation_id: str | None = None
    ) -> None:
        """
        Saves uncommitted events and optionally creates a snapshot.
        """
        if not aggregate.uncommitted_events:
            return

        expected_version = aggregate.version
        if expected_version == 0:
            expected_version = -1
        new_version = await self.event_store.append(
            stream_id=aggregate.stream_id,
            events=aggregate.uncommitted_events,
            expected_version=expected_version if expected_version > 0 else -1,
            aggregate_type=aggregate_type,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        
        # Optionally create a snapshot if threshold is crossed
        if new_version // self.snapshot_threshold > expected_version // self.snapshot_threshold:
            state_json = json.dumps(aggregate.get_state())
            async with self.db.transaction() as conn:
                await conn.execute(
                    "INSERT INTO snapshots (stream_id, aggregate_type, version, state) VALUES ($1, $2, $3, $4) "
                    "ON CONFLICT (stream_id, version) DO NOTHING",
                    aggregate.stream_id, aggregate_type, new_version, state_json
                )

        aggregate.uncommitted_events.clear()
        aggregate.version = new_version
