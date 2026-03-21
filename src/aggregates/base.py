from typing import Any, TypeVar, Type, cast
from abc import ABC

from src.models.events import BaseEvent, StoredEvent, EVENT_REGISTRY

T = TypeVar('T', bound='BaseAggregate')

class BaseAggregate(ABC):
    """
    Base class for all aggregates.
    """
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.version = 0
        self.uncommitted_events: list[BaseEvent] = []

    def load_from_history(self, events: list[StoredEvent]) -> None:
        """
        Loads the aggregate from a list of historical events.
        """
        for event in events:
            self.version = event.stream_position
            domain_event = self._reconstruct_event(event)
            if domain_event:
                self._apply_event(domain_event)

    def _reconstruct_event(self, stored_event: StoredEvent) -> BaseEvent | None:
        cls = EVENT_REGISTRY.get(stored_event.event_type)
        if not cls:
            return None
        
        # Merge payload with base fields
        data = dict(stored_event.payload)
        data['event_id'] = stored_event.event_id
        data['recorded_at'] = stored_event.recorded_at
        data['metadata'] = stored_event.metadata
        return cls(**data)

    def append_event(self, event: BaseEvent) -> None:
        """
        Records a new event, applies it to current state, and adds to uncommitted list.
        """
        self._apply_event(event)
        self.uncommitted_events.append(event)

    def _apply_event(self, event: BaseEvent) -> None:
        """
        Internal dispatcher. Must be implemented via specific apply_<event_type> methods on the aggregate.
        """
        handler_name = f"apply_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler:
            handler(event)

    def get_state(self) -> dict[str, Any]:
        """
        Returns the serializable state of the aggregate for snapshotting.
        Override in subclasses.
        """
        return {}

    def restore_state(self, state: dict[str, Any]) -> None:
        """
        Restores the aggregate state from a snapshot.
        Override in subclasses.
        """
        pass
