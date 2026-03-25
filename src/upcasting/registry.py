from __future__ import annotations
import logging
from typing import Callable, Dict, Tuple, List
from src.models.events import StoredEvent

logger = logging.getLogger(__name__)

# Type alias for an upcaster function: (payload, metadata) -> (new_payload, new_metadata)
UpcasterFunc = Callable[[dict, dict], Tuple[dict, dict]]

class UpcasterRegistry:
    """
    A pure, read-only registry for event upcasters.
    Applied during event loading to evolve schemas without mutating the store.
    """
    def __init__(self):
        # Maps (event_type, from_version) -> upcaster_function
        self._upcasters: Dict[Tuple[str, int], UpcasterFunc] = {}

    def register(self, event_type: str, from_version: int):
        """Decorator to register an upcaster function."""
        def decorator(func: UpcasterFunc):
            self._upcasters[(event_type, from_version)] = func
            return func
        return decorator

    def upcast(self, event: StoredEvent) -> StoredEvent:
        """
        Recursively applies all relevant upcasters to an event.
        Returns a NEW StoredEvent instance with updated payload/metadata.
        """
        current_type = event.event_type
        current_version = event.metadata.get("version", 1) if event.metadata else 1
        current_payload = dict(event.payload)
        current_metadata = dict(event.metadata or {})

        while (current_type, current_version) in self._upcasters:
            upcaster = self._upcasters[(current_type, current_version)]
            try:
                # Add recorded_at to metadata for upcaster context
                current_metadata["recorded_at"] = event.recorded_at
                current_payload, current_metadata = upcaster(current_payload, current_metadata)
                # Increment version explicitly in metadata
                current_version += 1
                current_metadata["version"] = current_version
                logger.debug(f"Upcasted {current_type} to version {current_version}")
            except Exception as e:
                logger.error(f"Failed to upcast {current_type} v{current_version}: {e}")
                break

        # Return a copy with upcasted data
        return StoredEvent(
            event_id=event.event_id,
            event_type=current_type,
            event_version=current_version,
            payload=current_payload,
            metadata=current_metadata,
            recorded_at=event.recorded_at,
            global_position=event.global_position,
            stream_id=event.stream_id,
            stream_position=event.stream_position
        )

# Global registry instance
registry = UpcasterRegistry()
