import uuid
from datetime import datetime
from typing import Any
from dataclasses import dataclass, field


@dataclass
class BaseEvent:
    """The base payload structure for appending new events."""
    event_type: str
    payload: dict[str, Any]
    event_version: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class StoredEvent:
    """The structure of an event loaded from the EventStore."""
    event_id: uuid.UUID
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int
    payload: dict[str, Any]
    metadata: dict[str, Any]
    recorded_at: datetime


@dataclass
class StreamMetadata:
    """Metadata about a specific aggregate stream."""
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: datetime | None
    metadata: dict[str, Any]
