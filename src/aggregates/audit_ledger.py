from typing import Any
from src.aggregates.base import BaseAggregate
from src.models.events import (
    BaseEvent,
    AuditIntegrityCheckRun,
    DomainError,
)

class AuditLedgerAggregate(BaseAggregate):
    """
    Sub for cross-stream audit trail tracking.
    """
    def __init__(self, stream_id: str):
        super().__init__(stream_id)

    def append_integrity_check(self, event: AuditIntegrityCheckRun) -> None:
        self.append_event(event)

    def apply_AuditIntegrityCheckRun(self, event: AuditIntegrityCheckRun) -> None:
        pass

    def get_state(self) -> dict[str, Any]:
        return {"version": self.version}

    def restore_state(self, state: dict[str, Any]) -> None:
        self.version = state.get("version", 0)
