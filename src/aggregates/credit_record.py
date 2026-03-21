from typing import Any
from src.aggregates.base import BaseAggregate
from src.models.events import (
    BaseEvent,
    CreditRecordOpened,
    HistoricalProfileConsumed,
    ExtractedFactsConsumed,
    CreditAnalysisCompleted,
    CreditAnalysisDeferred,
    DomainError,
)

class CreditRecordAggregate(BaseAggregate):
    """
    Tracks credit results.
    Rule: Single credit analysis per application (no duplicates).
    Prevents duplicate CreditAnalysisCompleted events.
    """
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.is_opened = False
        self.has_analysis = False
        self.is_deferred = False

    def assert_no_analysis_exists(self) -> None:
        if self.has_analysis:
            raise DomainError(f"Credit record {self.stream_id} already has a completed analysis.")

    def apply_CreditRecordOpened(self, event: CreditRecordOpened) -> None:
        self.is_opened = True

    def apply_CreditAnalysisCompleted(self, event: CreditAnalysisCompleted) -> None:
        self.has_analysis = True

    def apply_CreditAnalysisDeferred(self, event: CreditAnalysisDeferred) -> None:
        self.is_deferred = True

    def get_state(self) -> dict[str, Any]:
        return {
            "is_opened": self.is_opened,
            "has_analysis": self.has_analysis,
            "is_deferred": self.is_deferred,
            "version": self.version,
        }

    def restore_state(self, state: dict[str, Any]) -> None:
        self.is_opened = state.get("is_opened", False)
        self.has_analysis = state.get("has_analysis", False)
        self.is_deferred = state.get("is_deferred", False)
        self.version = state.get("version", 0)

    # Command methods
    def open_record(self, event: CreditRecordOpened) -> None:
        if self.is_opened:
            raise DomainError(f"Credit record {self.stream_id} already opened.")
        self.append_event(event)

    def consume_historical_profile(self, event: HistoricalProfileConsumed) -> None:
        if not self.is_opened:
            raise DomainError(f"Credit record {self.stream_id} is not opened.")
        self.append_event(event)

    def consume_extracted_facts(self, event: ExtractedFactsConsumed) -> None:
        if not self.is_opened:
            raise DomainError(f"Credit record {self.stream_id} is not opened.")
        self.append_event(event)

    def complete_analysis(self, event: CreditAnalysisCompleted) -> None:
        if not self.is_opened:
            raise DomainError(f"Credit record {self.stream_id} is not opened.")
        self.assert_no_analysis_exists()
        
        # Rule 7 implies confidence capping based on quality but wait, the command handler caps it or we cap it here?
        # The prompt says: "Credit analysis caps confidence based on quality ... caps confidence per challenge spec ... DocumentPackageAggregate + CreditRecordAggregate"
        # We enforce it here if we want, but since confidence is passed in the event payload, 
        # the aggregate mainly ensures state constraints. The handler must provide the valid capped event, 
        # or the aggregate can validate the cap against some quality flag state if passed in.
        
        self.append_event(event)

    def defer_analysis(self, event: CreditAnalysisDeferred) -> None:
        if not self.is_opened:
            raise DomainError(f"Credit record {self.stream_id} is not opened.")
        self.append_event(event)
