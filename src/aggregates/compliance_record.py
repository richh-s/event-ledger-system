from typing import Any
from src.aggregates.base import BaseAggregate
from src.models.events import (
    BaseEvent,
    ComplianceCheckInitiated,
    ComplianceRulePassed,
    ComplianceRuleFailed,
    ComplianceRuleNoted,
    ComplianceCheckCompleted,
    DomainError,
)

class ComplianceRecordAggregate(BaseAggregate):
    """
    Tracks compliance check results.
    Rule: Compliance hard block stops evaluation. 
    (Track hard_block_encountered, conditional edges stop rules)
    """
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.is_initiated = False
        self.is_completed = False
        self.hard_block_encountered = False

    def apply_ComplianceCheckInitiated(self, event: ComplianceCheckInitiated) -> None:
        self.is_initiated = True

    def apply_ComplianceRulePassed(self, event: ComplianceRulePassed) -> None:
        pass

    def apply_ComplianceRuleFailed(self, event: ComplianceRuleFailed) -> None:
        if event.is_hard_block:
            self.hard_block_encountered = True

    def apply_ComplianceRuleNoted(self, event: ComplianceRuleNoted) -> None:
        pass

    def apply_ComplianceCheckCompleted(self, event: ComplianceCheckCompleted) -> None:
        self.is_completed = True

    def get_state(self) -> dict[str, Any]:
        return {
            "is_initiated": self.is_initiated,
            "is_completed": self.is_completed,
            "hard_block_encountered": self.hard_block_encountered,
            "version": self.version,
        }

    def restore_state(self, state: dict[str, Any]) -> None:
        self.is_initiated = state.get("is_initiated", False)
        self.is_completed = state.get("is_completed", False)
        self.hard_block_encountered = state.get("hard_block_encountered", False)
        self.version = state.get("version", 0)

    # Command methods
    def initiate_check(self, event: ComplianceCheckInitiated) -> None:
        if self.is_initiated:
            raise DomainError(f"Compliance check {self.stream_id} already initiated.")
        self.append_event(event)

    def record_rule_passed(self, event: ComplianceRulePassed) -> None:
        self._assert_evaluable()
        self.append_event(event)

    def record_rule_failed(self, event: ComplianceRuleFailed) -> None:
        self._assert_evaluable()
        self.append_event(event)

    def record_rule_noted(self, event: ComplianceRuleNoted) -> None:
        self._assert_evaluable()
        self.append_event(event)

    def complete_check(self, event: ComplianceCheckCompleted) -> None:
        if not self.is_initiated:
            raise DomainError(f"Compliance check {self.stream_id} not initiated.")
        if self.is_completed:
            raise DomainError(f"Compliance check {self.stream_id} already completed.")
        self.append_event(event)

    def _assert_evaluable(self) -> None:
        if not self.is_initiated:
            raise DomainError(f"Compliance check {self.stream_id} not initiated.")
        if self.is_completed:
            raise DomainError(f"Compliance check {self.stream_id} already completed.")
        if self.hard_block_encountered:
            # Rule 5: No error - evaluation stops.
            # But the agent shouldn't send evaluating events if we're hard blocked.
            # We raise an exception here so the command handler can catch it or prevent it.
            raise DomainError(f"Compliance check {self.stream_id} encountered a hard block. No further rules can be evaluated.")
