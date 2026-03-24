from typing import Any, Set
from src.aggregates.base import BaseAggregate, ReconstructionStatus
from src.models.events import (
    BaseEvent,
    AgentSessionStarted,
    AgentNodeExecuted,
    AgentToolCalled,
    AgentContextLoaded,
    AgentOutputWritten,
    AgentSessionCompleted,
    AgentSessionFailed,
    AgentSessionRecovered,
    DomainError,
)


class PreconditionFailedError(DomainError):
    pass

class AgentSessionAggregate(BaseAggregate):
    """
    Enforces Gas Town memory and execution tracking constraints.
    Rule: AgentSessionStarted must be the first event before ANY work (AgentNodeExecuted, etc.).
    Rule: AgentContextLoaded MUST happen before AgentOutputWritten.
    Rule: AgentOutputWritten MUST reference a valid loaded context ID.
    """
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.is_started = False
        self.loaded_context_ids: Set[str] = set()
        self.is_completed = False
        self.is_failed = False
        self.nodes_executed = 0
        self.tool_calls = 0
        self.last_successful_node: str | None = None
        self.model_version: str | None = None
        
    def load_from_history(self, events: list[Any]) -> None:
        if events and events[0].event_type != "AgentSessionStarted":
            self.reconstruction_issues.append("First event must be AgentSessionStarted")
            
        super().load_from_history(events)

    @property
    def reconstruction_status(self) -> ReconstructionStatus:
        return ReconstructionStatus.NEEDS_RECONCILIATION if self.reconstruction_issues else ReconstructionStatus.HEALTHY

    def assert_session_started(self) -> None:
        if not self.is_started:
            raise PreconditionFailedError(f"Session {self.stream_id} is not started. AgentSessionStarted must be the first event.")

    def apply_AgentSessionStarted(self, event: AgentSessionStarted) -> None:
        self.is_started = True
        self.model_version = event.model_version

    def apply_AgentContextLoaded(self, event: AgentContextLoaded) -> None:
        self.loaded_context_ids.add(str(event.event_id))

    def apply_AgentOutputWritten(self, event: AgentOutputWritten) -> None:
        # Gas Town Replay-time check
        if event.context_event_id not in self.loaded_context_ids:
            self.reconstruction_issues.append(f"Missing context reference: {event.context_event_id}")

    def apply_AgentNodeExecuted(self, event: AgentNodeExecuted) -> None:
        self.nodes_executed += 1
        self.last_successful_node = event.node_name

    def apply_AgentToolCalled(self, event: AgentToolCalled) -> None:
        self.tool_calls += 1

    def apply_AgentSessionCompleted(self, event: AgentSessionCompleted) -> None:
        self.is_completed = True

    def apply_AgentSessionFailed(self, event: AgentSessionFailed) -> None:
        self.is_failed = True

    def apply_AgentSessionRecovered(self, event: AgentSessionRecovered) -> None:
        self.is_failed = False

    def get_last_successful_node(self) -> str | None:
        return self.last_successful_node

    def get_state(self) -> dict[str, Any]:
        return {
            "is_started": self.is_started,
            "loaded_context_ids": list(self.loaded_context_ids),
            "is_completed": self.is_completed,
            "is_failed": self.is_failed,
            "nodes_executed": self.nodes_executed,
            "tool_calls": self.tool_calls,
            "last_successful_node": self.last_successful_node,
            "model_version": self.model_version,
            "version": self.version,
        }

    def restore_state(self, state: dict[str, Any]) -> None:
        self.is_started = state.get("is_started", False)
        self.loaded_context_ids = set(state.get("loaded_context_ids", []))
        self.is_completed = state.get("is_completed", False)
        self.is_failed = state.get("is_failed", False)
        self.nodes_executed = state.get("nodes_executed", 0)
        self.tool_calls = state.get("tool_calls", 0)
        self.last_successful_node = state.get("last_successful_node")
        self.model_version = state.get("model_version")
        self.version = state.get("version", 0)

    # Command methods
    def start_session(self, event: AgentSessionStarted) -> None:
        if self.is_started:
            raise DomainError(f"Session {self.stream_id} already started.")
        self.append_event(event)

    def record_context_loaded(self, event: AgentContextLoaded) -> None:
        self.assert_session_started()
        self.append_event(event)

    def record_node_execution(self, event: AgentNodeExecuted) -> None:
        self.assert_session_started()
        self.append_event(event)

    def record_tool_call(self, event: AgentToolCalled) -> None:
        self.assert_session_started()
        self.append_event(event)

    def record_output_written(self, event: AgentOutputWritten) -> None:
        self.assert_session_started()
        
        # Gas Town Write-Time Enforcement:
        # Invariant: Output MUST reference a context loaded in this session.
        if event.context_event_id not in self.loaded_context_ids:
            raise DomainError(
                f"Gas Town Violation: AgentOutputWritten references unknown context_event_id '{event.context_event_id}'. "
                "Context MUST be loaded and recorded before decisions are written."
            )
        
        self.append_event(event)

    def complete_session(self, event: AgentSessionCompleted) -> None:
        self.assert_session_started()
        if self.is_completed:
            raise DomainError(f"Session {self.stream_id} already completed.")
        self.append_event(event)

    def fail_session(self, event: AgentSessionFailed) -> None:
        self.assert_session_started()
        self.append_event(event)

    def recover_session(self, event: AgentSessionRecovered) -> None:
        self.assert_session_started()
        self.append_event(event)
