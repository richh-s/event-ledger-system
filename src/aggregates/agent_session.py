from typing import Any
from src.aggregates.base import BaseAggregate
from src.models.events import (
    BaseEvent,
    AgentSessionStarted,
    AgentNodeExecuted,
    AgentToolCalled,
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
    """
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.is_started = False
        self.context_loaded = False
        self.is_completed = False
        self.is_failed = False
        self.nodes_executed = 0
        self.tool_calls = 0
        self.last_successful_node: str | None = None
        self.model_version: str | None = None

    def assert_session_started(self) -> None:
        if not self.is_started:
            raise PreconditionFailedError(f"Session {self.stream_id} is not started. AgentSessionStarted must be the first event.")

    def assert_model_version(self, version: str) -> None:
        if self.model_version and self.model_version != version:
            raise DomainError(f"Model version mismatch. Expected {self.model_version}, got {version}")

    def apply_AgentSessionStarted(self, event: AgentSessionStarted) -> None:
        self.is_started = True
        self.context_loaded = True
        self.model_version = event.model_version

    def apply_AgentNodeExecuted(self, event: AgentNodeExecuted) -> None:
        self.nodes_executed += 1
        self.last_successful_node = event.node_name

    def apply_AgentToolCalled(self, event: AgentToolCalled) -> None:
        self.tool_calls += 1

    def apply_AgentSessionCompleted(self, event: AgentSessionCompleted) -> None:
        self.is_completed = True

    def apply_AgentSessionFailed(self, event: AgentSessionFailed) -> None:
        self.is_failed = True
        # last_successful_node remains what it was before failure

    def apply_AgentSessionRecovered(self, event: AgentSessionRecovered) -> None:
        self.is_failed = False

    def get_last_successful_node(self) -> str | None:
        return self.last_successful_node

    def get_state(self) -> dict[str, Any]:
        return {
            "is_started": self.is_started,
            "context_loaded": self.context_loaded,
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
        self.context_loaded = state.get("context_loaded", False)
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

    def record_node_execution(self, event: AgentNodeExecuted) -> None:
        self.assert_session_started()
        # No model_version in AgentNodeExecuted according to models? 
        # Actually models may vary. Let's check models. 
        self.append_event(event)

    def record_tool_call(self, event: AgentToolCalled) -> None:
        self.assert_session_started()
        self.append_event(event)

    def record_output_written(self, event: AgentOutputWritten) -> None:
        self.assert_session_started()
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
