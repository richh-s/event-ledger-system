from typing import Any
from src.aggregates.base import BaseAggregate
from src.models.events import (
    BaseEvent,
    FraudScreeningInitiated,
    FraudAnomalyDetected,
    FraudScreeningCompleted,
    DomainError,
)

class FraudScreeningAggregate(BaseAggregate):
    """
    Tracks fraud screening events.
    Rule: Fraud score range validation (0.0 to 1.0).
    """
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.is_initiated = False
        self.is_completed = False
        self.fraud_score = 0.0

    def apply_FraudScreeningInitiated(self, event: FraudScreeningInitiated) -> None:
        self.is_initiated = True

    def apply_FraudAnomalyDetected(self, event: FraudAnomalyDetected) -> None:
        pass

    def apply_FraudScreeningCompleted(self, event: FraudScreeningCompleted) -> None:
        self.is_completed = True
        self.fraud_score = event.fraud_score

    def get_state(self) -> dict[str, Any]:
        return {
            "is_initiated": self.is_initiated,
            "is_completed": self.is_completed,
            "fraud_score": self.fraud_score,
            "version": self.version,
        }

    def restore_state(self, state: dict[str, Any]) -> None:
        self.is_initiated = state.get("is_initiated", False)
        self.is_completed = state.get("is_completed", False)
        self.fraud_score = state.get("fraud_score", 0.0)
        self.version = state.get("version", 0)

    # Command methods
    def initiate_screening(self, event: FraudScreeningInitiated) -> None:
        if self.is_initiated:
            raise DomainError(f"Fraud screening {self.stream_id} already initiated.")
        self.append_event(event)

    def record_anomaly(self, event: FraudAnomalyDetected) -> None:
        if not self.is_initiated:
            raise DomainError(f"Fraud screening {self.stream_id} is not initiated.")
        self.append_event(event)

    def validate_fraud_score(self, score: float) -> None:
        if not (0.0 <= score <= 1.0):
            raise DomainError(f"Fraud score {score} is out of range [0.0, 1.0].")

    def complete_screening(self, event: FraudScreeningCompleted) -> None:
        if not self.is_initiated:
            raise DomainError(f"Fraud screening {self.stream_id} is not initiated.")
        if self.is_completed:
            raise DomainError(f"Fraud screening {self.stream_id} is already completed.")
            
        self.validate_fraud_score(event.fraud_score)
            
        self.append_event(event)
