from typing import Any
from src.aggregates.base import BaseAggregate
from src.models.events import (
    BaseEvent,
    PackageCreated,
    DocumentAdded,
    DocumentFormatValidated,
    DocumentFormatRejected,
    ExtractionStarted,
    ExtractionCompleted,
    ExtractionFailed,
    QualityAssessmentCompleted,
    PackageReadyForAnalysis,
    DomainError,
)

class DocumentPackageAggregate(BaseAggregate):
    """
    Tracks document processing state.
    Rule: Tracks extraction facts quality and field confidence (Rule 7).
    """
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.is_created = False
        self.is_ready = False
        self.documents: set[str] = set()
        self.extractions: set[str] = set()
        self.quality_anomalies: list[str] = []
        self.overall_confidence = 1.0

    def apply_PackageCreated(self, event: PackageCreated) -> None:
        self.is_created = True

    def apply_DocumentAdded(self, event: DocumentAdded) -> None:
        self.documents.add(event.document_id)

    def apply_ExtractionCompleted(self, event: ExtractionCompleted) -> None:
        self.extractions.add(event.document_id)
        # Store field confidence for Rule 7/Checklist 1.23
        if event.facts and event.facts.field_confidence:
             conf_values = event.facts.field_confidence.values()
             if conf_values:
                 self.overall_confidence = min(self.overall_confidence, min(conf_values))

    def apply_QualityAssessmentCompleted(self, event: QualityAssessmentCompleted) -> None:
        min_conf = min(self.overall_confidence, event.overall_confidence)
        self.overall_confidence = min_conf
        if event.anomalies:
            self.quality_anomalies.extend(event.anomalies)

    def apply_PackageReadyForAnalysis(self, event: PackageReadyForAnalysis) -> None:
        self.is_ready = True

    # Intentionally omitted boilerplate applies for simplicity

    def get_state(self) -> dict[str, Any]:
        return {
            "is_created": self.is_created,
            "is_ready": self.is_ready,
            "documents": list(self.documents),
            "extractions": list(self.extractions),
            "quality_anomalies": self.quality_anomalies,
            "overall_confidence": self.overall_confidence,
            "version": self.version,
        }

    def restore_state(self, state: dict[str, Any]) -> None:
        self.is_created = state.get("is_created", False)
        self.is_ready = state.get("is_ready", False)
        self.documents = set(state.get("documents", []))
        self.extractions = set(state.get("extractions", []))
        self.quality_anomalies = state.get("quality_anomalies", [])
        self.overall_confidence = state.get("overall_confidence", 1.0)
        self.version = state.get("version", 0)

    # Command methods
    def create_package(self, event: PackageCreated) -> None:
        if self.is_created:
            raise DomainError(f"Package {self.stream_id} already created.")
        self.append_event(event)

    def add_document(self, event: DocumentAdded) -> None:
        if not self.is_created:
            raise DomainError(f"Package {self.stream_id} not created.")
        self.append_event(event)
        
    def validate_format(self, event: DocumentFormatValidated) -> None:
        self.append_event(event)
        
    def reject_format(self, event: DocumentFormatRejected) -> None:
        self.append_event(event)

    def start_extraction(self, event: ExtractionStarted) -> None:
        self.append_event(event)

    def complete_extraction(self, event: ExtractionCompleted) -> None:
        self.append_event(event)
        
    def fail_extraction(self, event: ExtractionFailed) -> None:
        self.append_event(event)

    def assess_quality(self, event: QualityAssessmentCompleted) -> None:
        self.append_event(event)

    def mark_ready(self, event: PackageReadyForAnalysis) -> None:
        if self.is_ready:
            raise DomainError(f"Package {self.stream_id} is already ready.")
        self.append_event(event)
