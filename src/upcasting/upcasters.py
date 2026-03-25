from __future__ import annotations
from datetime import datetime, timezone
from typing import Tuple, Any
from src.upcasting.registry import registry

@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_analysis_v1_to_v2(payload: dict, metadata: dict) -> Tuple[dict, dict]:
    """
    Evolve CreditAnalysisCompleted from v1 to v2.
    Adds required observability fields: model_version, confidence_score, regulatory_basis.
    Uses timestamp-based inference for model_version and regulatory basis.
    """
    new_payload = dict(payload)
    recorded_at = metadata.get("recorded_at")
    
    if isinstance(recorded_at, str):
        try:
            recorded_at = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
        except:
            recorded_at = None

    if recorded_at and recorded_at.tzinfo is None:
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)

    # 1. model_version inference
    if recorded_at and recorded_at < datetime(2024, 6, 1, tzinfo=timezone.utc):
        new_payload.setdefault("model_version", "gpt-4")
    else:
        new_payload.setdefault("model_version", "gpt-4o")

    # 2. confidence_score: null with documented reasoning (stored in metadata)
    new_payload.setdefault("confidence_score", None)
    metadata["upcast_reasoning"] = "v1 records lacked probability distribution metrics; defaulting to null."

    # 3. regulatory_basis: Inferred from rule versions active at recorded date
    if recorded_at and recorded_at < datetime(2024, 2, 1, tzinfo=timezone.utc):
        new_payload.setdefault("regulatory_basis", ["BASEL-III", "internal-v1"])
    else:
        new_payload.setdefault("regulatory_basis", ["BASEL-IV-prelim", "internal-v2"])
    
    return new_payload, metadata

@registry.register("DecisionGenerated", from_version=1)
def upcast_decision_generated_v1_to_v2(payload: dict, metadata: dict) -> Tuple[dict, dict]:
    """
    Evolve DecisionGenerated from v1 to v2.
    Adds lineage fields: confidence_score, contributing_agent_sessions, model_versions.
    """
    new_payload = dict(payload)
    
    # Reconstruct lineage: In v1 these were implicit
    new_payload.setdefault("confidence_score", 0.0) # Conservative default
    new_payload.setdefault("contributing_agent_sessions", [])
    
    # model_versions reconstruction
    # Inferred from common orchestration patterns in v1
    new_payload.setdefault("model_versions", {
        "orchestrator": "decision-v1-static",
        "credit_sub_model": "legacy-risk-engine"
    })
    
    return new_payload, metadata

# Initialize the upcasters implicitly when imported
def initialize_upcasters():
    pass
