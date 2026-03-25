from __future__ import annotations
from typing import Tuple, Dict, Any
from src.upcasting.registry import registry

@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_analysis_v1_to_v2(payload: dict, metadata: dict) -> Tuple[dict, dict]:
    """
    Evolve CreditAnalysisCompleted from v1 to v2.
    Adds required observability fields: model_version, confidence_score, regulatory_basis.
    """
    new_payload = dict(payload)
    new_payload.setdefault("model_version", "unknown")
    new_payload.setdefault("confidence_score", None)
    new_payload.setdefault("regulatory_basis", "Legacy analysis - no basis recorded")
    
    # Metadata update happens in the registry loop
    return new_payload, metadata

@registry.register("DecisionGenerated", from_version=1)
def upcast_decision_generated_v1_to_v2(payload: dict, metadata: dict) -> Tuple[dict, dict]:
    """
    Evolve DecisionGenerated from v1 to v2.
    Adds lineage fields: confidence_score, contributing_agent_sessions, decision_basis_summary.
    """
    new_payload = dict(payload)
    new_payload.setdefault("confidence_score", 0.0)
    new_payload.setdefault("contributing_agent_sessions", [])
    new_payload.setdefault("decision_basis_summary", "Summary not available for legacy decision.")
    new_payload.setdefault("model_versions", {})
    
    return new_payload, metadata

# Initialize the upcasters implicitly when imported
def initialize_upcasters():
    pass
