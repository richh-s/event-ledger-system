"""
src/what_if/projector.py
========================
Phase 6 Minimal 'What-If' Support that Stays Aligned.
"""
from typing import Dict, Any, Optional
from src.database import Database
from src.event_store import EventStore

async def run_what_if(
    application_id: str,
    alternate_model: str,
    db: Database,
    store: EventStore,
    as_of_global_position: Optional[int] = None,
    override_risk_tier: Optional[str] = None
) -> Dict[str, Any]:
    """
    Counterfactual 'What-If' projection:
    Injects an alternate Credit Analysis outcome and re-runs the Decision Orchestrator's 
    authority layer rules to show the cascading effect on the final application verdict.
    """
    from src.regulatory.package import generate_regulatory_package
    from src.models.events import RiskTier
    
    # 1. Load historical context
    hist = await generate_regulatory_package(application_id, db, store, as_of_global_position=as_of_global_position)
    
    orig_credit_ev = next((e for e in hist["full_event_history"] if e["event_type"] == "CreditAnalysisCompleted"), None)
    if not orig_credit_ev:
        return {"error": f"No historical credit analysis found for application {application_id}."}
        
    orig_payload = orig_credit_ev["payload"]
    orig_decision = orig_payload.get("decision", {})
    orig_tier = orig_decision.get("risk_tier")
    
    # 2. Determine Counterfactual Outcome
    cf_tier = override_risk_tier or ("HIGH" if orig_tier == "MEDIUM" else "MEDIUM")
    
    # Simulate LLM synthesis (hypothetically remains the same for the experiment)
    hypothetical_llm_rec = "APPROVE" if orig_tier in ["LOW", "MEDIUM"] else "REFER"
    
    # 3. APPLY AUTHORITY LAYER RULES (Cascading Logic)
    # This mimics the logic in src/agents/decision_orchestrator.py
    recomputed_rec = hypothetical_llm_rec
    violations = []
    
    # RULE: HIGH risk tier must be REFERRED for human review (Authority Overrides LLM)
    if cf_tier == "HIGH":
        if recomputed_rec == "APPROVE":
            recomputed_rec = "REFER"
            violations.append("POLICY_OVERRIDE: HIGH_RISK_MANDATORY_REFERRAL")
            
    # RULE: Compliance check must be CLEAR (fetch from history)
    compliance_verdict = hist["reconstructed_read_model"].get("compliance_status")
    if compliance_verdict == "BLOCKED":
        recomputed_rec = "DECLINE"
        violations.append("POLICY_ENFORCEMENT: COMPLIANCE_BLOCK")

    return {
        "metadata": {
            "application_id": application_id,
            "experiment_type": "COUNTER_FACTUAL_RISK_INJECTION",
            "alternate_model": alternate_model
        },
        "original_scenario": {
            "risk_tier": orig_tier,
            "orchestrator_recommendation": hist["reconstructed_read_model"].get("decision")
        },
        "counterfactual_scenario": {
            "risk_tier": cf_tier,
            "final_verdict": recomputed_rec,
            "policy_violations": violations,
            "is_different": recomputed_rec != hist["reconstructed_read_model"].get("decision")
        },
        "impact_summary": f"Changing risk from {orig_tier} to {cf_tier} resulted in a final verdict of {recomputed_rec} (Originally {hist['reconstructed_read_model'].get('decision')})."
    }
