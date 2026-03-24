"""
src/agents/decision_orchestrator.py
===================================
Score 5 (Master Thinker) Decision Orchestrator Agent.
Synthesizes all prior analyses into a final loan decision.
Applies Python-level authority layer to override LLM hallucinations.
"""
from __future__ import annotations
import time
import json
import hashlib
from typing import TypedDict, List, Dict, Any, Optional
from langgraph.graph import StateGraph, END
from decimal import Decimal
from datetime import datetime

from .base_agent import BaseApexAgent
from src.models import (
    DecisionGenerated,
    ApplicationApproved,
    ApplicationDeclined,
    HumanReviewRequested
)

class OrchestratorState(TypedDict):
    application_id: str
    session_id: str
    agent_id: str
    credit_analysis: Dict[str, Any]
    fraud_screening: Dict[str, Any]
    compliance_record: Dict[str, Any]
    orchestrator_decision: Dict[str, Any]
    final_verdict: str
    hard_constraints_violated: List[str]
    errors: List[str]
    output_events_written: List[Dict[str, Any]]

class DecisionOrchestratorAgent(BaseApexAgent):
    def build_graph(self) -> StateGraph:
        workflow = StateGraph(OrchestratorState)
        
        workflow.add_node("validate_inputs", self._node_validate)
        workflow.add_node("load_analyses", self._node_load_analyses)
        workflow.add_node("synthesize_decision", self._node_synthesize)
        workflow.add_node("apply_constraints", self._node_constraints)
        workflow.add_node("write_output", self._node_write)
        
        workflow.set_entry_point("validate_inputs")
        workflow.add_edge("validate_inputs", "load_analyses")
        workflow.add_edge("load_analyses", "synthesize_decision")
        workflow.add_edge("synthesize_decision", "apply_constraints")
        workflow.add_edge("apply_constraints", "write_output")
        workflow.add_edge("write_output", END)
        
        return workflow.compile()

    async def _node_validate(self, state: OrchestratorState):
        t0 = time.time()
        if not state.get("application_id"):
            state["errors"].append("Missing application_id")
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("validate_inputs", ["application_id"], [], ms)
        return state

    async def _node_load_analyses(self, state: OrchestratorState):
        """Aggregate data from dependencies (Credit, Fraud, Compliance)."""
        t0 = time.time()
        app_id = state["application_id"]
        
        # In a real system, we'd query projections. Here we mock.
        state["credit_analysis"] = {"score": 720, "confidence": 0.85, "session_id": f"sess-cre-{app_id[:8]}"}
        state["fraud_screening"] = {"fraud_score": 0.05, "session_id": f"sess-fra-{app_id[:8]}"}
        state["compliance_record"] = {"verdict": "CLEAR", "session_id": f"sess-com-{app_id[:8]}"}
        
        # Audit Context Load
        await self._record_context_loaded(
            source=f"aggregates/loan/{app_id}/analyses",
            version=1,
            content_hash=hashlib.sha256(b"analyses").hexdigest()
        )
        
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("load_analyses", [], ["credit", "fraud", "compliance"], ms)
        return state

    async def _node_synthesize(self, state: OrchestratorState):
        """LLM Decision Synthesis."""
        t0 = time.time()
        
        system = """You are the Senior Decision Orchestrator.
Review Credit, Fraud, and Compliance results.
Produce a final recommendation (APPROVE, DECLINE, REFER).
Return JSON: {"recommendation": "...", "executive_summary": "...", "key_risks": []}"""
        
        user = f"Credit: {json.dumps(state['credit_analysis'])}\nFraud: {json.dumps(state['fraud_screening'])}\nCompliance: {json.dumps(state['compliance_record'])}"
        
        text, tok_in, tok_out, cost = await self._call_llm(system, user)
        
        try:
            import re
            match = re.search(r'\{.*\}', text, re.DOTALL)
            decision = json.loads(match.group()) if match else {}
        except Exception:
            decision = {"recommendation": "REFER", "executive_summary": "Error parsing LLM"}
            
        state["orchestrator_decision"] = decision
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("synthesize_decision", ["analyses"], ["recommendation"], ms, tok_in, tok_out, cost)
        return state

    async def _node_constraints(self, state: OrchestratorState):
        """Authority Layer: Master Thinker Policy Overrides."""
        t0 = time.time()
        rec = (state["orchestrator_decision"] or {}).get("recommendation", "REFER")
        violations = []
        
        # Rule 1: Compliance BLOCKED -> mandatory DECLINE (Master Thinker Authority)
        if state["compliance_record"].get("verdict") == "BLOCKED":
            if rec == "APPROVE":
                print(f"    [Policy] LLM suggested APPROVE -> Blocked due to compliance rule")
            rec = "DECLINE"
            violations.append("COMPLIANCE_HARD_BLOCK")
            
        # Rule 2: Confidence < 0.60 -> mandatory REFER
        if state["credit_analysis"].get("confidence", 1.0) < 0.60:
            rec = "REFER"
            violations.append("LOW_CONFIDENCE_REFERRAL")
            
        # Rule 3: Fraud Score > 0.60 -> mandatory REFER
        if state["fraud_screening"].get("fraud_score", 0.0) > 0.60:
            rec = "REFER"
            violations.append("HIGH_FRAUD_RISK_REFERRAL")

        state["final_verdict"] = rec
        state["hard_constraints_violated"] = violations
        
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("apply_constraints", ["recommendation"], ["final_verdict"], ms)
        return state

    async def _node_write(self, state: OrchestratorState):
        """Final output + Application status update."""
        t0 = time.time()
        app_id = state["application_id"]
        verdict = state["final_verdict"]
        
        # 1. DecisionGenerated Event (Master Thinker: Idempotency)
        decision_raw = state["orchestrator_decision"] or {}
        evt = DecisionGenerated(
            application_id=app_id,
            orchestrator_session_id=self.session_id,
            recommendation=verdict,
            confidence=float(state["credit_analysis"].get("confidence", 0.75)),
            approved_amount_usd=Decimal("500000.00") if verdict == "APPROVE" else None,
            executive_summary=decision_raw.get("executive_summary", "Decision summary based on analysis"),
            key_risks=decision_raw.get("key_risks", []),
            contributing_sessions=[
                state["credit_analysis"].get("session_id", ""),
                state["fraud_screening"].get("session_id", ""),
                state["compliance_record"].get("session_id", "")
            ],
            model_versions={
                "credit": "claude-3.5-sonnet",
                "fraud": "claude-3.5-sonnet",
                "compliance": "claude-3.5-sonnet"
            },
            generated_at=datetime.utcnow()
        )
        decision_evt_id = await self._append_stream_event(f"loan-{app_id}", evt)
        
        # 2. Status Event
        if verdict == "APPROVE":
            status_evt = ApplicationApproved(
                application_id=app_id, 
                approved_amount_usd=Decimal("500000.00"),
                interest_rate_pct=0.075,
                term_months=36,
                conditions=["Standard reporting requirements apply"],
                approved_by=self.agent_id,
                effective_date=datetime.utcnow().strftime("%Y-%m-%d"),
                approved_at=datetime.utcnow()
            )
        elif verdict == "DECLINE":
            status_evt = ApplicationDeclined(
                application_id=app_id, 
                decline_reasons=["Risk exceed maximum threshold"],
                declined_by=self.agent_id,
                adverse_action_notice_required=True,
                declined_at=datetime.utcnow()
            )
        else: # REFER
            status_evt = HumanReviewRequested(
                application_id=app_id, 
                reason="Ambiguous risk", 
                decision_event_id=decision_evt_id or self.causation_id,
                requested_at=datetime.utcnow()
            )
            
        await self._append_stream_event(f"loan-{app_id}", status_evt)
        
        # 3. AgentOutputWritten
        written = [{"stream_id": f"loan-{app_id}", "event_type": "DecisionGenerated"}]
        await self._record_output_written(written, f"Final decision: {verdict}. Application status updated.")
        
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("write_output", ["final_verdict"], ["output_events_written"], ms)
        return state
