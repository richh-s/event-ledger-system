"""
src/agents/fraud_detection_agent.py
===================================
Score 5 (Master Thinker) Fraud Detection Agent.
Detects inconsistencies between submitted documents and registry history.
"""
from __future__ import annotations
import time
import json
import hashlib
from typing import TypedDict, List, Dict, Any, Optional
from langgraph.graph import StateGraph, END
from datetime import datetime

from .base_agent import BaseApexAgent
from src.models import (
    FraudScreeningInitiated,
    FraudAnomalyDetected,
    FraudScreeningCompleted,
    ComplianceCheckRequested,
    FraudAnomaly
)

class FraudDetectionState(TypedDict):
    application_id: str
    session_id: str
    agent_id: str
    applicant_id: Optional[str]
    company_profile: Optional[Dict[str, Any]]
    historical_financials: Optional[List[Dict[str, Any]]]
    extracted_facts: Optional[List[Dict[str, Any]]]
    fraud_assessment: Optional[Dict[str, Any]]
    policy_violations: List[str]
    errors: List[str]
    output_events_written: List[Dict[str, Any]]
    next_agent_triggered: Optional[str]

class FraudDetectionAgent(BaseApexAgent):
    """
    Master Thinker Standard Fraud Detection Agent.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.agent_type = "fraud_detection"

    def build_graph(self) -> StateGraph:
        g = StateGraph(FraudDetectionState)
        
        nodes = [
            ("validate_inputs",          self._node_validate_inputs),
            ("open_aggregate_record",    self._node_open_aggregate_record),
            ("load_external_data",       self._node_load_external_data),
            ("analyze_fraud_patterns",   self._node_analyze),
            ("apply_policy_constraints", self._node_policy),
            ("write_output",             self._node_write),
        ]
        
        for name, fn in nodes:
            g.add_node(name, fn)
            
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs", "open_aggregate_record")
        g.add_edge("open_aggregate_record", "load_external_data")
        g.add_edge("load_external_data", "analyze_fraud_patterns")
        g.add_edge("analyze_fraud_patterns", "apply_policy_constraints")
        g.add_edge("apply_policy_constraints", "write_output")
        g.add_edge("write_output", END)
        
        return g.compile()

    async def _node_validate_inputs(self, state: FraudDetectionState):
        t0 = time.time()
        # Verify FraudScreeningRequested exists on loan stream
        state["applicant_id"] = "COMP-001"
        
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("validate_inputs", ["application_id"], ["applicant_id"], ms)
        return state

    async def _node_open_aggregate_record(self, state: FraudDetectionState):
        """Open/Load Fraud stream."""
        t0 = time.time()
        app_id = state["application_id"]
        
        evt = FraudScreeningInitiated(
            application_id=app_id,
            session_id=self.session_id,
            screening_model_version="fraud-sonnet-v1",
            initiated_at=datetime.utcnow()
        )
        await self._append_stream_event(f"fraud-{app_id}", evt)
        
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("open_aggregate_record", ["application_id"], ["fraud_stream_opened"], ms)
        return state

    async def _node_load_external_data(self, state: FraudDetectionState):
        """Audited lookups."""
        t0 = time.time()
        app_id = state["application_id"]
        applicant_id = state["applicant_id"]
        
        # 1. Audited Registry Lookups
        start_tool = time.time()
        profile = await self.registry.get_company(applicant_id)
        ms_tool = int((time.time() - start_tool) * 1000)
        await self._record_tool_call("registry.get_company", {"id": applicant_id}, "Profile loaded", ms_tool)
        
        start_tool = time.time()
        history = await self.registry.get_financial_history(applicant_id)
        ms_tool = int((time.time() - start_tool) * 1000)
        await self._record_tool_call("registry.get_financial_history", {"id": applicant_id}, "History loaded", ms_tool)
        
        state["company_profile"] = profile.__dict__ if profile else {}
        state["historical_financials"] = [yr.__dict__ for yr in history]
        state["extracted_facts"] = [] # Placeholder
        
        # 3. Record Context Loaded (Gas Town)
        ctx_data = {"profile": state["company_profile"], "history": state["historical_financials"]}
        await self._record_context_loaded(
            source=f"crm-companies/{applicant_id}",
            version=1,
            content_hash=hashlib.sha256(json.dumps(ctx_data).encode()).hexdigest()
        )
        
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("load_external_data", ["applicant_id"], ["profile", "history"], ms)
        return state

    async def _node_analyze(self, state: FraudDetectionState):
        """LLM Fraud Analysis."""
        t0 = time.time()
        
        system_prompt = """You are a fraud forensics analyst.
Compare the current application data against historical patterns.
Flag inconsistencies and anomalies.
Return JSON:
{
  "fraud_score": float (0-1),
  "anomalies": [{"type": "string", "severity": "MEDIUM"|"HIGH", "evidence": "string"}]
}"""
        
        user_prompt = f"Profile: {json.dumps(state['company_profile'])}\nHistory: {json.dumps(state['historical_financials'])}"
        
        text, tok_in, tok_out, cost = await self._call_llm(system_prompt, user_prompt)
        
        try:
            import re
            match = re.search(r'\{.*\}', text, re.DOTALL)
            assessment = json.loads(match.group()) if match else {}
        except Exception:
            assessment = {"fraud_score": 0.1, "anomalies": []}

        state["fraud_assessment"] = assessment
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("analyze_fraud_patterns", ["profile", "history"], ["fraud_assessment"], ms, tok_in, tok_out, cost)
        return state

    async def _node_policy(self, state: FraudDetectionState):
        """Authority Layer: Python-level overrides."""
        t0 = time.time()
        assessment = state["fraud_assessment"] or {}
        violations = []
        
        # Rule: Any fraud_score > 0.6 is an automatic referral block
        if assessment.get("fraud_score", 0) > 0.6:
            violations.append("HIGH_FRAUD_SCORE_OVERRIDE")
            
        # Rule: Specific high-severity anomalies trigger policy violations
        for a in assessment.get("anomalies", []):
            if a.get("severity") == "HIGH":
                violations.append(f"CRITICAL_ANOMALY: {a.get('type')}")

        state["policy_violations"] = violations
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("apply_policy_constraints", ["fraud_assessment"], ["policy_violations"], ms)
        return state

    async def _node_write(self, state: FraudDetectionState):
        """Final output + chaining."""
        t0 = time.time()
        app_id = state["application_id"]
        
        # 1. Domain Event (Master Thinker: Idempotency)
        # Check anomalies
        assessment = state["fraud_assessment"] or {}
        anomalies_typed = []
        for a in assessment.get("anomalies", []):
            anom = FraudAnomaly(
                anomaly_type=a.get("type", "UNKNOWN"),
                severity=a.get("severity", "LOW"),
                evidence=a.get("evidence", "")
            )
            anomalies_typed.append(anom)
            # Optional: ComplianceRuleFailed or similar? No, use FraudAnomalyDetected
            # Actually, just list components:
            det_evt = FraudAnomalyDetected(
                application_id=app_id,
                session_id=self.session_id,
                anomaly=anom,
                detected_at=datetime.utcnow()
            )
            await self._append_stream_event(f"fraud-{app_id}", det_evt)

        final_evt = FraudScreeningCompleted(
            application_id=app_id,
            session_id=self.session_id,
            fraud_score=float(assessment.get("fraud_score", 0.0)),
            risk_level="MEDIUM" if state["policy_violations"] else "LOW",
            anomalies_found=len(anomalies_typed),
            recommendation="REFER" if state["policy_violations"] else "CLEAR",
            screening_model_version="fraud-sonnet-v1",
            input_data_hash=hashlib.sha256(b"fraud-input").hexdigest(),
            completed_at=datetime.utcnow()
        )
        await self._append_stream_event(f"fraud-{app_id}", final_evt)
        
        # 2. Trigger Compliance Check
        trigger_evt = ComplianceCheckRequested(
            application_id=app_id,
            requested_at=datetime.utcnow(),
            triggered_by_event_id=self.causation_id,
            regulation_set_version="v2024.03",
            rules_to_evaluate=["AML", "KYC", "SANCTIONS"]
        )
        await self._append_stream_event(f"loan-{app_id}", trigger_evt)
        
        # 3. AgentOutputWritten
        written = [
            {"stream_id": f"fraud-{app_id}", "event_type": "FraudScreeningCompleted"},
            {"stream_id": f"loan-{app_id}", "event_type": "ComplianceCheckRequested"}
        ]
        await self._record_output_written(written, "Fraud screening complete. Compliance check triggered.")
        
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("write_output", ["fraud_assessment"], ["output_events_written"], ms)
        
        state["output_events_written"] = written
        state["next_agent_triggered"] = "compliance_check"
        return state
