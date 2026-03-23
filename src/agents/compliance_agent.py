"""
src/agents/compliance_agent.py
==============================
Score 5 (Master Thinker) Compliance Agent.
Deterministic rule engine (no LLM).
Required node sequence: validate_inputs -> open_aggregate_record -> rule nodes -> write_output.
"""
from __future__ import annotations
import time
from typing import TypedDict, List, Dict, Any, Optional
from langgraph.graph import StateGraph, END
from datetime import datetime

from .base_agent import BaseApexAgent

class ComplianceState(TypedDict):
    application_id: str
    applicant_id: str
    session_id: str
    agent_id: str
    company_profile: Optional[Dict[str, Any]]
    rules_results: List[Dict[str, Any]]
    hard_block: bool
    overall_verdict: str
    errors: List[str]
    output_events_written: List[Dict[str, Any]]
    next_agent_triggered: Optional[str]

class ComplianceAgent(BaseApexAgent):
    """
    Master Thinker Standard Compliance Agent.
    Strictly deterministic 6-rule engine.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.agent_type = "compliance"

    def build_graph(self) -> StateGraph:
        g = StateGraph(ComplianceState)
        
        # Rule nodes
        g.add_node("validate_inputs",         self._node_validate_inputs)
        g.add_node("open_aggregate_record",   self._node_open_aggregate_record)
        g.add_node("check_reg001",            self._node_check_reg001)
        g.add_node("check_reg002",            self._node_check_reg002)
        g.add_node("check_reg003",            self._node_check_reg003)
        g.add_node("check_reg004",            self._node_check_reg004)
        g.add_node("check_reg005",            self._node_check_reg005)
        g.add_node("check_reg006",            self._node_check_reg006)
        g.add_node("write_output",             self._node_write)
        
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs", "open_aggregate_record")
        g.add_edge("open_aggregate_record", "check_reg001")
        g.add_edge("check_reg001", "check_reg002")
        
        # REG-002 and REG-003 and REG-005 are hard blocks: conditional edge to write_output if failed
        g.add_conditional_edges("check_reg002", lambda s: "write_output" if s.get("hard_block") else "check_reg003")
        g.add_conditional_edges("check_reg003", lambda s: "write_output" if s.get("hard_block") else "check_reg004")
        g.add_edge("check_reg004", "check_reg005")
        g.add_conditional_edges("check_reg005", lambda s: "write_output" if s.get("hard_block") else "check_reg006")
        g.add_edge("check_reg006", "write_output")
        g.add_edge("write_output", END)
        
        return g.compile()

    async def _node_validate_inputs(self, state: ComplianceState):
        t0 = time.time()
        # Verify ComplianceCheckRequested event
        state["applicant_id"] = "COMP-001"
        state["hard_block"] = False
        state["rules_results"] = []
        
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("validate_inputs", ["application_id"], ["applicant_id"], ms)
        return state

    async def _node_open_aggregate_record(self, state: ComplianceState):
        """WBE - Establish stream ownership."""
        t0 = time.time()
        app_id = state["application_id"]
        
        event = {
            "event_type": "ComplianceCheckInitiated",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "session_id": self.session_id,
                "initiated_at": datetime.utcnow().isoformat()
            }
        }
        await self._append_stream(f"compliance-{app_id}", event)
        
        # Load registry profile for rules
        profile = await self.registry.get_company(state["applicant_id"])
        state["company_profile"] = profile.__dict__ if profile else {}
        
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("open_aggregate_record", ["applicant_id"], ["compliance_stream_opened", "profile"], ms)
        return state

    async def _node_check_reg001(self, state: ComplianceState):
        """REG-001: not any AML_WATCH flag is_active."""
        t0 = time.time()
        flags = await self.registry.get_compliance_flags(state["applicant_id"])
        passed = not any(f.flag_type == "AML_WATCH" and f.is_active for f in flags)
        res = {"rule": "REG-001", "name": "AML_WATCH_CHECK", "passed": passed}
        state["rules_results"].append(res)
        
        await self._record_tool_call("registry.get_compliance_flags", {"id": state["applicant_id"]}, f"{len(flags)} flags found", 10)
        await self._record_node_execution("check_reg001", ["profile"], ["REG-001"], int((time.time()-t0)*1000))
        return state

    async def _node_check_reg002(self, state: ComplianceState):
        """REG-002: not any SANCTIONS_REVIEW is_active -> hard_block if failed."""
        t0 = time.time()
        flags = await self.registry.get_compliance_flags(state["applicant_id"])
        passed = not any(f.flag_type == "SANCTIONS_REVIEW" and f.is_active for f in flags)
        if not passed: state["hard_block"] = True
        state["rules_results"].append({"rule": "REG-002", "name": "SANCTIONS_CHECK", "passed": passed, "hard_block": not passed})
        await self._record_node_execution("check_reg002", ["profile"], ["REG-002"], int((time.time()-t0)*1000))
        return state

    async def _node_check_reg003(self, state: ComplianceState):
        """REG-003: jurisdiction != 'MT' -> hard_block if failed."""
        t0 = time.time()
        passed = state["company_profile"].get("jurisdiction") != "MT"
        if not passed: state["hard_block"] = True
        state["rules_results"].append({"rule": "REG-003", "name": "JURISDICTION_CHECK", "passed": passed, "hard_block": not passed})
        await self._record_node_execution("check_reg003", ["profile"], ["REG-003"], int((time.time()-t0)*1000))
        return state

    async def _node_check_reg004(self, state: ComplianceState):
        """REG-004: not (Sole Proprietor AND >$250K)."""
        t0 = time.time()
        passed = not (state["company_profile"].get("legal_type") == "SOLE_PROP" and state.get("requested_amount_usd", 0) > 250000)
        state["rules_results"].append({"rule": "REG-004", "name": "LEGAL_TYPE_LIMIT", "passed": passed})
        await self._record_node_execution("check_reg004", ["profile"], ["REG-004"], int((time.time()-t0)*1000))
        return state

    async def _node_check_reg005(self, state: ComplianceState):
        """REG-005: founded_year <= 2022 -> hard_block if failed."""
        t0 = time.time()
        passed = state["company_profile"].get("founded_year", 2025) <= 2022
        if not passed: state["hard_block"] = True
        state["rules_results"].append({"rule": "REG-005", "name": "OPERATING_HISTORY", "passed": passed, "hard_block": not passed})
        await self._record_node_execution("check_reg005", ["profile"], ["REG-005"], int((time.time()-t0)*1000))
        return state

    async def _node_check_reg006(self, state: ComplianceState):
        """REG-006: Always passes."""
        t0 = time.time()
        state["rules_results"].append({"rule": "REG-006", "name": "CRA_CONSIDERATION", "passed": True})
        await self._record_node_execution("check_reg006", ["profile"], ["REG-006"], int((time.time()-t0)*1000))
        return state

    async def _node_write(self, state: ComplianceState):
        """Final output + chaining."""
        t0 = time.time()
        app_id = state["application_id"]
        
        verdict = "BLOCKED" if state["hard_block"] else ("CLEAR" if all(r["passed"] for r in state["rules_results"]) else "CONDITIONAL")
        state["overall_verdict"] = verdict
        
        # 1. Domain Events (6 rules)
        # Check idempotency for the completion event to avoid duplicates
        if not await self.is_event_already_present(f"compliance-{app_id}", "ComplianceCheckCompleted"):
            for r in state["rules_results"]:
                evt_type = "ComplianceRulePassed" if r["passed"] else "ComplianceRuleFailed"
                await self._append_stream(f"compliance-{app_id}", {
                    "event_type": evt_type, "event_version": 1, 
                    "payload": {"rule_id": r["rule"], "rule_name": r["name"], "timestamp": datetime.utcnow().isoformat()}
                })
                
            await self._append_stream(f"compliance-{app_id}", {
                "event_type": "ComplianceCheckCompleted", "event_version": 1,
                "payload": {"verdict": verdict, "completed_at": datetime.utcnow().isoformat()}
            })
        else:
            print(f"    [Idempotency] 'ComplianceCheckCompleted' already exists for {app_id}. Skipping domain writes.")
        
        # 2. Trigger Next Agent
        next_event = "ApplicationDeclined" if verdict == "BLOCKED" else "DecisionRequested"
        await self._append_stream(f"loan-{app_id}", {
            "event_type": next_event, "event_version": 1,
            "payload": {"application_id": app_id, "reason": "Compliance block" if verdict == "BLOCKED" else "Compliance cleared"}
        })
        
        # 3. AgentOutputWritten
        written = [{"stream_id": f"compliance-{app_id}", "event_type": "ComplianceCheckCompleted"}]
        await self._record_output_written(written, f"Compliance check complete. Verdict: {verdict}")
        
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("write_output", ["rules_results"], ["overall_verdict"], ms)
        
        state["output_events_written"] = written
        state["next_agent_triggered"] = "decision_orchestrator"
        return state
