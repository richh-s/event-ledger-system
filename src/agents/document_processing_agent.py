"""
src/agents/document_processing_agent.py
=======================================
Score 5 (Master Thinker) Document Processing Agent.
Handles the transition from ApplicationSubmitted to DOCUMENTS_PROCESSED.
Mocks OCR extraction of financial facts and establishes the docpkg stream.
"""
import time
import json
import hashlib
from typing import TypedDict, List, Dict, Any, Optional
from langgraph.graph import StateGraph, END
from datetime import datetime

from .base_agent import BaseApexAgent
from src.models import (
    DocumentPackageCreated,
    DocumentAdded,
    ApplicationSubmitted,
    CreditAnalysisRequested,
    FinancialFacts,
    DocumentType,
    DocumentFormat
)

class DocProcessingState(TypedDict):
    application_id: str
    session_id: str
    agent_id: str
    documents: List[Dict[str, Any]]
    extracted_facts: List[Dict[str, Any]]
    package_id: str
    errors: List[str]
    output_events_written: List[Dict[str, Any]]
    next_agent_triggered: Optional[str]

class DocumentProcessingAgent(BaseApexAgent):
    """
    Master Thinker Standard Document Processing Agent.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.agent_type = "document_processing"

    def build_graph(self) -> StateGraph:
        g = StateGraph(DocProcessingState)
        
        nodes = [
            ("validate_inputs",          self._node_validate_inputs),
            ("open_aggregate_record",    self._node_open_aggregate_record),
            ("simulate_ocr",             self._node_ocr),
            ("simulated_verification",   self._node_verify),
            ("write_output",             self._node_write),
        ]
        
        for name, fn in nodes:
            g.add_node(name, fn)
            
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs", "open_aggregate_record")
        g.add_edge("open_aggregate_record", "simulate_ocr")
        g.add_edge("simulate_ocr", "simulated_verification")
        g.add_edge("simulated_verification", "write_output")
        g.add_edge("write_output", END)
        
        return g.compile()

    async def _node_validate_inputs(self, state: DocProcessingState):
        t0 = time.time()
        # In a real system, load loan stream to find ApplicationSubmitted
        state["documents"] = [{"id": "doc_1", "type": "TAX_RETURN_2023"}]
        state["package_id"] = f"pkg-{state['application_id'][0:8]}"
        
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("validate_inputs", ["application_id"], ["documents"], ms)
        return state

    async def _node_open_aggregate_record(self, state: DocProcessingState):
        t0 = time.time()
        app_id = state["application_id"]
        
        evt = DocumentPackageCreated(
            application_id=app_id,
            package_id=state["package_id"],
            required_documents=[DocumentType.APPLICATION_PROPOSAL, DocumentType.INCOME_STATEMENT],
            session_id=self.session_id,
            created_at=datetime.utcnow()
        )
        await self._append_stream_event(f"docpkg-{app_id}", evt)
        
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("open_aggregate_record", ["application_id"], ["stream_opened"], ms)
        return state

    async def _node_ocr(self, state: DocProcessingState):
        t0 = time.time()
        # Mock OCR extraction
        facts = [
            {"year": 2023, "revenue": 1500000.0, "net_income": 300000.0}
        ]
        state["extracted_facts"] = facts
        
        # Audit Context Load (Mocking the "Doc" content as context)
        await self._record_context_loaded(
            source=f"blob-store/{state['package_id']}",
            version=1,
            content_hash=hashlib.sha256(json.dumps(facts).encode()).hexdigest()
        )
        
        app_id = state["application_id"]
        for doc in state["documents"]:
            # Map mock string types to enum if possible, else default
            d_type = DocumentType.INCOME_STATEMENT
            if "proposal" in str(doc.get("type", "")).lower():
                d_type = DocumentType.APPLICATION_PROPOSAL
                
            evt = DocumentAdded(
                package_id=state["package_id"],
                document_id=doc["id"],
                document_type=d_type,
                document_format=DocumentFormat.PDF,
                file_hash=hashlib.sha256(doc.get("content", "").encode()).hexdigest(),
                added_at=datetime.utcnow()
            )
            await self._append_stream_event(f"docpkg-{app_id}", evt)

        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("simulate_ocr", ["documents"], ["facts"], ms)
        return state

    async def _node_verify(self, state: DocProcessingState):
        t0 = time.time()
        # Simulate human-in-the-loop or automated verify
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("simulated_verification", ["facts"], ["verified"], ms)
        return state

    async def _node_write(self, state: DocProcessingState):
        t0 = time.time()
        app_id = state["application_id"]
        
        # 1. Trigger next: Credit Analysis
        trigger_evt = CreditAnalysisRequested(
            application_id=app_id,
            requested_at=datetime.utcnow(),
            requested_by=self.agent_id,
            triggered_by_event_id=self.causation_id
        )
        await self._append_stream_event(f"loan-{app_id}", trigger_evt)
        
        written = [{"stream_id": f"loan-{app_id}", "event_type": "CreditAnalysisRequested"}]
        await self._record_output_written(written, "Document processing complete. Credit analysis requested.")
        
        ms = int((time.time() - t0) * 1000)
        await self._record_node_execution("write_output", ["verified"], ["next"], ms)
        
        state["output_events_written"] = written
        state["next_agent_triggered"] = "credit_analysis"
        return state
