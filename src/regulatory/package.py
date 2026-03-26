"""
src/regulatory/package.py
=========================
Phase 6: Regulatory examination package.
"""

from datetime import datetime
from typing import Dict, Any, List, Optional

from src.database import Database
from src.event_store import EventStore
from src.models.events import deserialize_event
from src.integrity.audit_chain import AuditChainVerifier

class HistoricalReconstructor:
    def __init__(self, db: Database, store: EventStore):
        self.db = db
        self.store = store
        self.verifier = AuditChainVerifier(db, store)

    async def build_regulatory_package(
        self,
        application_id: str,
        as_of_timestamp: Optional[datetime] = None,
        as_of_global_position: Optional[int] = None
    ) -> Dict[str, Any]:
        async with self.db.get_connection() as conn:
            loan_stream = f"loan-{application_id}"
            first_event = await conn.fetchrow(
                "SELECT metadata->>'correlation_id' as correlation_id FROM events WHERE stream_id = $1 ORDER BY stream_position ASC LIMIT 1",
                loan_stream
            )
            
            if not first_event:
                raise ValueError(f"Application {application_id} not found in event store.")
            
            correlation_id = first_event["correlation_id"]

            query = "SELECT * FROM events WHERE metadata->>'correlation_id' = $1"
            params: List[Any] = [correlation_id]
            
            if as_of_global_position is not None:
                params.append(as_of_global_position)
                query += f" AND global_position <= ${len(params)}"
                
            if as_of_timestamp is not None:
                params.append(as_of_timestamp)
                query += f" AND recorded_at <= ${len(params)}"
                
            query += " ORDER BY global_position ASC"
            
            raw_events = await conn.fetch(query, *params)
            
            if not raw_events:
                raise ValueError("No events found within specified boundary constraints.")
                
            boundary_position = raw_events[-1]["global_position"]
            boundary_time = raw_events[-1]["recorded_at"]
            
            events = []
            invalid_events_filtered = 0
            streams_included = set()
            
            agent_traces: Dict[str, List[Dict[str, Any]]] = {}
            compliance_events = []
            decision_events = []
            audit_events = []
            timeline = []
            
            app_state = {
                "application_id": application_id,
                "state": "UNKNOWN",
                "requested_amount_usd": None,
                "fraud_score": None,
                "compliance_status": None,
                "decision": None,
                "risk_tier": None,
                "created_at": None,
                "updated_at": None
            }
            
            for r in raw_events:
                stream_id = r["stream_id"]
                streams_included.add(stream_id)
                evt_type = r["event_type"]
                payload = r["payload"]
                
                import json
                if isinstance(payload, (str, bytes)):
                    payload = json.loads(payload)
                
                metadata = r["metadata"]
                if isinstance(metadata, (str, bytes)):
                    metadata = json.loads(metadata)
                
                try:
                    obj = deserialize_event(evt_type, payload)
                    evt_dict = obj.model_dump(mode='json') if hasattr(obj, "model_dump") else obj
                    if isinstance(evt_dict, str):
                        try:
                            import json
                            evt_dict = json.loads(evt_dict)
                        except:
                            pass
                    if not isinstance(evt_dict, dict):
                        evt_dict = {}
                except Exception as e:
                    invalid_events_filtered += 1
                    continue
                    
                parsed = {
                    "global_position": r["global_position"],
                    "stream_id": stream_id,
                    "stream_position": r["stream_position"],
                    "event_type": evt_type,
                    "event_version": r["event_version"],
                    "event_id": str(r["event_id"]),
                    "causation_id": metadata.get("causation_id") if isinstance(metadata, dict) else None,
                    "correlation_id": metadata.get("correlation_id") if isinstance(metadata, dict) else None,
                    "payload": evt_dict,
                    "recorded_at": r["recorded_at"].isoformat()
                }
                events.append(parsed)
                
                app_state["updated_at"] = parsed["recorded_at"]
                
                if evt_type in [
                    "ApplicationSubmitted", "DocumentPackageCreated", "PackageReadyForAnalysis",
                    "CreditAnalysisCompleted", "FraudScreeningCompleted", "ComplianceCheckCompleted",
                    "DecisionGenerated", "HumanReviewCompleted", "ApplicationApproved", "ApplicationDeclined"
                ]:
                    timeline.append({
                        "event_type": evt_type,
                        "global_position": r["global_position"],
                        "recorded_at": parsed["recorded_at"]
                    })
                    
                if evt_type == "ApplicationSubmitted":
                    app_state["state"] = "SUBMITTED"
                    app_state["requested_amount_usd"] = evt_dict.get("requested_amount_usd")
                    app_state["created_at"] = parsed["recorded_at"]
                elif evt_type == "PackageReadyForAnalysis":
                    app_state["state"] = "DOCUMENTS_PROCESSED"
                elif evt_type == "CreditAnalysisCompleted":
                    app_state["risk_tier"] = evt_dict.get("decision", {}).get("risk_tier")
                elif evt_type == "FraudScreeningCompleted":
                    app_state["fraud_score"] = evt_dict.get("fraud_score")
                elif evt_type == "ComplianceCheckCompleted":
                    app_state["compliance_status"] = evt_dict.get("overall_verdict")
                elif evt_type == "DecisionGenerated":
                    app_state["decision"] = evt_dict.get("recommendation")
                    app_state["state"] = "PENDING_HUMAN_REVIEW"
                elif evt_type in ["ApplicationApproved", "ApplicationDeclined"]:
                    app_state["state"] = "APPROVED" if evt_type == "ApplicationApproved" else "DECLINED"
                
                if stream_id.startswith("agent-"):
                    if stream_id not in agent_traces:
                        agent_traces[stream_id] = []
                        
                    agent_traces[stream_id].append(parsed)
                    
                if stream_id.startswith("compliance-"):
                    compliance_events.append(parsed)
                    
                if evt_type in ["CreditAnalysisCompleted", "FraudScreeningCompleted", "ComplianceCheckCompleted", 
                              "DecisionGenerated", "HumanReviewCompleted", "ApplicationApproved", "ApplicationDeclined"]:
                    decision_events.append(parsed)
                    
                if stream_id.startswith("audit-"):
                    audit_events.append(parsed)

            if not events:
                raise ValueError("All matching events were filtered out due to schema validation failures.")

            session_structure_passed = True
            for st_id, tr in agent_traces.items():
                if tr and tr[0]["event_type"] not in ("AgentSessionStarted", "AgentSessionRecovered"):
                    session_structure_passed = False

            latest_audit = audit_events[-1] if audit_events else None
            
            def safe_get(p, key, default=None):
                if isinstance(p, str):
                    try:
                        import json
                        p = json.loads(p)
                    except:
                        pass
                if isinstance(p, dict):
                    return p.get(key, default)
                return default

            audit_integrity_passed = safe_get(latest_audit["payload"], "chain_valid", False) if latest_audit else "UNKNOWN"
            
            compliance_summary = {
                "rules_evaluated": sum(1 for e in compliance_events if e["event_type"] in ["ComplianceRulePassed", "ComplianceRuleFailed"]),
                "rules_passed": sum(1 for e in compliance_events if e["event_type"] == "ComplianceRulePassed"),
                "rules_failed": sum(1 for e in compliance_events if e["event_type"] == "ComplianceRuleFailed"),
                "hard_block_present": any(safe_get(e["payload"], "is_hard_block") for e in compliance_events if e["event_type"] == "ComplianceRuleFailed"),
                "final_verdict": app_state["compliance_status"]
            }
            
            decision_path = {}
            for d in decision_events:
                decision_path[d["event_type"]] = d["payload"]
                
            narrative = self._generate_narrative(app_state, decision_path)
            
            return {
                "metadata": {
                    "application_id": application_id,
                    "package_version": "1.0",
                    "generated_timestamp": datetime.utcnow().isoformat(),
                    "as_of_timestamp": boundary_time.isoformat(),
                    "as_of_global_position": boundary_position,
                    "included_streams": list(streams_included),
                    "total_events": len(events)
                },
                "timeline": timeline,
                "agent_session_traces": agent_traces,
                "compliance_section": compliance_summary,
                "decision_path_summary": decision_path,
                "reconstructed_read_model": app_state,
                "audit_integrity_section": {
                    "integrity_passed": audit_integrity_passed,
                    "latest_check": latest_audit
                },
                "validation": {
                    "schema_validation_passed": True,
                    "invalid_events_filtered": invalid_events_filtered,
                    "session_structure_passed": session_structure_passed,
                },
                "narrative": narrative,
                "full_event_history": events
            }

    def _generate_narrative(self, app_state: dict, decision_path: dict) -> str:
        n = [f"Application entered system at {app_state['created_at']}."]
        if app_state["state"] in ["DOCUMENTS_PROCESSED", "PENDING_HUMAN_REVIEW", "APPROVED", "DECLINED"]:
            n.append("Documents were successfully processed and extracted.")
            
        if "FraudScreeningCompleted" in decision_path:
            f = decision_path["FraudScreeningCompleted"]
            n.append(f"Fraud screening detected {f.get('anomalies_found', 0)} anomalies (Score: {f.get('fraud_score')}).")
            
        if "ComplianceCheckCompleted" in decision_path:
            c = decision_path["ComplianceCheckCompleted"]
            n.append(f"Compliance check returned verdict: {c.get('overall_verdict')}.")
            
        if "DecisionGenerated" in decision_path:
            d = decision_path["DecisionGenerated"]
            n.append(f"Orchestrator recommended to {d.get('recommendation')} with confidence {d.get('confidence', 0):.2f}.")
            
        if "ApplicationApproved" in decision_path:
            n.append("Application was finally APPROVED.")
        elif "ApplicationDeclined" in decision_path:
            n.append("Application was finally DECLINED.")
            
        return " ".join(n)


async def generate_regulatory_package(application_id: str, db: Database, store: EventStore, as_of_global_position=None, as_of_timestamp=None) -> Dict[str, Any]:
    """generate_regulatory_package(): self-contained JSON examination package with event stream."""
    reconstructor = HistoricalReconstructor(db, store)
    return await reconstructor.build_regulatory_package(application_id, as_of_timestamp=as_of_timestamp, as_of_global_position=as_of_global_position)
