"""
src/server.py
=============
Phase 5 Master MCP Server for Apex Financial Services.
Exposes Decision History, Causal Traversal, and Integrity Verification.
Port: 8765
"""
import asyncio
import logging
import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

from fastmcp import FastMCP
from src.database import Database
from src.event_store import EventStore
from src.ledger.schema.events import (
    ApplicationSubmitted, 
    AuditIntegrityCheckRun,
    BaseEvent,
    deserialize_event
)
from src.integrity.audit_chain import AuditChainVerifier
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_decision_trace import AgentDecisionTraceProjection
from src.projections.audit_registry import AuditRegistryProjection
from src.queries.historical_reconstruction import HistoricalReconstructor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("apex-mcp")

# Initialize MCP
mcp = FastMCP("ApexLedger", port=8765)

# Global instances (initialized in start)
db: Optional[Database] = None
store: Optional[EventStore] = None
daemon: Optional[ProjectionDaemon] = None

@mcp.tool()
async def record_application(
    application_id: str,
    applicant_name: str,
    requested_amount: float,
    loan_purpose: str = "working_capital"
) -> str:
    """Submit a new loan application to the ledger."""
    evt = ApplicationSubmitted(
        application_id=application_id,
        applicant_name=applicant_name,
        requested_amount_usd=requested_amount,
        loan_purpose=loan_purpose,
        submitted_at=datetime.utcnow()
    )
    
    await store.append(
        stream_id=f"loan-{application_id}",
        events=[evt],
        expected_version=-1, # Must be new
        correlation_id=f"corr-{application_id[0:8]}",
        aggregate_type="LoanApplication"
    )
    return f"Application {application_id} recorded in ledger."

@mcp.tool()
async def get_decision_history(application_id: str) -> Dict[str, Any]:
    """
    TRAVERSAL starting from LoanApplication roots via correlation_id graph.
    Returns the full decision history of application X in under 60 seconds.
    """
    stream_id = f"loan-{application_id}"
    events = await store.load_stream(stream_id)
    if not events:
        return {"error": "Application not found"}
        
    # Get correlation_id from the first event (ApplicationSubmitted)
    first_event = events[0]
    corr_id = first_event.metadata.get("correlation_id")
    
    # Query ALL events with the same correlation_id (Causal Linking Rule 1)
    async with db.get_connection() as conn:
        all_related = await conn.fetch(
            "SELECT * FROM events WHERE correlation_id = $1 ORDER BY global_position ASC",
            corr_id
        )
        
    history = []
    for r in all_related:
        evt = deserialize_event(r["event_type"], r["payload"])
        history.append({
            "event_id": str(r["event_id"]),
            "type": r["event_type"],
            "stream": r["stream_id"],
            "payload": r["payload"],
            "causation_id": str(r["causation_id"]) if r["causation_id"] else None,
            "recorded_at": r["recorded_at"].isoformat()
        })
        
    return {
        "application_id": application_id,
        "correlation_id": corr_id,
        "event_count": len(history),
        "history": history
    }

@mcp.tool()
async def run_integrity_check(stream_id: str) -> Dict[str, Any]:
    """
    Perform cryptographic audit integrity verification on a stream.
    Persists the result to the audit stream.
    """
    verifier = AuditChainVerifier(db, store)
    result = await verifier.verify_stream_integrity(stream_id)
    
    # Persist the check run back to the ledger (Phase 4 requirement)
    parts = stream_id.split("-")
    entity_type = parts[0]
    entity_id = "-".join(parts[1:])
    
    audit_stream = f"audit-{entity_type}-{entity_id}"
    ver = await store.stream_version(audit_stream)
    
    audit_evt = AuditIntegrityCheckRun(
        entity_id=entity_id,
        entity_type=entity_type,
        check_timestamp=datetime.utcnow(),
        events_verified_count=result["count"],
        integrity_hash=result["hash"],
        previous_hash=None, # In a real system, we'd lookup previous hash
        last_event_position=result["last_position"]
    )
    
    await store.append(
        stream_id=audit_stream,
        events=[audit_evt],
        expected_version=ver,
        correlation_id=f"audit-{uuid.uuid4().hex[0:8]}",
        aggregate_type="AuditRegistry"
    )
    
    return result

@mcp.tool()
async def get_projection_lag() -> Dict[str, int]:
    """Check the processing lag of all active projections."""
    lags = {}
    for name in ["application_summary", "agent_decision_trace", "audit_registry"]:
        lags[name] = await daemon.get_lag(name)
    return lags

@mcp.resource("ledger://applications/{id}/history")
async def application_history_resource(id: str) -> str:
    """Gets the temporal reconstruction history for an application."""
    reconstructor = HistoricalReconstructor(db, store)
    try:
        data = await reconstructor.build_regulatory_package(id)
        return json.dumps({
            "timeline": data["timeline"],
            "reconstructed_read_model": data["reconstructed_read_model"],
            "event_count": data["metadata"]["total_events"],
            "included_streams": data["metadata"]["included_streams"]
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})

@mcp.resource("ledger://applications/{id}/regulatory-package")
async def regulatory_package_resource(id: str) -> str:
    """Returns the comprehensive phase 6 regulatory examination package."""
    reconstructor = HistoricalReconstructor(db, store)
    try:
        package = await reconstructor.build_regulatory_package(id)
        # Exclude the massive full_event_history for the string resource view to keep it crisp,
        # but the JSON structure is preserved
        package.pop("full_event_history", None)
        return json.dumps(package, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})

@mcp.tool()
async def recompute_credit_decision(
    application_id: str,
    alternate_model: str,
    as_of_global_position: Optional[int] = None
) -> Dict[str, Any]:
    """
    Phase 6 aligned temporal query "what-if" tool.
    Recomputes the credit decision locally using an alternate model 
    valuing historical facts without polluting the event stream.
    """
    reconstructor = HistoricalReconstructor(db, store)
    return await reconstructor.what_if_credit_recomputation(
        application_id, 
        alternate_model, 
        as_of_global_position
    )

async def start():
    global db, store, daemon
    db = Database()
    await db.connect()
    
    store = EventStore(db)
    
    # Initialize Projections
    projections = [
        ApplicationSummaryProjection(),
        AgentDecisionTraceProjection(),
        AuditRegistryProjection()
    ]
    
    daemon = ProjectionDaemon(db, store, projections)
    await daemon.start()
    
    logger.info("Apex MCP Server initialized.")

if __name__ == "__main__":
    asyncio.run(start())
