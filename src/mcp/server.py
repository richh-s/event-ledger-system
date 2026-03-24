from __future__ import annotations
import json
from mcp.server.fastmcp import FastMCP

# Define the server
mcp = FastMCP("EventLedger")

# Import logic from tools.py and resources.py
from src.mcp import tools, resources

# ==============================================================================
# TOOLS (8 COMMANDS) — write-side aggregate invocations
# ==============================================================================

@mcp.tool()
async def submit_application(applicant_id: str, amount: float, purpose: str, contact_name: str, contact_email: str) -> str:
    """Submit a loan application."""
    return await tools.submit_application(applicant_id, amount, purpose, contact_name, contact_email)

@mcp.tool()
async def upload_document(application_id: str, document_type: str, filename: str, file_path: str) -> str:
    """Record a document upload."""
    return await tools.upload_document(application_id, document_type, filename, file_path)

@mcp.tool()
async def run_document_processing(application_id: str) -> str:
    """Invoke document processing agent."""
    return await tools.run_document_processing(application_id)

@mcp.tool()
async def run_credit_analysis(application_id: str) -> str:
    """Invoke credit analysis agent."""
    return await tools.run_credit_analysis(application_id)

@mcp.tool()
async def run_fraud_check(application_id: str) -> str:
    """Invoke fraud detection agent."""
    return await tools.run_fraud_check(application_id)

@mcp.tool()
async def run_compliance_check(application_id: str) -> str:
    """Invoke compliance agent."""
    return await tools.run_compliance_check(application_id)

@mcp.tool()
async def generate_decision(application_id: str) -> str:
    """Generate final decision."""
    return await tools.generate_decision(application_id)

@mcp.tool()
async def complete_human_review(application_id: str, verdict: str, reason: str) -> str:
    """Complete manual review."""
    return await tools.complete_human_review(application_id, verdict, reason)


# ==============================================================================
# RESOURCES (6 READ-SIDE) — spec-aligned projection queries
# ==============================================================================

# 1. ledger://applications/{id} → ApplicationSummary
@mcp.resource("ledger://applications/{id}")
async def get_application_summary(id: str) -> str:
    """Read-optimized current application state from ApplicationSummary projection."""
    data = await resources.get_application_summary(id)
    return json.dumps(data, indent=2, default=str)

# 2. ledger://applications/{id}/compliance → ComplianceAuditView
@mcp.resource("ledger://applications/{id}/compliance")
async def get_compliance(id: str, as_of: int | None = None) -> str:
    """
    Current compliance rule verdicts for an application.
    Supports ?as_of={global_position} for temporal queries.
    """
    if as_of is not None:
        data = await resources.get_compliance_at(id, as_of)
    else:
        data = await resources.get_compliance_current(id)
    return json.dumps(data, indent=2, default=str)

# 3. ledger://applications/{id}/audit-trail → direct event store read (exception)
@mcp.resource("ledger://applications/{id}/audit-trail")
async def get_application_audit_trail(id: str) -> str:
    """Raw, immutable event stream for an application (direct event store read)."""
    data = await resources.get_application_audit_trail(id)
    return json.dumps(data, indent=2, default=str)

# 4. ledger://agents/{id}/performance → AgentPerformanceLedger
@mcp.resource("ledger://agents/{id}/performance")
async def get_agent_performance(id: str) -> str:
    """Aggregated performance metrics per model version for an agent."""
    data = await resources.get_agent_performance(id)
    return json.dumps(data, indent=2, default=str)

# 5. ledger://agents/{id}/sessions/{session_id} → direct stream read (exception)
@mcp.resource("ledger://agents/{id}/sessions/{session_id}")
async def get_session_trace(id: str, session_id: str) -> str:
    """Isolated event stream for a specific agent session."""
    data = await resources.get_agent_session_trace(session_id)
    return json.dumps(data, indent=2, default=str)

# 6. ledger://ledger/health → projection lag + DLQ
@mcp.resource("ledger://ledger/health")
async def get_system_health() -> str:
    """Projection lag metrics and dead letter queue count."""
    data = await resources.get_health_metrics()
    return json.dumps(data, indent=2, default=str)


if __name__ == "__main__":
    mcp.run()
