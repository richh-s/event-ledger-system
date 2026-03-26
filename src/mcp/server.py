from __future__ import annotations
import asyncio
import logging
from fastmcp import FastMCP

from src.mcp.tools import (
    submit_application, record_credit_analysis, record_fraud_screening,
    record_compliance_check, generate_decision, record_human_review,
    start_agent_session, run_integrity_check
)
from src.mcp.resources import (
    get_application_summary, get_compliance_view, get_application_audit_trail,
    get_agent_performance, get_agent_session_trace, get_health_metrics
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("apex-mcp")

# Initialize MCP
mcp = FastMCP("ApexLedger", port=8765)

# Register Tools
mcp.tool()(submit_application)
mcp.tool()(record_credit_analysis)
mcp.tool()(record_fraud_screening)
mcp.tool()(record_compliance_check)
mcp.tool()(generate_decision)
mcp.tool()(record_human_review)
mcp.tool()(start_agent_session)
mcp.tool()(run_integrity_check)

# Register Resources
mcp.resource("ledger://applications/{application_id}")(get_application_summary)
mcp.resource("ledger://applications/{application_id}/compliance")(get_compliance_view)
mcp.resource("ledger://applications/{application_id}/audit-trail")(get_application_audit_trail)
mcp.resource("ledger://agents/{agent_id}/performance")(get_agent_performance)
mcp.resource("ledger://agents/{agent_id}/sessions/{session_id}")(get_agent_session_trace)
mcp.resource("ledger://ledger/health")(get_health_metrics)

if __name__ == "__main__":
    logger.info("Starting Apex MCP Server on port 8765...")
    mcp.run()
