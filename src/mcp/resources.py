from __future__ import annotations
import json
import typing
from src.database import Database
from src.event_store import EventStore

# ============================================================================
# MCP Resource Implementations (5 read-only resources)
# ============================================================================


async def get_application_summary(application_id: str):
    """Resource 1: ledger://applications/{id} → ApplicationSummary projection."""
    from src.database import get_db
    db = get_db()
    async with db.get_connection() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            application_id
        )
        if not row:
            return {"error": "Application not found"}
        return dict(row)


async def get_compliance_current(application_id: str):
    """Resource 2: ledger://applications/{id}/compliance → ComplianceAuditView (current)."""
    from src.database import get_db
    db = get_db()
    async with db.get_connection() as conn:
        rows = await conn.fetch(
            "SELECT * FROM compliance_audit_view WHERE application_id = $1 ORDER BY recorded_at ASC",
            application_id
        )
        if not rows:
            return {"application_id": application_id, "rules": [], "status": "no_data"}
        return {
            "application_id": application_id,
            "rules": [dict(r) for r in rows],
            "has_hard_block": any(r["is_hard_block"] for r in rows),
        }


async def get_compliance_at(application_id: str, as_of_position: int):
    """
    Resource 2 (temporal variant): Snapshot-based temporal query.
    Returns compliance state as it existed at a specific global_position.
    Uses ComplianceAuditViewProjection.get_compliance_at() internally.
    """
    from src.database import get_db
    from src.projections.compliance_view import ComplianceAuditViewProjection
    db = get_db()
    async with db.get_connection() as conn:
        rules = await ComplianceAuditViewProjection.get_compliance_at(
            conn, application_id, as_of_position
        )
        return {
            "application_id": application_id,
            "as_of_position": as_of_position,
            "rules": rules,
        }


async def get_agent_performance(agent_id: str):
    """Resource 3: ledger://agents/{id}/performance → AgentPerformanceLedger."""
    from src.database import get_db
    db = get_db()
    async with db.get_connection() as conn:
        rows = await conn.fetch(
            "SELECT * FROM agent_performance_ledger WHERE agent_id = $1 ORDER BY last_seen_at DESC",
            agent_id
        )
        if not rows:
            return {"error": "No performance data for agent"}
        return [dict(r) for r in rows]


async def get_agent_session_trace(session_id: str):
    """Resource 4: ledger://agents/{id}/sessions/{session_id} → direct stream read."""
    from src.database import get_db
    db = get_db()
    store = EventStore(db)

    # Read from the agent session stream directly (event-sourced, not projected)
    stream_candidates = [
        f"agent-credit_analysis-{session_id}",
        f"agent-compliance-{session_id}",
        f"agent-fraud_detection-{session_id}",
        f"agent-document_processing-{session_id}",
        f"session-{session_id}",
    ]

    for stream_id in stream_candidates:
        events = await store.load_stream(stream_id)
        if events:
            return {
                "session_id": session_id,
                "stream_id": stream_id,
                "events": [
                    {
                        "event_id": str(e.event_id),
                        "event_type": e.event_type,
                        "payload": e.payload,
                        "recorded_at": str(e.recorded_at),
                        "global_position": e.global_position,
                    }
                    for e in events
                ]
            }

    return {"error": f"No session stream found for {session_id}"}


async def get_health_metrics():
    """Resource 5: ledger://ledger/health → projection lag + DLQ count."""
    from src.database import get_db
    db = get_db()
    async with db.get_connection() as conn:
        latest_pos = await conn.fetchval("SELECT MAX(global_position) FROM events") or 0
        rows = await conn.fetch("SELECT * FROM projection_checkpoints")
        dlq_count = await conn.fetchval("SELECT COUNT(*) FROM dead_letter_queue")

        projs = []
        for r in rows:
            lag = latest_pos - r['last_position']
            projs.append({
                "name": r['projection_name'],
                "last_processed": r['last_position'],
                "lag": lag
            })

        return {
            "status": "healthy" if dlq_count == 0 else "degraded",
            "global_position": latest_pos,
            "projections": projs,
            "dead_letter_count": dlq_count
        }
