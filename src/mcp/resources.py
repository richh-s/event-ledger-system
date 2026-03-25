from __future__ import annotations
import json
from typing import Dict, Any, List, Optional
from src.database import get_db
from src.event_store import EventStore

async def get_application_summary(application_id: str) -> Dict[str, Any]:
    """Resource 1: ledger://applications/{id} → ApplicationSummary projection."""
    db = get_db()
    async with db.get_connection() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            application_id
        )
        if not row:
            return {"error": "Application not found"}
        return dict(row)

async def get_compliance_view(application_id: str, timestamp: Optional[str] = None) -> Dict[str, Any]:
    """Resource 2: ledger://applications/{id}/compliance → ComplianceAuditView."""
    db = get_db()
    async with db.get_connection() as conn:
        # If timestamp is provided, we'd need to convert it to a global position or use a new method.
        # Rubric: ComplianceAuditView projection with temporal query support (get_compliance_at(application_id, timestamp))
        if timestamp:
            from src.projections.compliance_audit import ComplianceAuditViewProjection
            from datetime import datetime
            ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            pos = await conn.fetchval(
                "SELECT MAX(global_position) FROM events WHERE stream_id = $1 AND recorded_at <= $2",
                f"compliance-{application_id}", ts
            )
            if pos is None:
                return {"application_id": application_id, "rules": [], "status": "no_data"}
            rules = await ComplianceAuditViewProjection.get_compliance_at(conn, application_id, pos)
            return {
                "application_id": application_id,
                "rules": rules,
                "has_hard_block": any(r.get("is_hard_block", False) for r in rules),
            }
        
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

async def get_application_audit_trail(application_id: str) -> Dict[str, Any]:
    """Resource 3: ledger://applications/{id}/audit-trail → direct stream read (Justified Exception)."""
    db = get_db()
    store = EventStore(db)
    stream_id = f"loan-{application_id}"
    events = await store.load_stream(stream_id)
    if not events: return {"error": "Application stream not found"}
    return {
        "application_id": application_id,
        "events": [
            {"event_id": str(e.event_id), "event_type": e.event_type, "payload": e.payload, "recorded_at": str(e.recorded_at)}
            for e in events
        ]
    }

async def get_agent_performance(agent_id: str) -> List[Dict[str, Any]]:
    """Resource 4: ledger://agents/{id}/performance → AgentPerformanceLedger."""
    db = get_db()
    async with db.get_connection() as conn:
        rows = await conn.fetch(
            "SELECT * FROM agent_performance_ledger WHERE agent_id = $1 ORDER BY last_seen_at DESC",
            agent_id
        )
        return [dict(r) for r in rows]

async def get_agent_session_trace(session_id: str) -> Dict[str, Any]:
    """Resource 5: ledger://agents/{id}/sessions/{session_id} → direct stream read (Justified Exception)."""
    db = get_db()
    store = EventStore(db)
    # Replay session stream
    for prefix in ["agent-credit_analysis-", "agent-compliance-", "agent-fraud_detection-", "agent-document_processing-"]:
        events = await store.load_stream(f"{prefix}{session_id}")
        if events:
            return {
                "session_id": session_id,
                "events": [{"event_type": e.event_type, "payload": e.payload} for e in events]
            }
    return {"error": "Session stream not found"}

async def get_health_metrics() -> Dict[str, Any]:
    """Resource 6: ledger://ledger/health → projection lag + DLQ count."""
    db = get_db()
    async with db.get_connection() as conn:
        latest_pos = await conn.fetchval("SELECT MAX(global_position) FROM events") or 0
        rows = await conn.fetch("SELECT * FROM projection_checkpoints")
        dlq_count = await conn.fetchval("SELECT COUNT(*) FROM dead_letter_queue")
        return {
            "status": "healthy" if dlq_count == 0 else "degraded",
            "global_position": latest_pos,
            "projections": [{"name": r['projection_name'], "lag": latest_pos - r['last_position']} for r in rows],
            "dead_letter_count": dlq_count
        }
