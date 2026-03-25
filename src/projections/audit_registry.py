from __future__ import annotations
import logging
from src.projections.base import BaseProjection
from src.models import StoredEvent

logger = logging.getLogger(__name__)

class AuditRegistryProjection(BaseProjection):
    """Projection 3: Current status of integrity for all streams."""
    @property
    def projection_name(self) -> str:
        return "audit_registry"

    async def handle_event(self, conn, event: StoredEvent) -> None:
        # We only care about AuditIntegrityCheckRun events
        if event.event_type != "AuditIntegrityCheckRun":
            return
            
        p = event.payload
        metadata = event.metadata or {}
        correlation_id = metadata.get('correlation_id')
        
        # entity_id is derived from stream_id: "audit-{entity_type}-{entity_id}"
        # Wait, the event store schema says: 
        # stream_id = f"audit-{entity_type}-{entity_id}"
        # But we want to know WHICH stream was verified.
        # AuditIntegrityCheckRun payload has: entity_id, entity_type (from Phase 4 implem)
        
        entity_id = p.get('entity_id')
        entity_type = p.get('entity_type')
        if not entity_id or not entity_type:
            return
            
        target_stream_id = f"{entity_type}-{entity_id}"
        
        await conn.execute(
            """
            INSERT INTO audit_registry_view (
                stream_id, aggregate_type, last_verified_version, 
                last_verified_at, integrity_hash, events_verified_count, 
                check_status, last_correlation_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'VERIFIED', $7)
            ON CONFLICT (stream_id) DO UPDATE SET
                last_verified_version = EXCLUDED.last_verified_version,
                last_verified_at = EXCLUDED.last_verified_at,
                integrity_hash = EXCLUDED.integrity_hash,
                events_verified_count = EXCLUDED.events_verified_count,
                check_status = 'VERIFIED',
                last_correlation_id = EXCLUDED.last_correlation_id
            """,
            target_stream_id, entity_type, p.get('last_event_position'),
            event.recorded_at, p.get('integrity_hash'), 
            p.get('events_verified_count'), correlation_id
        )

    async def rebuild_from_scratch(self, conn) -> None:
        await conn.execute("TRUNCATE audit_registry_view")
        await conn.execute(
            "UPDATE projection_checkpoints SET last_position = 0 WHERE projection_name = $1",
            self.projection_name
        )
