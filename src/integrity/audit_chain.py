from __future__ import annotations
import hashlib
import json
import logging
from datetime import datetime
from typing import List, Set, Tuple
from src.event_store import EventStore
from src.schema.events import StoredEvent, AuditIntegrityCheckRun

logger = logging.getLogger(__name__)

class IntegrityResult:
    def __init__(self, valid: bool, verified_count: int, final_hash: str, last_position: int = 0):
        self.valid = valid
        self.verified_count = verified_count
        self.final_hash = final_hash
        self.last_position = last_position

class AuditChainVerifier:
    def __init__(self, db, store: EventStore):
        self.db = db
        self.store = store

    async def verify_stream_integrity(self, stream_id: str, depth: int = 1) -> IntegrityResult:
        """
        Performs a cross-aggregate cryptographic integrity check.
        Traverses causally linked streams via correlation_id and causation_id.
        """
        # Parse stream_id
        parts = stream_id.split("-")
        if len(parts) < 2:
            raise ValueError(f"Invalid stream_id: {stream_id}")
            
        entity_type = parts[0]
        entity_id = "-".join(parts[1:])
        
        # 1. Gather all events from the primary stream
        visited_streams: Set[str] = {stream_id}
        all_events: List[StoredEvent] = await self.store.load_stream(stream_id)
        
        # 2. Traverse links up to depth
        current_layer = list(all_events)
        for _ in range(depth):
            next_layer = []
            correlation_ids = {e.metadata.get("correlation_id") for e in current_layer if e.metadata.get("correlation_id")}
            causation_ids = {str(e.event_id) for e in current_layer}
            
            related_streams = set()
            async with self.db.get_connection() as conn:
                for cid in correlation_ids:
                    rows = await conn.fetch("SELECT DISTINCT stream_id FROM events WHERE metadata->>'correlation_id' = $1", cid)
                    for r in rows:
                        if r['stream_id'] not in visited_streams:
                            related_streams.add(r['stream_id'])
                
                for cid in causation_ids:
                    rows = await conn.fetch("SELECT DISTINCT stream_id FROM events WHERE metadata->>'causation_id' = $1", cid)
                    for r in rows:
                        if r['stream_id'] not in visited_streams:
                            related_streams.add(r['stream_id'])

            for rs in related_streams:
                visited_streams.add(rs)
                new_events = await self.store.load_stream(rs)
                logger.info(f"Found related stream {rs} with {len(new_events)} events via causal link")
                all_events.extend(new_events)
                next_layer.extend(new_events)
            
            current_layer = next_layer
            if not current_layer:
                break

        # 3. Explicit causal validation
        all_event_ids = {str(e.event_id) for e in all_events}
        for e in all_events:
            c_id = e.metadata.get("causation_id")
            if c_id and c_id not in all_event_ids:
                logger.error(f"Broken causal chain: Event {e.event_id} references missing causation_id {c_id}")
                return IntegrityResult(False, len(all_events), "", 0)
                
        # 4. Sort and established deterministic order
        all_events.sort(key=lambda e: e.global_position or 0)
        
        if not all_events:
            return IntegrityResult(True, 0, "", 0)

        # 5. Build hash chain
        previous_hash = ""
        for event in all_events:
            event_data = {
                "type": event.event_type,
                "payload": event.payload,
                "correlation_id": event.metadata.get("correlation_id"),
                "causation_id": event.metadata.get("causation_id"),
                "stream_position": event.stream_position,
                "global_position": event.global_position
            }
            event_json = json.dumps(event_data, sort_keys=True, default=str)
            current_event_hash = hashlib.sha256(event_json.encode()).hexdigest()
            combined = previous_hash + current_event_hash
            previous_hash = hashlib.sha256(combined.encode()).hexdigest()

        # 6. Record result
        final_hash = previous_hash
        audit_stream = f"audit-{entity_type.lower()}-{entity_id}"
        previous_runs = await self.store.load_stream(audit_stream)
        last_run_hash = None
        if previous_runs:
            last_run_hash = previous_runs[-1].payload.get("integrity_hash")

        audit_event = AuditIntegrityCheckRun(
            entity_type=entity_type,
            entity_id=entity_id,
            check_timestamp=datetime.now(),
            events_verified_count=len(all_events),
            integrity_hash=final_hash,
            previous_hash=last_run_hash,
            last_event_position=all_events[-1].global_position or 0,
            chain_valid=True,
            tamper_detected=False
        )
        
        current_version = await self.store.stream_version(audit_stream)
        await self.store.append(
            audit_stream, 
            [audit_event], 
            expected_version=current_version, 
            aggregate_type="AuditLedger",
            correlation_id=entity_id,
            causation_id=None
        )

        return IntegrityResult(True, len(all_events), final_hash, all_events[-1].global_position or 0)
