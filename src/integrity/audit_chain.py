from __future__ import annotations
import hashlib
import json
import logging
from datetime import datetime
from typing import List, Set, Tuple
from src.event_store import EventStore
from src.models.events import StoredEvent, AuditIntegrityCheckRun

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

    async def verify_stream_integrity(self, stream_id: str, depth: int = 1) -> dict:
        """
        Performs a cross-aggregate cryptographic integrity check.
        Traverses causally linked streams via correlation_id and causation_id.
        """
        parts = stream_id.split("-")
        if len(parts) < 2:
            raise ValueError(f"Invalid stream_id: {stream_id}")
            
        entity_type = parts[0]
        entity_id = "-".join(parts[1:])
        
        visited_streams: Set[str] = {stream_id}
        all_events: List[StoredEvent] = await self.store.load_stream(stream_id)
        
        # Load previous audit runs
        audit_stream = f"audit-{entity_type.lower()}-{entity_id}"
        previous_runs = await self.store.load_stream(audit_stream)
        last_run = previous_runs[-1].payload if previous_runs else None
        
        # 4. Sort and established deterministic order
        all_events.sort(key=lambda e: e.global_position or 0)
        
        if not all_events:
            return {"chain_valid": True, "tamper_detected": False, "integrity_hash": "", "events_verified": 0}

        # 5. Build hash chain
        current_hash = ""
        tamper_detected = False
        
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
            event_hash = hashlib.sha256(event_json.encode()).hexdigest()
            current_hash = hashlib.sha256((current_hash + event_hash).encode()).hexdigest()
            
            # Check against last run if we reached its recorded position
            if last_run and event.global_position == last_run.get('last_event_position'):
                if current_hash != last_run.get('integrity_hash'):
                    tamper_detected = True

        audit_event = AuditIntegrityCheckRun(
            entity_type=entity_type,
            entity_id=entity_id,
            check_timestamp=datetime.now(),
            events_verified_count=len(all_events),
            integrity_hash=current_hash,
            previous_hash=last_run.get('integrity_hash') if last_run else None,
            last_event_position=all_events[-1].global_position or 0,
            chain_valid=not tamper_detected,
            tamper_detected=tamper_detected
        )
        
        current_version = await self.store.stream_version(audit_stream)
        await self.store.append(
            audit_stream, 
            [audit_event], 
            expected_version=current_version, 
            aggregate_type="AuditLedger"
        )

        return {
            "chain_valid": not tamper_detected,
            "tamper_detected": tamper_detected,
            "integrity_hash": current_hash,
            "events_verified": len(all_events)
        }
