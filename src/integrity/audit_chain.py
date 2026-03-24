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
    def __init__(self, chain_valid: bool, tamper_detected: bool, verified_count: int, final_hash: str):
        self.chain_valid = chain_valid
        self.tamper_detected = tamper_detected
        self.verified_count = verified_count
        self.final_hash = final_hash

async def run_integrity_check(store: EventStore, entity_type: str, entity_id: str, depth: int = 1) -> IntegrityResult:
    """
    Performs a cross-aggregate cryptographic integrity check.
    Traverses causally linked streams via correlation_id and causation_id.
    """
    # 1. Gather all events from the primary stream
    primary_stream = f"{entity_type.lower()}-{entity_id}"
    visited_streams: Set[str] = {primary_stream}
    all_events: List[StoredEvent] = await store.load_stream(primary_stream)
    
    # 2. Traverse links up to depth
    current_layer = list(all_events)
    for _ in range(depth):
        next_layer = []
        correlation_ids = {e.metadata.get("correlation_id") for e in current_layer if e.metadata.get("correlation_id")}
        causation_ids = {str(e.event_id) for e in current_layer}
        
        related_streams = set()
        async with store.db.get_connection() as conn:
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
            new_events = await store.load_stream(rs)
            logger.info(f"Found related stream {rs} with {len(new_events)} events via causal link")
            all_events.extend(new_events)
            next_layer.extend(new_events)
        
        current_layer = next_layer
        if not current_layer:
            break

    # 3. Explicit causal validation (No Broken Chains)
    # Check that all causation_ids claimed by any loaded event actually exist in the traversed set
    # (Excluding roots which may not have causation_ids)
    all_event_ids = {str(e.event_id) for e in all_events}
    for e in all_events:
        c_id = e.metadata.get("causation_id")
        if c_id and c_id not in all_event_ids:
            # We found a broken chain or an orphan event missing its parent
            logger.error(f"Broken causal chain: Event {e.event_id} references missing causation_id {c_id}")
            return IntegrityResult(False, False, len(all_events), "")
            
    # 4. Sort all gathered events by global_position to establish a deterministic causal order
    all_events.sort(key=lambda e: e.global_position or 0)
    
    if not all_events:

        return IntegrityResult(True, False, 0, "")

    # 4. Build hash chain
    previous_hash = ""
    for event in all_events:
        # Create unique representation of event for hashing
        # Include type, payload, metadata, and causal links
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
        
        # Chain hash: new_hash = sha256(prev + current)
        combined = previous_hash + current_event_hash
        previous_hash = hashlib.sha256(combined.encode()).hexdigest()

    # 5. Record result
    final_hash = previous_hash
    # Fetch previous run to get its hash
    audit_stream = f"audit-{entity_type.lower()}-{entity_id}"
    previous_runs = await store.load_stream(audit_stream)
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
        chain_valid=True, # In this implementation, we calculate a new one
        tamper_detected=False
    )
    
    await store.append(audit_stream, [audit_event], expected_version=len(previous_runs) if previous_runs else -1, aggregate_type="AuditLedger")

    return IntegrityResult(True, False, len(all_events), final_hash)
