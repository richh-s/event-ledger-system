import pytest
import uuid
import json
from datetime import datetime
from src.integrity.audit_chain import AuditChainVerifier
from src.event_store import EventStore
from src.models.events import ApplicationSubmitted, DocumentUploadRequested

@pytest.mark.asyncio
async def test_run_integrity_check(db):
    """
    Test the cryptographic integrity checking mechanism.
    Validates that a constructed stream generates a valid audit chain,
    and directly attacking the DB payload flips tamper_detected=True.
    """
    store = EventStore(db)
    verifier = AuditChainVerifier(db, store)
    
    app_id = f"APP-{uuid.uuid4().hex[:6]}"
    stream_id = f"loan-{app_id}"
    
    # 1. Append valid events
    ev1 = ApplicationSubmitted(
        application_id=app_id, applicant_id="CUST-1", requested_amount_usd=10000.0,
        loan_purpose="expansion", loan_term_months=12, submission_channel="web",
        contact_name="Bob", contact_email="b@b.com", submitted_at=datetime.utcnow(),
        application_reference="REF"
    )
    ev2 = DocumentUploadRequested(
        application_id=app_id, required_document_types=["bank_statements"],
        deadline=datetime.utcnow(), requested_by="system"
    )
    
    await store.append(stream_id, [ev1, ev2], expected_version=-1, aggregate_type="LoanApplication")
    
    # 2. Run check (should be valid)
    result = await verifier.verify_stream_integrity(stream_id)
    assert result["chain_valid"] is True
    assert result["tamper_detected"] is False
    assert result["events_verified"] == 2
    
    # 3. Simulate Database Tampering
    async with db.transaction() as conn:
        rows = await conn.fetch("SELECT global_position, payload FROM events WHERE stream_id = $1 ORDER BY global_position ASC", stream_id)
        
        # Modify the amount in the payload of the first event
        payload_data = dict(rows[0]['payload']) if not isinstance(rows[0]['payload'], str) else json.loads(rows[0]['payload'])
        payload_data['requested_amount_usd'] = 9999999.0
        
        await conn.execute(
            "UPDATE events SET payload = $1::jsonb WHERE global_position = $2",
            json.dumps(payload_data), rows[0]['global_position']
        )
        
    # 4. Re-run check (should fail)
    result2 = await verifier.verify_stream_integrity(stream_id)
    assert result2["chain_valid"] is False
    assert result2["tamper_detected"] is True
