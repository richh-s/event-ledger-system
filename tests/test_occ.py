import asyncio
import os
import uuid
import pytest
from src.database import Database
from src.event_store import EventStore
from src.models import ApplicationSubmitted, OptimisticConcurrencyError, LoanPurpose
from datetime import datetime

async def test_occ_concurrency():
    db = Database(os.getenv("DATABASE_URL"))
    await db.connect()
    store = EventStore(db)
    
    stream_id = f"loan-TEST-OCC-{uuid.uuid4().hex[:8]}"
    
    # 1. Base event
    evt1 = ApplicationSubmitted(
        application_id=stream_id, applicant_id="A1", requested_amount_usd=1000,
        loan_purpose=LoanPurpose.WORKING_CAPITAL, loan_term_months=12,
        submission_channel="web", contact_email="a@b.com", contact_name="A B",
        submitted_at=datetime.now(), application_reference="REF1"
    )
    
    # Create stream
    await store.append(stream_id, [evt1], expected_version=-1, aggregate_type="LoanApplication")
    
    # 2. Two tasks try to append to the same version (0)
    current_v = await store.stream_version(stream_id)
    assert current_v == 1 # stream_position starts at 1
    
    evt2a = ApplicationSubmitted(
        application_id=stream_id, applicant_id="A1", requested_amount_usd=2000,
        loan_purpose=LoanPurpose.WORKING_CAPITAL, loan_term_months=12,
        submission_channel="web", contact_email="a@b.com", contact_name="A B",
        submitted_at=datetime.now(), application_reference="REF2a"
    )
    evt2b = ApplicationSubmitted(
        application_id=stream_id, applicant_id="A1", requested_amount_usd=3000,
        loan_purpose=LoanPurpose.WORKING_CAPITAL, loan_term_months=12,
        submission_channel="web", contact_email="a@b.com", contact_name="A B",
        submitted_at=datetime.now(), application_reference="REF2b"
    )

    async def do_append(evt):
        try:
            await store.append(stream_id, [evt], expected_version=1)
            return "SUCCESS"
        except OptimisticConcurrencyError:
            return "FAILURE"
        except Exception as e:
            return f"ERROR: {type(e).__name__}: {e}"

    results = await asyncio.gather(do_append(evt2a), do_append(evt2b))
    
    print(f"Results: {results}")
    
    # Exactly one should succeed, one should fail
    assert results.count("SUCCESS") == 1
    assert results.count("FAILURE") == 1
    
    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(test_occ_concurrency())
