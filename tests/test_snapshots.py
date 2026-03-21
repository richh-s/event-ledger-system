import pytest
from datetime import datetime

from src.database import Database
from src.event_store import EventStore
from src.aggregates.repository import AggregateRepository
from src.aggregates.document_package import DocumentPackageAggregate
from src.models.events import (
    PackageCreated,
    DocumentAdded,
    DocumentType,
    DocumentFormat
)

@pytest.mark.asyncio
async def test_snapshots_creation_and_reconstruction(db: Database):
    store = EventStore(db)
    # Set threshold low to force a snapshot
    repo = AggregateRepository(store, db, snapshot_threshold=2)
    
    stream_id = "docpkg-snapshot-test"
    agg = DocumentPackageAggregate(stream_id)
    
    agg.create_package(PackageCreated(
        package_id="pkg1", application_id="test", required_documents=[], created_at=datetime.now()
    ))
    agg.add_document(DocumentAdded(
        package_id="pkg1", document_id="doc1", document_type=DocumentType.BANK_STATEMENTS,
        document_format=DocumentFormat.PDF, file_hash="hash", added_at=datetime.now()
    ))
    
    # Save -> Should create snapshot since threshold is 2 and we have 2 events
    await repo.save(agg, "DocumentPackage")
    
    # Check if snapshot is in db
    async with db.get_connection() as conn:
        row = await conn.fetchrow("SELECT * FROM snapshots WHERE stream_id = $1", stream_id)
        assert row is not None
        assert row["version"] == 2
        
    # Reload aggregate entirely from repository
    reloaded_agg = await repo.load(DocumentPackageAggregate, stream_id)
    
    # Should have loaded from snapshot, the state should be exact
    assert reloaded_agg.version == 2
    assert reloaded_agg.is_created == True
    assert "doc1" in reloaded_agg.documents
    
    # Add one more event -> version 3, no snapshot
    reloaded_agg.add_document(DocumentAdded(
        package_id="pkg1", document_id="doc2", document_type=DocumentType.TAX_RETURNS,
        document_format=DocumentFormat.PDF, file_hash="hash2", added_at=datetime.now()
    ))
    await repo.save(reloaded_agg, "DocumentPackage")
    
    # Reload again -> should load snapshot (pos 2) + 1 event
    final_agg = await repo.load(DocumentPackageAggregate, stream_id)
    assert final_agg.version == 3
    assert "doc2" in final_agg.documents
