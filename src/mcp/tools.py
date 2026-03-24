from __future__ import annotations
from datetime import datetime
from uuid import uuid4
from mcp.server.fastmcp import FastMCP
from src.database import Database
from src.event_store import EventStore
from src.aggregates.repository import AggregateRepository
from src.aggregates.loan_application import LoanApplicationAggregate
from src.schema.events import (
    ApplicationSubmitted, 
    DocumentUploaded, 
    LoanPurpose, 
    DocumentType, 
    DocumentFormat
)

# Initialize FastMCP
mcp = FastMCP("EventLedger")

# Helper to provide database and store
# In a real app, these would be managed by a context manager or dependency injection
async def get_ops():
    import os
    db = Database(os.getenv("DATABASE_URL"))
    await db.connect()
    store = EventStore(db)
    repo = AggregateRepository(store)
    return db, store, repo

@mcp.tool()
async def submit_application(
    applicant_id: str,
    amount: float,
    purpose: str,
    contact_name: str,
    contact_email: str
) -> str:
    """Submit a new loan application."""
    db, store, repo = await get_ops()
    app_id = f"APP-{uuid4().hex[:6].upper()}"
    
    event = ApplicationSubmitted(
        application_id=app_id,
        applicant_id=applicant_id,
        requested_amount_usd=amount,
        loan_purpose=LoanPurpose(purpose),
        loan_term_months=12,
        submission_channel="MCP",
        contact_name=contact_name,
        contact_email=contact_email,
        submitted_at=datetime.utcnow(),
        application_reference=f"REF-{app_id}"
    )
    
    # Save via repository (which handles aggregate logic and persistence)
    agg = await repo.load(LoanApplicationAggregate, f"loan-{app_id}")
    agg.submit_application(event)
    await repo.save(agg, "LoanApplication")
    
    await db.disconnect()
    return f"Application {app_id} submitted successfully."

@mcp.tool()
async def upload_document(
    application_id: str,
    document_type: str,
    filename: str,
    file_path: str
) -> str:
    """Record a document upload for an application."""
    db, store, repo = await get_ops()
    doc_id = f"DOC-{uuid4().hex[:4].upper()}"
    
    event = DocumentUploaded(
        application_id=application_id,
        document_id=doc_id,
        document_type=DocumentType(document_type),
        document_format=DocumentFormat.PDF,
        filename=filename,
        file_path=file_path,
        file_size_bytes=1024, # Dummy
        file_hash="sha256:dummy",
        uploaded_at=datetime.utcnow(),
        uploaded_by="MCP_USER"
    )
    
    # Normally this would hit a DocumentPackage aggregate
    # For now we'll just record it in the ledger via direct append if needed, 
    # but the plan says "invokes aggregate command".
    from src.aggregates.document_package import DocumentPackageAggregate
    agg = await repo.load(DocumentPackageAggregate, f"docpkg-{application_id}")
    # Note: Aggregate might need a method to accept this event
    # If not present, we'll append to stream directly.
    await store.append(f"docpkg-{application_id}", [event], expected_version=agg.version, aggregate_type="DocumentPackage")
    
    await db.disconnect()
    return f"Document {doc_id} uploaded for {application_id}."

@mcp.tool()
async def run_document_processing(application_id: str) -> str:
    """Invoke document processing agent (Stub for Phase 3)."""
    return f"Document processing initiated for {application_id}."

@mcp.tool()
async def run_credit_analysis(application_id: str) -> str:
    """Invoke the credit analysis agent (Stub for Phase 3)."""
    return f"Credit analysis initiated for {application_id}."

@mcp.tool()
async def run_fraud_check(application_id: str) -> str:
    """Invoke the fraud detection agent (Stub for Phase 3)."""
    return f"Fraud screening initiated for {application_id}."

@mcp.tool()
async def run_compliance_check(application_id: str) -> str:
    """Invoke the compliance agent (Stub for Phase 3)."""
    return f"Compliance check initiated for {application_id}."

@mcp.tool()
async def generate_decision(application_id: str) -> str:
    """Generate a loan decision (Stub for Phase 3)."""
    return f"Decision generation initiated for {application_id}."

@mcp.tool()
async def complete_human_review(application_id: str, verdict: str, reason: str) -> str:
    """Record a human reviewer’s override/decision."""
    db, store, repo = await get_ops()
    
    from src.schema.events import HumanReviewCompleted
    event = HumanReviewCompleted(
        application_id=application_id,
        reviewer_id="HUMAN-01",
        override=(verdict == "APPROVE"),
        original_recommendation="REFER",
        final_decision=verdict,
        override_reason=reason,
        reviewed_at=datetime.utcnow()
    )
    
    agg = await repo.load(LoanApplicationAggregate, f"loan-{application_id}")
    agg.complete_human_review(event)
    await repo.save(agg, "LoanApplication")
    
    await db.disconnect()
    return f"Human review completed for {application_id} with verdict: {verdict}."
