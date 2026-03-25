import asyncio
import os
import json
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

from src.database import get_db, disconnect_db
from src.event_store import EventStore
from src.queries.historical_reconstruction import HistoricalReconstructor
from src.projections.application_summary import ApplicationSummaryProjection

app = FastAPI(title="Apex Ledger Hub", version="6.0")
store = None
db = None
reconstructor = None

@app.on_event("startup")
async def startup():
    global db, store, reconstructor
    db = get_db()
    await db.connect()
    store = EventStore(db)
    reconstructor = HistoricalReconstructor(db, store)

@app.on_event("shutdown")
async def shutdown():
    await disconnect_db()

@app.get("/api/applications")
async def list_applications():
    """Retrieve all applications by scanning root streams."""
    async with db.get_connection() as conn:
        records = await conn.fetch("""
             SELECT stream_id, metadata->>'correlation_id' as correlation_id, recorded_at 
             FROM events 
             WHERE event_type = 'ApplicationSubmitted'
             ORDER BY recorded_at DESC
        """)
    return [
        {
            "id": r["stream_id"].replace("loan-", ""), 
            "correlation_id": r["correlation_id"],
            "submitted_at": r["recorded_at"]
        } 
        for r in records
    ]

@app.get("/api/applications/{app_id}/history")
async def get_regulatory_package(app_id: str):
    """Deliver Phase 6 Regulatory Extracted Package."""
    try:
        package = await reconstructor.build_regulatory_package(app_id)
        # Exclude massive raw event log to favor crisp UI response
        package.pop("full_event_history", None)
        return package
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class WhatIfRequest(BaseModel):
    alternate_model: str

@app.post("/api/applications/{app_id}/what-if")
async def recompute_credit_decision(app_id: str, request: WhatIfRequest):
    """Phase 6 'what-if' counterfactual API."""
    try:
        res = await reconstructor.what_if_credit_recomputation(app_id, alternate_model=request.alternate_model)
        if "error" in res:
            raise HTTPException(status_code=400, detail=res["error"])
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    with open("static/index.html", "r") as f:
        return f.read()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.web:app", host="0.0.0.0", port=8000, reload=True)
