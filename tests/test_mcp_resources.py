import pytest
import asyncio
import json
from src.database import Database
from src.mcp.resources import (
    get_application_summary, 
    get_decision_timeline, 
    get_agent_session_trace,
    get_health_metrics
)

@pytest.fixture
async def db():
    import os
    db = Database(os.getenv("DATABASE_URL"))
    await db.connect()
    yield db
    await db.disconnect()

@pytest.mark.asyncio
async def test_mcp_application_summary_resource(db):
    app_id = "APP-TEST-MCP"
    # Seed summary manually for resource test
    async with db.transaction() as conn:
        await conn.execute(
            """
            INSERT INTO application_summary (application_id, state, last_event_position)
            VALUES ($1, $2, 0)
            ON CONFLICT (application_id) DO UPDATE SET state = $2
            """,
            app_id, "SUBMITTED"
        )
        
    res = await get_application_summary(app_id)
    assert res is not None
    assert res['state'] == "SUBMITTED"

@pytest.mark.asyncio
async def test_mcp_health_resource(db):
    # Ensure a checkpoint exists
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO projection_checkpoints (projection_name, last_position) VALUES ($1, 10) ON CONFLICT DO NOTHING",
            "health_test_proj"
        )
        
    health = await get_health_metrics()
    assert "status" in health
    assert "projections" in health
    assert any(p['name'] == "health_test_proj" for p in health['projections'])
    assert health['dead_letter_count'] >= 0
