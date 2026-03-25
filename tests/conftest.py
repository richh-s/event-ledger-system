import os
import pytest
import asyncpg
import asyncio
from typing import AsyncGenerator

from src.database import Database

# Use environment variable for CI flexibility, fallback to standard localhost
# We don't try to drop/create db if using a remote Supabase DB, we just truncate and use it
TEST_DB_DSN = os.getenv("DATABASE_URL") or os.getenv("TEST_DB_DSN", "postgres://postgres:postgres@localhost:5434/ledger_test")


@pytest.fixture(scope="session")
def event_loop():
    """Redefine event_loop to session scope for db setup."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def setup_test_db() -> AsyncGenerator[Database, None]:
    """Uses the specified database, initializes the schema, and yields a Database instance."""
    db = Database(TEST_DB_DSN)
    try:
        await db.connect()
    except Exception as e:
        pytest.skip(f"Could not connect to test db at {TEST_DB_DSN}: {e}")
    
    import pathlib
    schema_path = pathlib.Path(__file__).parent.parent / "src" / "schema.sql"
    proj_schema_path = pathlib.Path(__file__).parent.parent / "src" / "projections" / "schema.sql"
    await db.init_schema(str(schema_path))
    await db.init_schema(str(proj_schema_path))
    
    yield db
    
    await db.disconnect()


@pytest.fixture
async def db(setup_test_db: Database) -> Database:
    """Truncates all tables before each test to guarantee isolation."""
    async with setup_test_db.transaction() as conn:
        tables = ["outbox", "events", "snapshots", "event_streams", "projection_checkpoints", "dead_letter_queue", "agent_performance_ledger", "application_summary", "compliance_audit_view", "compliance_snapshots", "agent_decision_trace", "audit_registry_view"]
        await conn.execute(f"TRUNCATE {', '.join(tables)} CASCADE")
        
    import src.database
    src.database._db_instance = setup_test_db
    
    return setup_test_db
    src.database._db_instance = setup_test_db
    
    return setup_test_db
