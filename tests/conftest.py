import os
import pytest
import asyncpg
import asyncio
from typing import AsyncGenerator

from ledger.database import Database

# Use environment variable for CI flexibility, fallback to standard localhost
TEST_DB_DSN = os.getenv("TEST_DB_DSN", "postgres://postgres:postgres@localhost:5432/ledger_test")
DEFAULT_DB_DSN = os.getenv("DEFAULT_DB_DSN", "postgres://postgres:postgres@localhost:5432/postgres")


@pytest.fixture(scope="session")
def event_loop():
    """Redefine event_loop to session scope for db setup."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def setup_test_db() -> AsyncGenerator[Database, None]:
    """Creates the ledger_test database, initializes the schema, and yields a Database instance."""
    # Attempt to create test database
    try:
        conn = await asyncpg.connect(DEFAULT_DB_DSN)
        try:
            await conn.execute("CREATE DATABASE ledger_test")
        except asyncpg.exceptions.DuplicateDatabaseError:
            pass
        finally:
            await conn.close()
    except Exception as e:
        pytest.skip(f"Could not connect to local PostgreSQL to create test db: {e}")

    db = Database(TEST_DB_DSN)
    await db.connect()
    
    import pathlib
    schema_path = pathlib.Path(__file__).parent.parent / "ledger" / "schema.sql"
    await db.init_schema(str(schema_path))
    
    yield db
    
    await db.disconnect()
    
    # Teardown database
    try:
        conn = await asyncpg.connect(DEFAULT_DB_DSN)
        await conn.execute("DROP DATABASE ledger_test WITH (FORCE)")
        await conn.close()
    except Exception:
        pass


@pytest.fixture
async def db(setup_test_db: Database) -> Database:
    """Truncates all tables before each test to guarantee isolation."""
    async with setup_test_db.get_connection() as conn:
        await conn.execute("TRUNCATE events, event_streams, outbox, projection_checkpoints RESTART IDENTITY CASCADE")
    return setup_test_db
