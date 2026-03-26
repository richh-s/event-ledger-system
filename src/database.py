import asyncpg
import json
import ssl
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, Any, List, Optional

class Database:
    """Async database connection pool manager for the Ledger."""
    def __init__(self, dsn: str):
        # Enforce SSL for Supabase / external managed DBs if not targeting localhost natively
        if dsn and "localhost" not in dsn and "sslmode=" not in dsn:
            dsn += "&sslmode=require" if "?" in dsn else "?sslmode=require"
            
        self.dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self):
        """Initializes the asyncpg connection pool."""
        if not self._pool:
            ssl_ctx = None
            if "sslmode=require" in self.dsn:
                # asyncpg requires an SSLContext when using SSL
                ssl_ctx = ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE
            
            async def init(con):
                # We can't easily register codecs for jsonb AND use pgbouncer in statement mode if 
                # we rely on it, but Supabase pooling (6543) is fine with simple json encoding if handled manually
                # or if we use the default.
                pass

            self._pool = await asyncpg.create_pool(
                self.dsn,
                min_size=2,
                max_size=50,
                ssl=ssl_ctx,
                command_timeout=60,
                statement_cache_size=0 # DISABLE statement cache for PgBouncer/Supabase
            )

    async def disconnect(self):
        """Closes the asyncpg connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def transaction(self):
        """Yields an explicit connection within a READ COMMITTED transaction block."""
        if not self._pool:
            raise RuntimeError("Database not connected. Call connect() first.")
        
        async with self._pool.acquire() as conn:
            # Read Committed is the Postgres default, but we declare it explicitly per plan.
            async with conn.transaction(isolation='read_committed'):
                yield conn

    @asynccontextmanager
    async def get_connection(self):
        """Yields a raw connection from the pool without an explicit transaction wrapper."""
        if not self._pool:
            raise RuntimeError("Database not connected. Call connect() first.")
        
        async with self._pool.acquire() as conn:
            yield conn
            
    async def init_schema(self, schema_path: str):
        """Executes the DDL script to initialize the tables."""
        if not os.path.exists(schema_path):
            return
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        async with self.get_connection() as conn:
            await conn.execute(schema_sql)

# Global instance for singleton-like access
_db_instance: Database | None = None

def get_db() -> Database:
    """Returns a singleton Database instance initialized from DATABASE_URL."""
    global _db_instance
    if _db_instance is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL environment variable is not set.")
        _db_instance = Database(dsn)
    return _db_instance

async def disconnect_db():
    """Closes the global database pool."""
    global _db_instance
    if _db_instance:
        await _db_instance.disconnect()
        _db_instance = None
