import asyncpg
from contextlib import asynccontextmanager


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
            import json
            async def init(con):
                await con.set_type_codec(
                    'jsonb',
                    encoder=json.dumps,
                    decoder=json.loads,
                    schema='pg_catalog',
                )
            # We enforce standard settings here. The caller should pass
            # a proper DSN, e.g., 'postgres://user:pass@host/db'
            self._pool = await asyncpg.create_pool(
                self.dsn,
                min_size=1,
                max_size=10,
                init=init,
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
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        async with self.get_connection() as conn:
            await conn.execute(schema_sql)
