import asyncio
from src.database import get_db
from src.event_store import EventStore
from src.regulatory.package import generate_regulatory_package

async def main():
    db = get_db()
    await db.connect()
    try:
        store = EventStore(db)
        await generate_regulatory_package("APP-MCP-8E5C19", db, store)
    except Exception as e:
        print("Failure:", e)

if __name__ == "__main__":
    asyncio.run(main())
