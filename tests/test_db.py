import subscrape
from subscrape.db.subscrape_db import SubscrapeDB
import pytest

@pytest.mark.asyncio
async def test_db():
    subscrape.wipe_cache()
    file_path = "data/cache/test_db.db"
    db = SubscrapeDB.sqliteInstanceForPath(file_path)
    

