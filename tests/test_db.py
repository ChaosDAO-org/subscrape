import subscrape
from subscrape.db.subscrape_db import SubscrapeDB, ExtrinsicMetadata, Extrinsic, EventMetadata, Event
import pytest

@pytest.mark.asyncio
async def test_db():
    subscrape.wipe_cache()
    db_connection_string = "sqlite:///data/cache/test_db.db"
    db = SubscrapeDB(db_connection_string)
    
    db.close()
