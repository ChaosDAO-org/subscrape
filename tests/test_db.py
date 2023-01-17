import subscrape
from subscrape.db.subscrape_db import SubscrapeDB, Extrinsic, Event
import pytest
import datetime
import substrateinterface.utils.ss58 as ss58


@pytest.mark.asyncio
async def test_db():
    subscrape.wipe_cache()
    db_connection_string = "sqlite:///data/cache/test_db.db"
    db = SubscrapeDB(db_connection_string)

    extrinsic = Extrinsic(
        chain="chain",
        id="123-1",
        block_number="123",
        block_timestamp=datetime.datetime.now(),
        module="module",
        call="call",
        origin_address="Gch4VxQ79WhjgQqHomvJbqF3Woza5g5cYgM8SVQdDb9szz1",
        origin_public_key=ss58.ss58_decode("Gch4VxQ79WhjgQqHomvJbqF3Woza5g5cYgM8SVQdDb9szz1"),
        nonce=1,
        extrinsic_hash="0x123",
        success=True,
        params={"param2": "value2"},
        fee=1,
        fee_used=1,
        error=None,
        finalized=True,
        tip=1
    )

    event = Event(
        chain="chain",
        id="123-5",
        block_number=123,
        block_timestamp=datetime.datetime.now(),
        extrinsic_id="123-1",
        module="module",
        event="event",
        params={"param1": "value1"},
        finalized=True
    )
    
    db.write_item(extrinsic)
    db.write_item(event)
    db.flush()

    extrinsic = db.query_extrinsic("chain", "123-1")
    assert extrinsic is not None
    assert extrinsic.id == "123-1"
    assert len(extrinsic.events) == 1
    assert extrinsic.events[0].id == "123-5"
    assert extrinsic.events[0].extrinsic.extrinsic_hash == "0x123"

    db.close()
