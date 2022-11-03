from audioop import add
from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.asyncio
@pytest.mark.parametrize("api", ["SubscanV2"])
async def test_transfers(api):
    
    address = "DGaHm71gRHJGQVF2ubunkx37qgLpno2gYibq2WHoDJpFMt6"

    config = {
        "kusama":{
            "_api": api,
            "transfers":{
                address : address
            }
        }
    }
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_cache()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB.sqliteInstanceForPath("sqlite:///data/cache/test_transfers.db")
    transfers_storage = db.storage_manager_for_transfers(address)
    transfers = dict(transfers_storage.get_iter())

    first_transfer = transfers["7849231-5"]
    assert first_transfer["hash"] == '0x494dc0b0e077861fd4beb7b45792b043d879ad13f4740740dd69c002b7662b31'


