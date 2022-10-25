from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.asyncio
@pytest.mark.parametrize("api", [(None), ("SubscanV2")])
async def test_events(api):
    
    config = {
        "kusama":{
            "_api": api,
            "events":{
                "council": ["proposed"],
                "_params": {
                    "address": "FcxNWVy5RESDsErjwyZmPCW6Z8Y3fbfLzmou34YZTrbcraL"
                }
            }
        }
    }

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_storage()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB("kusama")
    events_storage = db.storage_manager_for_events_call("council", "proposed")
    events = dict(events_storage.get_iter())

    assert "15038335-25" not in events # created by another address
    proposal_event = events["7608975-2"]
    assert proposal_event["extrinsic_hash"] == '0x2e8d37a0ec4613b445dfd08d927710c6ad4938bc17b7c9ced8467652ed9835ab'
