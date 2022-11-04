from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.asyncio
async def test_events():
    
    config = {
        "kusama":{
            "events":{
                "council": ["proposed"]
            }
        }
    }

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_cache()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB()
    events_query = db.events_query()
    proposal_event = events_query.get("7608975-2")
    assert proposal_event is not None, "The event should exist in the database"
    assert proposal_event.id == "7608975-2"

    db.close()

@pytest.mark.asyncio
async def test_fetch_all_events_from_module():
    
    config = {
        "kusama":{
            "events":{
                "council": None,
            }
        }
    }

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_cache()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB()

    events_storage = db.storage_manager_for_events_call("council", "proposed")
    events = dict(events_storage.get_iter())
    proposal_event = events["7608975-2"]
    assert proposal_event["extrinsic_hash"] == '0x2e8d37a0ec4613b445dfd08d927710c6ad4938bc17b7c9ced8467652ed9835ab'

    events_storage = db.storage_manager_for_events_call("council", "voted")
    events = dict(events_storage.get_iter())
    proposal_event = events["14938460-47"]
    assert proposal_event["extrinsic_hash"] == '0x339d3522cc716887e83accbc2f7a17173be0871023a2dbe9e4daf25fc1f37852'

    db.close()

@pytest.mark.asyncio
async def test_fetch_all_events_from_module():
    
    config = {
        "kusama":{
            "events": None,
            "_params": {
                "address": "FcxNWVy5RESDsErjwyZmPCW6Z8Y3fbfLzmou34YZTrbcraL"
            }
        }
    }

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_cache()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB()
    events_query = db.events_query()
    event = events_query.get("14804812-56")
    assert event is not None, "The event should exist in the database"
    assert event.id == "14804812-56"

    db.close()