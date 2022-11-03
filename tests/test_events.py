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

    db = SubscrapeDB.sqliteInstanceForPath("sqlite:///data/cache/test_events.db")
    events_storage = db.storage_manager_for_events_call("council", "proposed")
    events = dict(events_storage.get_iter())
    proposal_event = events["7608975-2"]
    assert proposal_event["extrinsic_hash"] == '0x2e8d37a0ec4613b445dfd08d927710c6ad4938bc17b7c9ced8467652ed9835ab'

@pytest.mark.asyncio
@pytest.mark.parametrize("api", [(None), ("SubscanV2")])
async def test_fetch_all_events_from_module(api):
    
    config = {
        "kusama":{
            "_api": api,
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

    db = SubscrapeDB.sqliteInstanceForPath("sqlite:///data/cache/test_fetch_all_events_from_module.db")

    events_storage = db.storage_manager_for_events_call("council", "proposed")
    events = dict(events_storage.get_iter())
    proposal_event = events["7608975-2"]
    assert proposal_event["extrinsic_hash"] == '0x2e8d37a0ec4613b445dfd08d927710c6ad4938bc17b7c9ced8467652ed9835ab'

    events_storage = db.storage_manager_for_events_call("council", "voted")
    events = dict(events_storage.get_iter())
    proposal_event = events["14938460-47"]
    assert proposal_event["extrinsic_hash"] == '0x339d3522cc716887e83accbc2f7a17173be0871023a2dbe9e4daf25fc1f37852'

@pytest.mark.asyncio
@pytest.mark.parametrize("api", [(None), ("SubscanV2")])
async def test_fetch_all_events_from_module(api):
    
    config = {
        "kusama":{
            "_api": api,
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

    db = SubscrapeDB.sqliteInstanceForPath("sqlite:///data/cache/test_fetch_all_events_from_module.db")

    events_storage = db.storage_manager_for_events_call("society", "defendervote")
    events = dict(events_storage.get_iter())
    proposal_event = events["14804812-56"]
    assert proposal_event["extrinsic_hash"] == '0x6f8f1cb925d533d7754ec81bf744d7b0ed98230bc1c116b1dd9a29035aa41c75'