from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest
from subscrape.db.subscrape_db import Event

@pytest.mark.asyncio
async def test_fetch_events_list():
    
    event_index = "14238250-39"
    
    config = {
        "kusama":{
            "events-list":[
                event_index
            ]
        }
    }

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_cache()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("testing")

    db = SubscrapeDB()
    event = db.read_event(event_index)

    assert event is not None
    assert event.extrinsic_id == '14238250-2'
    assert type(event.params) is list

    db.close()
    

@pytest.mark.asyncio
@pytest.mark.parametrize("auto_hydrate", [True, False])
async def test_fetch_and_hydrate_event(auto_hydrate):
    
    config = {
        "kusama":{
            "_auto_hydrate": auto_hydrate,
            "events": None,
            "_params": {
                "block_num": 700000
            }
        }
    }

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_cache()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("testing")

    db = SubscrapeDB()
    event = db.read_event("700000-0")
    assert event is not None, "The event should exist in the database"
    assert event.extrinsic_id == "700000-0"
    if auto_hydrate:
        assert type(event.params) is list, "Hydrated events should have a list of params"
    else:
        assert event.params is None, "Non-hydrated events should have no params"

    db.close()

@pytest.mark.asyncio
async def test_fetch_events_by_module_event():
    
    config = {
        "kusama":{
            "_auto_hydrate": False,
            "events":{
                "council": ["proposed"]
            }
        }
    }

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_cache()
    logging.info("scraping")
    scrape_result = await subscrape.scrape(config)
    logging.info("testing")

    db = SubscrapeDB()
    events = db.events_query("council", "proposed").all()

    events = [e for e in events if e.id == "52631-4"]
    assert len(events) == 1, "Expected 1 event"
    event:Event = events[0]
    assert event.extrinsic_id == '52631-3'

    db.close()

@pytest.mark.asyncio
async def test_fetch_events_by_module():
    
    config = {
        "kusama":{
            "_auto_hydrate": False,
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

    events = db.events_query("council", "proposed")
    events = [e for e in events if e.id == "14966317-39"]
    assert len(events) == 1, "Expected 1 event"
    event:Event = events[0]
    assert event.extrinsic_id == '14966317-2'

    events = db.events_query("council", "voted")
    events = [e for e in events if e.id == "14938460-47"]
    assert len(events) == 1, "Expected 1 event"
    event = events[0]
    assert event.extrinsic_id == '14938460-4'

    db.close()

@pytest.mark.asyncio
async def test_fetch_events_by_address():
    
    config = {
        "kusama":{
            "_auto_hydrate": False,
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
    assert event.extrinsic_id == "14804812-11"

    db.close()