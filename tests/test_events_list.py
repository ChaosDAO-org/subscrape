from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.asyncio
async def test_events_list():
    
    event_index = "14238250-39"
    
    db_connection_string = f"sqlite:///data/cache/test_events_list.db"

    config = {
        "kusama":{
            "_db_connection_string": db_connection_string,
            "events-list":[
                event_index
            ]
        }
    }

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    subscrape.wipe_cache()
    items = await subscrape.scrape(config)

    assert len(items) == 1, "There should be one item in the list"

    db = SubscrapeDB(db_connection_string)
    event = db.read_event(event_index)

    assert event is not None
    assert event.id == event_index
    assert type(event.params) is list
    
