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

    logging.info("wiping storage")
    subscrape.wipe_cache()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB(db_connection_string)
    data = db.read_event(event_index)

    assert data.id == event_index
    assert type(data.params) is list
    
