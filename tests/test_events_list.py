from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.parametrize("api", [None, "SubscanV2"])
def test(api):
    
    event_index = "14238250-39"
    
    config = {
        "kusama":{
            "_api": api,
            "events-list":[
                event_index
            ]
        }
    }

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_storage()
    logging.info("scraping")
    subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB("Kusama")
    data = db.read_event(event_index)

    assert data["extrinsic_hash"] == '0x408aacc9a42189836d615944a694f4f7e671a89f1a30bf0977a356cf3f6c301c'
    assert type(data["params"]) is list
    
