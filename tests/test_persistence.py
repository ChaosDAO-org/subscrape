from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.parametrize("scraper", [None, "SubscanV2"])
def test(scraper):
        
    config = {
        "kusama":{
            "_scraper": scraper,
            "extrinsics":{
                "crowdloan": ["create"]
            },
            "events":{
                "crowdloan": ["created"]
            }
        },
    }
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_storage()
    logging.info("scraping")
    items_scraped1 = subscrape.scrape(config)
    items_scraped2 = subscrape.scrape(config)
    
    assert items_scraped1 != items_scraped2
