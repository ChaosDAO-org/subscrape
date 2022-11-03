from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.asyncio
async def test_persistence():
        
    config = {
        "kusama":{
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
    subscrape.wipe_cache()
    logging.info("scraping")
    items_scraped1 = await subscrape.scrape(config)
    items_scraped2 = await subscrape.scrape(config)
    
    assert items_scraped1 != items_scraped2

    db = SubscrapeDB()
    extrinsics_storage = db.storage_manager_for_extrinsics_call("crowdloan", "create")
    extrinsics = extrinsics_storage.get_iter()
    extrinsic_list = []
    for index, extrinsic in extrinsics:
        extrinsic_list.append(index)
    
    subscrape_config = {
        "kusama":{
            "extrinsics-list":extrinsic_list
        }
    }

    logging.info("scraping")
    items_scraped3 = await subscrape.scrape(subscrape_config)
    items_scraped4 = await subscrape.scrape(subscrape_config)

    assert items_scraped3 != items_scraped4
    assert items_scraped4 == 0

