from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.parametrize("scraper", [None, "SubscanV2"])
def test(scraper):
    
    extrinsic_idx = "14238250-2"
    
    config = {
        "kusama":{
            "_scraper": scraper,
            "extrinsics-list":[
                extrinsic_idx
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
    data = db.read_extrinsic(extrinsic_idx)

    assert data["extrinsic_hash"] == '0x408aacc9a42189836d615944a694f4f7e671a89f1a30bf0977a356cf3f6c301c'

