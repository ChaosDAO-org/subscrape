
from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape

config = {
    "kusama": {
        "extrinsics-list": [
            "14238250-2"
        ]
    }
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("scraping")
subscrape.scrape(config)
logging.info("transforming")

db = SubscrapeDB("Kusama")
