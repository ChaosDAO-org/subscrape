from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape


def test():

    config = {
        "kusama": {
            "events": {
                "crowdloan": ["created"]
            }
        }
    }

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_storage()
    logging.info("scraping")
    subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB("kusama")
    events_storage = db.storage_manager_for_events_call("crowdloan", "created")
    events = dict(events_storage.get_iter())

    first_crowdloan = events["14215808-27"]
    assert first_crowdloan["extrinsic_hash"] == '0x9d3430cd00bff235d4cdd595513375e6ceacb5228590a8629a865922d67f056f'


test()
