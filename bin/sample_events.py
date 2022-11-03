from numpy import block
from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pandas as pd


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    config = {
        "mangatax": {
            "events": {
                "system": ["extrinsicsuccess"]
            }
        }
    }

    logging.info("scraping...")
    subscrape.scrape(config)

    logging.info("transforming...")
    db = SubscrapeDB.sqliteInstanceForPath("sqlite:///data/cache/sample_events.db")
    highest_event_index = 0
    highest_event_index_block = None
    highest_tx_index = 0
    highest_tx_index_block = None
    events = db.storage_manager_for_events_call("system", "extrinsicsuccess").get_iter()
    for index, event in events:
        event_idx = event["event_idx"]
        if event_idx > highest_event_index:
            highest_event_index = event_idx
            highest_event_index_block = event["block_num"]
        extrinsic_idx = event["extrinsic_idx"]
        if extrinsic_idx > highest_tx_index:
            highest_tx_index = extrinsic_idx
            highest_tx_index_block = event["block_num"]
        
 
    # print the highest event index and block
    print("highest event index:", highest_event_index)
    print("highest event block:", highest_event_index_block)
    # print the highest extrinsic index and block
    print("highest extrinsic index:", highest_tx_index)
    print("highest extrinsic block:", highest_tx_index_block)


if __name__ == "__main__":
    main()
