import csv
import json

import pandas as pd
from subscrape.db.subscrape_db import SubscrapeDB
from substrateinterface.utils import ss58
import logging
import subscrape
import datetime

config = {
    "mangatax":{
        "extrinsics":{
            "bootstrap": ["provision"]
        }
    }
}

db = SubscrapeDB("mangatax")

def unwrap_event_params(params):
    result = []
    for param in params:
        name = param["type_name"]
        value = param["value"]
        result.append({name:value})
    return result



def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("scraping")
    subscrape.scrape(config)
    logging.info("transforming")
    extrinsics = db.storage_manager_for_extrinsics_call("bootstrap", "provision").get_iter()

    extrinsics = {index:extrinsic for index, extrinsic in extrinsics}

    for index in extrinsics:
        pass

    print(len(extrinsics))


if __name__ == "__main__":
    main()
