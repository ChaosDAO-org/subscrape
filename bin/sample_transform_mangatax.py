import csv
import json
from subscrape.db.subscrape_db import SubscrapeDB
from substrateinterface.utils import ss58
import logging
import subscrape
import datetime

config = {
    "mangatax":{
        "events":{
            "xyk": ["assetsswapped"]
        }
    }
}

db = SubscrapeDB("mangatax")
swaps = [["event_index", "block_timestamp", "datetime", "address", "sold_asset_id", "sold_asset_amount", "bought_asset_id", "bought_asset_amount"]]

def unwrap_event_params(params):
    result = []
    for param in params:
        name = param["type_name"]
        value = param["value"]
        result.append({name:value})
    return result

def fetch_swaps():
    events_storage = db.storage_manager_for_events_call("xyk", "assetsswapped")
    events = events_storage.get_iter()

    for index, event in events:
        params = json.loads(event["params"])
        params = unwrap_event_params(params)

        if int(params[1]["TokenId"]) == 0:
            params[2]["Balance"] = int(params[2]["Balance"]) / 1e18
        else:
            params[2]["Balance"] = int(params[2]["Balance"]) / 1e12

        if int(params[3]["TokenId"]) == 0:
            params[4]["Balance"] = int(params[4]["Balance"]) / 1e18
        else:
            params[4]["Balance"] = int(params[4]["Balance"]) / 1e12

        swaps.append([
            event["event_index"],
            event["block_timestamp"],
            datetime.datetime.fromtimestamp(int(event["block_timestamp"])),
            ss58.ss58_encode(params[0]["AccountId"], ss58_format=42),
            params[1]["TokenId"],
            params[2]["Balance"],
            params[3]["TokenId"],
            params[4]["Balance"]
        ])

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("scraping")
    subscrape.scrape(config)
    logging.info("transforming")
    fetch_swaps()
    file_path = "data/transforms/mangatax_swaps.csv"
    with open(file_path, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(swaps)



if __name__ == "__main__":
    main()
