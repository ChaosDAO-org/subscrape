import csv
import json
from subscrape.db.subscrape_db import SubscrapeDB
from substrateinterface.utils import ss58
import logging
import subscrape

config = {
    "mangatax":{
        "events":{
            "xyk": ["assetsswapped"]
        }
    }
}

db = SubscrapeDB("Kusama")

def fetch_swaps():
    extrinsics_storage = db.storage_manager_for_extrinsics_call("utility", "batch_all")
    extrinsics = extrinsics_storage.get_iter()

    for index, extrinsic in extrinsics:
        params = json.loads(extrinsic["params"])
        params = unwrap_params(params)
        if params["index"] == 2110 and extrinsic["success"] == True:
            #account_id = extrinsic["account_id"]
            value = params["value"]
            row = ["direct", value, ""]
            row.extend(extract_interesting_extrinsic_properties(extrinsic))
            rows.append(row)

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("scraping")
    subscrape.scrape(config)
    logging.info("transforming")
    fetch_swaps()
    file_path = "data/transforms/mangatax_swaps.csv"
    with open(file_path, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(rows)



if __name__ == "__main__":
    main()
