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
        "events":{
            "xyk": [
                "assetsswapped", 
            #    "rewardsclaimed", 
            #    "liquidityminted", 
            #    "liquidityactivated", 
            #    "liquiditydeactivated", 
            #    "liquidityburned"
            ],
            #"bootstrap": ["provisioned", "rewardsclaimed"],
            #"xtokens": ["transferred"],
            #"parachain_staking": ["rewarded"]
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

    # create a dataframe from the swaps
    df = pd.DataFrame(swaps, columns=["event_index", "block_timestamp", "datetime", "address", "sold_asset_id", "sold_asset_amount", "bought_asset_id", "bought_asset_amount"])
    # filter df to only include sold_asset_id == 0
    df_sold_mgx = df[df["sold_asset_id"] == 0]
    # add a column `ratio` that is the bought_asset_amount / sold_asset_amount
    df_sold_mgx["ratio"] = df_sold_mgx["bought_asset_amount"] / df_sold_mgx["sold_asset_amount"]
    # filter df to only include bought_asset_id == 0
    df_bought_mgx = df[df["bought_asset_id"] == 0]
    # add a column `ratio` that is the sold_asset_amount / bought_asset_amount
    df_bought_mgx["ratio"] = df_bought_mgx["sold_asset_amount"] / df_bought_mgx["bought_asset_amount"]
    # merge the two dataframes
    df_merged = pd.merge(df_sold_mgx, df_bought_mgx, on="event_index")
    # add a column `date` thate is the date of the event
    df_merged["date"] = df_merged["datetime"].dt.date
    # add a column `hour` that is the hour of the event

    pass

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
