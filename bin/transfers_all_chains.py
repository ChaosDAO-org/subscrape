import os
import json
import logging
import pandas as pd
from substrateinterface.utils import ss58
import subscrape
from subscrape.db.subscrape_db import SubscrapeDB

def scrape(addresses, chains):
    logging.info("scraping...")
    for chain in chains:
        config = {
            chain: {
                "transfers": addresses
            }
        }

        subscrape.scrape(config)


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # load config
    config_path = "config/transfers_config.json"
    if not os.path.exists(config_path):
        logging.error("missing transfers config. Exiting")
        exit
    f = open(config_path)
    raw_config = f.read()
    config = json.loads(raw_config)
    addresses = config["addresses"]
    chains = config["chains"]

    scrape(addresses, chains)

    logging.info("transforming...")

    chain_dfs = []

    for chain in chains:
        db = SubscrapeDB(chain)
        address_dfs = []
        for address in addresses:
            transfers = db.transfers_iter(address)
            if transfers is None:
                continue
            columns = list(transfers[0].keys())
            rows = []
            for transfer in transfers:
                rows.append(list(transfer.values()))
            address_df = pd.DataFrame(rows, columns=columns)
            address_df.insert(0, "account_name", addresses[address])
            address_dfs.append(address_df)
        if len(address_dfs) > 0:
            chain_df = pd.concat(address_dfs)
            chain_df.insert(0, "chain", chain)
            chain_dfs.append(chain_df)

    df = pd.concat(chain_dfs)

    logging.info("saving...")
    file_path = "data/transforms/transfers.csv"
    df.to_csv(file_path)


if __name__ == "__main__":
    main()
