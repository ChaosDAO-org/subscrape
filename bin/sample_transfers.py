from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pandas as pd


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    ksm_treasury = "F3opxRbN5ZbjJNU511Kj2TLuzFcDq9BGduA9TgiECafpg29"

    config = {
        "kusama": {
            "transfers": {
                ksm_treasury: "Treasury"
            }
        }
    }

    logging.info("scraping...")
    subscrape.scrape(config)

    logging.info("transforming...")
    db = SubscrapeDB("Kusama")
    transfers = db.transfers_iter(ksm_treasury)
    columns = list(transfers[0].keys())
    rows = []
    for transfer in transfers:
        rows.append(list(transfer.values()))

    logging.info("saving...")
    df = pd.DataFrame(rows, columns=columns)
    file_path = "data/transforms/transfers.csv"
    df.to_csv(file_path)


if __name__ == "__main__":
    main()
