from audioop import add
import logging
import json
import os
import asyncio
from subscrape.scrapers.moonbeam_scraper import MoonbeamScraper
from subscrape.subscan_wrapper import SubscanWrapper
from subscrape.moonscan_wrapper import MoonscanWrapper
from subscrape.scrapers.parachain_scraper import ParachainScraper
from subscrape.db.subscrape_db import SubscrapeDB

log_level = logging.INFO

def moonscan_factory(chain):
    endpoint = f"https://api-{chain}.moonscan.io/api"
    return MoonscanWrapper(endpoint)

def subscan_factory(chain):
    subscan_key = None
    if os.path.exists("config/subscan-key"):
        f = open("config/subscan-key")
        subscan_key = f.read()

    endpoint = f"https://{chain}.api.subscan.io"
    return SubscanWrapper(subscan_key, endpoint)


def scraper_factory(name):
    if name == "moonriver" or name == "moonbeam":
        db_path = f"data/parachains/{name}_"
        api = moonscan_factory(name)
        scraper = MoonbeamScraper(db_path, api)
        return scraper
    else:
        db = SubscrapeDB(name)
        api = subscan_factory(name)
        scraper = ParachainScraper(db, api)
        return scraper

async def main():
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

    # load config
    config_path = "config/scrape_config.json"
    if not os.path.exists(config_path):
        logging.error("missing scrape config. Exiting")
        exit
    f = open(config_path)
    raw_config = f.read()
    config = json.loads(raw_config)

    for key in config:
        if key.startswith("_"):
            if key == "_version" and config[key] != 1:
                logging.warn("config version != 1. It could contain runtime breaking contents")
            continue
        parachain_scraper = scraper_factory(key)
        await parachain_scraper.scrape(config[key])
    
    

asyncio.run(main())