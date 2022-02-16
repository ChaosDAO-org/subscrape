from audioop import add
import logging
import json
import os
import asyncio
from subscrape.scrapers.moonbeam_scraper import MoonbeamScraper
from subscrape.subscan_wrapper import SubscanWrapper
from subscrape.moonscan_wrapper import MoonscanWrapper
from subscrape.scrapers.parachain_scraper import ParachainScraper

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
        db_path = f"data/parachains/{name}_"
        api = subscan_factory(name)
        scraper = ParachainScraper(db_path, api)
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

    for parachain_name in config:
        parachain_scraper = scraper_factory(parachain_name)
        operations = config[parachain_name]
        for operation in operations:
            payload = operations[operation]
            await parachain_scraper.perform_operation(operation, payload)

    
    

asyncio.run(main())