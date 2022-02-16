from audioop import add
import logging
import json
import os
import asyncio
from subscrape.subscan_wrapper import SubscanWrapper
from subscrape.scrapers.parachain_scraper import ParachainScraper

log_level = logging.INFO

def scraper_factory(subscan, name):
    db_path = f"data/parachains/{name}_"
    endpoint = f"https://{name}.api.subscan.io"
    scraper = ParachainScraper(db_path, endpoint, subscan)
    return scraper

async def main():
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

    # Load Subscan API Key
    api_key = None
    if os.path.exists("config/subscan-key"):
        f = open("config/subscan-key")
        api_key = f.read()
    subscan = SubscanWrapper(api_key)

    # load config
    config_path = "config/scrape_config.json"
    if not os.path.exists(config_path):
        logging.error("missing scrape config. Exiting")
        exit
    f = open(config_path)
    raw_config = f.read()
    config = json.loads(raw_config)

    for parachain_name in config:
        parachain_scraper = scraper_factory(subscan, parachain_name)
        operations = config[parachain_name]
        for operation in operations:
            payload = operations[operation]
            await parachain_scraper.perform_operation(operation, payload)

    
    

asyncio.run(main())