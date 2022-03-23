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
from subscrape.scrapers.scrape_config import ScrapeConfig


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
        db_path = f"data/parachains"
        if not os.path.exists(db_path):
            os.makedirs(db_path)
        db_path += f'/{name}_'
        api = moonscan_factory(name)
        scraper = MoonbeamScraper(db_path, api)
        return scraper
    else:
        db = SubscrapeDB(name)
        api = subscan_factory(name)
        scraper = ParachainScraper(db, api)
        return scraper


async def main():
    """Loads `config/scrape_config.json and iterates over all chains present in the config.
    Will call `scraper_factors()` to retrieve the proper scraper for a chain.
    If `_version` in the config does not match the current version, a warning is logged.    
    """
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

    # load config
    config_path = "config/scrape_config.json"
    if not os.path.exists(config_path):
        logging.error("missing scrape config. Exiting")
        exit
    f = open(config_path)
    raw_config = f.read()
    chains = json.loads(raw_config)
    scrape_config = ScrapeConfig(chains)

    for chain in chains:
        if chain.startswith("_"):
            if chain == "_version" and chains[chain] != 1:
                logging.warning("config version != 1. It could contain runtime breaking contents")
            continue
        operations = chains[chain]
        chain_config = scrape_config.create_inner_config(operations)

        # check if we should skip this chain
        if chain_config.skip:
            logging.info(f"Config asks to skip chain {chain}")
            continue


        parachain_scraper = scraper_factory(chain)
        await parachain_scraper.scrape(operations, chain_config)


asyncio.run(main())
