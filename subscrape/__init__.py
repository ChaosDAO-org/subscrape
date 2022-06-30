import logging
import os
from subscrape.scrapers.moonbeam_scraper import MoonbeamScraper
from subscrape.subscan_wrapper import SubscanWrapper
from subscrape.blockscout_wrapper import BlockscoutWrapper
from subscrape.moonscan_wrapper import MoonscanWrapper
from subscrape.scrapers.parachain_scraper import ParachainScraper
from subscrape.db.subscrape_db import SubscrapeDB
from subscrape.scrapers.scrape_config import ScrapeConfig


def moonscan_factory(chain):
    moonscan_key = None
    if os.path.exists("config/moonscan-key"):
        f = open("config/moonscan-key")
        moonscan_key = f.read()

    return MoonscanWrapper(chain, moonscan_key)


def blockscout_factory(chain):
    return BlockscoutWrapper(chain)


def subscan_factory(chain):
    subscan_key = None
    if os.path.exists("config/subscan-key"):
        f = open("config/subscan-key")
        subscan_key = f.read()

    return SubscanWrapper(chain, subscan_key)


def scraper_factory(name):
    if name == "moonriver" or name == "moonbeam":
        db_path = f"data/parachains"
        if not os.path.exists(db_path):
            os.makedirs(db_path)
        db_path += f'/{name}_'
        moonscan_api = moonscan_factory(name)
        blockscout_api = blockscout_factory(name)
        scraper = MoonbeamScraper(db_path, moonscan_api, blockscout_api)
        return scraper
    else:
        db = SubscrapeDB(name)
        subscan_api = subscan_factory(name)
        scraper = ParachainScraper(db, subscan_api)
        return scraper


def scrape(chains):
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
        parachain_scraper.scrape(operations, chain_config)
