import logging
import os
from subscrape.scrapers.moonbeam_scraper import MoonbeamScraper
from subscrape.apis.subscan_v1 import SubscanV1
from subscrape.apis.subscan_v2 import SubscanV2
from subscrape.apis.blockscout_wrapper import BlockscoutWrapper
from subscrape.apis.moonscan_wrapper import MoonscanWrapper
from subscrape.scrapers.parachain_scraper import ParachainScraper
from subscrape.db.subscrape_db import SubscrapeDB
from subscrape.scrapers.scrape_config import ScrapeConfig


def moonscan_factory(chain):
    """
    Return a configured Moonscan API interface, including API key to speed up transactions

    :param chain: name of the specific EVM chain
    :type chain: str
    """
    moonscan_key = None
    if os.path.exists("config/moonscan-key"):
        f = open("config/moonscan-key")
        moonscan_key = f.read()

    return MoonscanWrapper(chain, moonscan_key)


def blockscout_factory(chain):
    """
    Return a configured Blockscout API interface

    :param chain: name of the specific EVM chain
    :type chain: str
    """
    return BlockscoutWrapper(chain)


def subscan_factory(chain, db: SubscrapeDB, chain_config: ScrapeConfig):
    """
    Return a configured Subscan API interface, including API key to speed up transactions

    :param chain: name of the specific substrate chain
    :type chain: str
    :param db: database to use for storing the scraped data
    :type db: SubscrapeDB
    :param chain_config: configuration for the specific chain
    :type chain_config: ScrapeConfig
    """
    subscan_key = None
    if os.path.exists("config/subscan-key"):
        f = open("config/subscan-key")
        subscan_key = f.read()

    selected_api = chain_config.api
    if selected_api is None:
        selected_api = "SubscanV1"
        logging.info("No scraper specified in the chains `_api` param. Assuming SubscanV1. This will change in the future. Please specify a scraper.")

    if selected_api == "SubscanV1":
        scraper = SubscanV1(chain, db, subscan_key)
    elif selected_api == "SubscanV2":
        scraper = SubscanV2(chain, db, subscan_key)
    else:
        raise Exception(f"Unknown scraper {selected_api}")

    return scraper

    return scraper


def scraper_factory(name, chain_config: ScrapeConfig):
    """
    Configure and return a configured object ready to scrape one or more Dotsama EVM or substrate-based chains

    :param name: name of the specific chain
    :type name: str
    """
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
        subscan_api = subscan_factory(name, db, chain_config)
        scraper = ParachainScraper(subscan_api)
        return scraper


def scrape(chains) -> int:
    """
    For each specified chain, get an appropriate scraper and then scrape the chain for transactions of interest based
    on the config file.

    :param chains: list of chains to scrape
    :type chains: list
    :return: number of items scraped
    """
    items_scraped = 0

    try:

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

            scraper = scraper_factory(chain, chain_config)
            items_scraped += scraper.scrape(operations, chain_config)
    except Exception as e:
        logging.error(f"Uncaught error during scraping: {e}")
        import traceback
        # log traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        raise e

    logging.info(f"Scraped {items_scraped} items")
    return items_scraped

def wipe_storage():
    """
    Wipe the complete storage the data folder
    """
    if os.path.exists("data"):
        import shutil
        logging.info("wiping data folder")
        shutil.rmtree("data/")
    else:
        logging.info("data folder does not exist")