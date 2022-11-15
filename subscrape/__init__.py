import logging
import os
from pathlib import Path
from subscrape.scrapers.moonbeam_scraper import MoonbeamScraper
from subscrape.subscan_wrapper import SubscanWrapper
from subscrape.blockscout_wrapper import BlockscoutWrapper
from subscrape.moonscan_wrapper import MoonscanWrapper
from subscrape.scrapers.parachain_scraper import ParachainScraper
from subscrape.db.subscrape_db import SubscrapeDB
from subscrape.scrapers.scrape_config import ScrapeConfig

repo_root = Path(__file__).parent.parent.absolute()


def moonscan_factory(chain):
    """
    Return a configured Moonscan API interface, including API key to speed up transactions

    :param chain: name of the specific EVM chain
    :type chain: str
    """
    moonscan_key = None
    moonscan_key_path = repo_root / 'config' / f'moonscan-{chain}-key'
    if moonscan_key_path.exists():
        with moonscan_key_path.open(encoding="UTF-8", mode='r') as source:
            moonscan_key = source.read()

    return MoonscanWrapper(chain, moonscan_key)


def blockscout_factory(chain):
    """
    Return a configured Blockscout API interface

    :param chain: name of the specific EVM chain
    :type chain: str
    """
    return BlockscoutWrapper(chain)


def subscan_factory(chain):
    """
    Return a configured Subscan API interface, including API key to speed up transactions

    :param chain: name of the specific substrate chain
    :type chain: str
    """
    subscan_key = None
    subscan_key_path = repo_root / 'config' / 'subscan-key'
    if subscan_key_path.exists():
        with subscan_key_path.open(encoding="UTF-8", mode='r') as source:
            subscan_key = source.read()

    return SubscanWrapper(chain, subscan_key)


def scraper_factory(name):
    """
    Configure and return a configured object ready to scrape one or more Dotsama EVM or substrate-based chains

    :param name: name of the specific chain
    :type name: str
    """
    if name == "moonriver" or name == "moonbeam":
        db_path = repo_root / 'data' / 'parachains'
        if not db_path.exists():
            db_path.mkdir()
        db_path = db_path / f'{name}_'
        moonscan_api = moonscan_factory(name)
        blockscout_api = blockscout_factory(name)
        scraper = MoonbeamScraper(db_path, moonscan_api, blockscout_api, name)
        return scraper
    else:
        db = SubscrapeDB(name)
        subscan_api = subscan_factory(name)
        scraper = ParachainScraper(db, subscan_api)
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

            parachain_scraper = scraper_factory(chain)
            items_scraped += parachain_scraper.scrape(operations, chain_config)
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
    Delete everything in the 'data' folder
    """
    data_path = repo_root / 'data'
    if data_path.exists():
        import shutil
        logging.info("wiping data folder")
        shutil.rmtree(data_path)
    else:
        logging.info("data folder does not exist")

