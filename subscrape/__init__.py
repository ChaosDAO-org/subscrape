import logging
import os
from pathlib import Path
from subscrape.scrapers.moonbeam_scraper import MoonbeamScraper
from subscrape.apis.blockscout_wrapper import BlockscoutWrapper
from subscrape.apis.moonscan_wrapper import MoonscanWrapper
from subscrape.scrapers.parachain_scraper import ParachainScraper
from subscrape.db.subscrape_db import SubscrapeDB
from subscrape.scrapers.scrape_config import ScrapeConfig
from subscrape.apis.subscan_wrapper import SubscanWrapper

repo_root = Path(__file__).parent.parent.absolute()
logger = logging.getLogger(__name__)


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
    subscan_key_path = repo_root / 'config' / 'subscan-key'
    if subscan_key_path.exists():
        with subscan_key_path.open(encoding="UTF-8", mode='r') as source:
            subscan_key = source.read()

    scraper = SubscanWrapper(chain, db, subscan_key)
    return scraper


def scraper_factory(chain_name, chain_config: ScrapeConfig, db_factory: callable = None):
    """
    Configure and return a configured object ready to scrape one or more Dotsama EVM or substrate-based chains

    :param chain_name: name of the specific chain
    :type chain_name: str
    :param chain_config: configuration for the specific chain
    :type chain_config: ScrapeConfig
    :param db_factory: optional function to use to create a database connection. takes the chain config as parameter
    :type db_factory: callable
    """
    if chain_name == "moonriver" or chain_name == "moonbeam":
        db_connection_string = f"data/parachains"
        if not os.path.exists(db_connection_string):
            os.makedirs(db_connection_string)
        db_connection_string += f'/{chain_name}_'
        moonscan_api = moonscan_factory(chain_name)
        blockscout_api = blockscout_factory(chain_name)
        scraper = MoonbeamScraper(db_connection_string, moonscan_api, blockscout_api)
        return scraper
    else:
        # determine the database connection string
        if chain_config.db_connection_string is None:
            db_connection_string = "sqlite:///data/cache/default.db"
        else:
            db_connection_string = chain_config.db_connection_string
        
        # create the database object
        if db_factory is None:
            db = SubscrapeDB(db_connection_string)
        else:
            db = db_factory(chain_config)

        subscan_api = subscan_factory(chain_name, db, chain_config)
        scraper = ParachainScraper(subscan_api)
        return scraper


async def scrape(chains_config, db_factory=None) -> list:
    """
    For each specified chain, get an appropriate scraper and then scrape the chain for transactions of interest based
    on the config file.

    :param chains_config: list of chains to scrape
    :type chains_config: list
    :param db_factory: optional function to use to create a database connection. takes the chain config as parameter
    :type db_factory: function
    :return: the list of scraped items
    """
    items = []

    try:
        scrape_config = ScrapeConfig(chains_config)

        for chain_name in chains_config:
            if chain_name.startswith("_"):
                if chain_name == "_version" and chains_config[chain_name] != 1:
                    logger.warning("config version != 1. It could contain runtime breaking contents")
                continue
            operations = chains_config[chain_name]
            chain_config = scrape_config.create_inner_config(operations)

            # check if we should skip this chain
            if chain_config.skip:
                logger.info(f"Config asks to skip chain {chain_name}")
                continue

            scraper = scraper_factory(chain_name, chain_config, db_factory)
            new_items = await scraper.scrape(operations, chain_config)
            items.extend(new_items)
    except Exception as e:
        logger.error(f"Uncaught error during scraping: {e}")
        import traceback
        # log traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise e

    logger.info(f"Scraped {len(items)} items")
    return items


def wipe_cache():
    """
    Wipe the cache folder
    """
    if os.path.exists("data/cache"):
        import shutil
        logger.info("wiping cache folder")
        shutil.rmtree("data/cache/")
    else:
        logger.info("cache folder does not exist")
