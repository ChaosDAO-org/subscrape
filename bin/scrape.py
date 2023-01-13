import logging
import json
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
import subscrape
import asyncio

log_level = logging.INFO


async def main():
    """Loads `config/scrape_config.json and iterates over all chains present in the config.
    Will call `scraper_factors()` to retrieve the proper scraper for a chain.
    If `_version` in the config does not match the current version, a warning is logged.
    """
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # load config
    repo_root = Path(__file__).parent.parent.absolute()
    config_path = repo_root / 'config' / 'scrape_config.json'
    if not config_path.exists():
        logging.error("missing scrape config. Exiting")
        exit
    f = open(config_path)
    raw_config = f.read()
    chains = json.loads(raw_config)
    await subscrape.scrape(chains)


if __name__ == "__main__":
    asyncio.run(main())
