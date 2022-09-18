from audioop import add
import logging
import json
import os
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
import subscrape

log_level = logging.INFO


def main():
    """Loads `config/scrape_config.json and iterates over all chains present in the config.
    Will call `scraper_factors()` to retrieve the proper scraper for a chain.
    If `_version` in the config does not match the current version, a warning is logged.    
    """
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

    # load config
    repo_root = Path(__file__).parent.parent.absolute()
    config_path = repo_root / 'config' / 'scrape_config.json'
    if not config_path.exists():
        logging.error("missing scrape config. Exiting")
        exit
    with open(str(config_path), encoding='UTF-8', mode='r') as config_file:
        chains = json.load(config_file)
    subscrape.scrape(chains)


if __name__ == "__main__":
    main()
