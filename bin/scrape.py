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
    config_path = "config/scrape_config.json"
    if not os.path.exists(config_path):
        logging.error("missing scrape config. Exiting")
        exit
    f = open(config_path)
    raw_config = f.read()
    chains = json.loads(raw_config)
    subscrape.scrape(chains)


if __name__ == "__main__":
    main()
