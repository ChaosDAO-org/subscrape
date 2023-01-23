import asyncio
import json
import logging
from pathlib import Path
import platform
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
import subscrape

log_level = logging.INFO


async def main():
    """Loads `config/scrape_config.json and iterates over all chains present in the config.
    Will call `scraper_factory()` to retrieve the proper scraper for a chain.
    If `_version` in the config does not match the current version, a warning is logged.
    """
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # httpx with asyncio can cause an "unclosed transport" error on Windows. A workaround is to set a different loop
    # policy. See https://github.com/encode/httpx/issues/914
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # load config
    repo_root = Path(__file__).parent.parent.absolute()
    config_path = repo_root / 'config' / 'scrape_config.json'
    if not config_path.exists():
        logging.error("missing scrape config. Exiting")
        exit

    with config_path.open('r', encoding="UTF-8") as config_file:
        raw_config = config_file.read()
    chains = json.loads(raw_config)
    await subscrape.scrape(chains)

    # Add a small sleep at the end to avoid Windows issue with asyncio not closing all trans
    # see https://github.com/encode/httpx/issues/914
    # time.sleep(1)
    await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
