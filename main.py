from audioop import add
import logging
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

    ## Load Subscan API Key
    api_key = None
    if os.path.exists("subscan-key"):
        f = open("subscan-key")
        api_key = f.read()

    # context
    subscan = SubscanWrapper(api_key)
    kusama_scraper = scraper_factory(subscan, "kusama")

    # execution
    await kusama_scraper.fetch_extrinsics("system", "remark")
    
    

asyncio.run(main())