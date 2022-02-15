from audioop import add
import logging
import os
import asyncio
from subscrape.subscan_wrapper import SubscanWrapper
from subscrape.parachains.parachain import Parachain

log_level = logging.INFO


def kusama_factory():
    db_path = "data/parachains/kusama/"
    endpoint = "https://kusama.api.subscan.io"
    parachain = Parachain(db_path, endpoint)
    return parachain

async def main():
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

    ## Load Subscan API Key
    api_key = None
    if os.path.exists("subscan-key"):
        f = open("subscan-key")
        api_key = f.read()

    # context
    kusama = kusama_factory()
    subscan = SubscanWrapper(api_key)

    # execution
    await kusama.fetch_addresses(subscan)
    
    

asyncio.run(main())