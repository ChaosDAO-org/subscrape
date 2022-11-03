from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.asyncio
async def test_params():

    account_id = "GXPPBuUaZYYYvsEquX55AQ1MRvgZ96kniEKyAVDSdv1SX96"    
    
    config = {
        "kusama":{
            "extrinsics":{
                "_params": {"address": account_id},
                "staking": ["bond"]
            }
        }
    }

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_cache()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB()
    extrinsics_storage = db.storage_manager_for_extrinsics_call("staking", "bond")
    extrinsics = dict(extrinsics_storage.get_iter())

    first_extrinsic = next(iter(extrinsics.values()))

    assert first_extrinsic["account_display"]["address"] == account_id


