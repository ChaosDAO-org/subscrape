from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.asyncio
@pytest.mark.parametrize("api", [None, "SubscanV2"])
async def test(api):

    account_id = "GXPPBuUaZYYYvsEquX55AQ1MRvgZ96kniEKyAVDSdv1SX96"    
    
    config = {
        "kusama":{
            "_api": api,
            "extrinsics":{
                "_params": {"address": account_id},
                "staking": ["bond"]
            }
        }
    }

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_storage()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB("kusama")
    extrinsics_storage = db.storage_manager_for_extrinsics_call("staking", "bond")
    extrinsics = dict(extrinsics_storage.get_iter())

    first_extrinsic = next(iter(extrinsics.values()))

    if api != "SubscanV2":
        assert first_extrinsic["account_id"] == account_id
    else:
        assert first_extrinsic["account_display"]["address"] == account_id


