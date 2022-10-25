from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.asyncio
@pytest.mark.parametrize("api", [None, "SubscanV2"])
async def test_extrinsics(api):
        
    config = {
        "kusama":{
            "_api": api,
            "extrinsics":{
                "bounties": None
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
    extrinsics_storage = db.storage_manager_for_extrinsics_call("bounties", "propose_bounty")
    extrinsics = dict(extrinsics_storage.get_iter())

    first_crowdloan = extrinsics["12935940-3"]
    assert first_crowdloan["extrinsic_hash"] == '0x28b3e9dc097036a98b43b9792745be89d3fecbbca71200b45a2aba901c7cc5af'

