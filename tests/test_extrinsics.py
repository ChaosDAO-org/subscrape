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
                "crowdloan": ["create"]
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
    extrinsics_storage = db.storage_manager_for_extrinsics_call("crowdloan", "create")
    extrinsics = dict(extrinsics_storage.get_iter())

    first_crowdloan = extrinsics["8974101-2"]
    assert first_crowdloan["extrinsic_hash"] == '0xee88a0694a88435c11b5f5d2ac971ab026180467ab215887b6608731c4679051'

