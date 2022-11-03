from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.asyncio
@pytest.mark.parametrize("api", [None, "SubscanV2"])
async def test_hydration(api):
        
    chain = "mangatax"
    module = "bootstrap"
    call = "provision_vested"

    config = {
        chain:{
            "_api": api,
            "extrinsics":{
                module: [call]
            }
        }
    }
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_cache()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB.sqliteInstanceForPath("sqlite:///data/cache/test_hydration.db")
    extrinsics_storage = db.storage_manager_for_extrinsics_call(module, call)
    extrinsics = extrinsics_storage.get_iter()
    extrinsic_list = []
    for index, extrinsic in extrinsics:
        extrinsic_list.append(index)

    subscrape_config = {chain:{"_api": api, "extrinsics-list":extrinsic_list}}
    await subscrape.scrape(subscrape_config)

    index = extrinsic_list[-1]
    data = db.read_extrinsic(index)
    assert type(data["params"]) is list


