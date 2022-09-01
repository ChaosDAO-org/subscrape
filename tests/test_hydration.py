from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.parametrize("api", [None, "SubscanV2"])
def test(api):
        
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
    subscrape.wipe_storage()
    logging.info("scraping")
    subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB("mangatax")
    extrinsics_storage = db.storage_manager_for_extrinsics_call(module, call)
    extrinsics = extrinsics_storage.get_iter()
    extrinsic_list = []
    for index, extrinsic in extrinsics:
        extrinsic_list.append(index)

    subscrape_config = {chain:{"_api": api, "extrinsics-list":extrinsic_list}}
    subscrape.scrape(subscrape_config)

    extrinsics = extrinsics_storage.get_iter()
    for index, extrinsic in extrinsics:
        pass



