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
                "bounties": ["propose_bounty"]
            }
        }
    }
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_cache()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB.sqliteInstanceForPath("sqlite:///data/cache/test_extrinsics.db")
    extrinsics_storage = db.storage_manager_for_extrinsics_call("bounties", "propose_bounty")
    extrinsics = dict(extrinsics_storage.get_iter())

    extrinsic = extrinsics["12935940-3"]
    assert extrinsic["extrinsic_hash"] == '0x28b3e9dc097036a98b43b9792745be89d3fecbbca71200b45a2aba901c7cc5af'

@pytest.mark.asyncio
@pytest.mark.parametrize("api", [None, "SubscanV2"])
async def test_fetch_all_extrinsics_from_module(api):
    
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
    subscrape.wipe_cache()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB.sqliteInstanceForPath("sqlite:///data/cache/test_fetch_all_extrinsics_from_module.db")

    extrinsics_storage = db.storage_manager_for_extrinsics_call("bounties", "propose_bounty")
    extrinsics = dict(extrinsics_storage.get_iter())
    extrinsic = extrinsics["12935940-3"]
    assert extrinsic["extrinsic_hash"] == '0x28b3e9dc097036a98b43b9792745be89d3fecbbca71200b45a2aba901c7cc5af'

    extrinsics_storage = db.storage_manager_for_extrinsics_call("bounties", "extend_bounty_expiry")
    extrinsics = dict(extrinsics_storage.get_iter())
    extrinsic = extrinsics["14534356-3"]
    assert extrinsic["extrinsic_hash"] == '0xf02b930789a35b4b942006c60ae6c83daee4d87237e213bab4ce0e7d93cfb0f4'

@pytest.mark.asyncio
@pytest.mark.parametrize("api", [None, "SubscanV2"])
async def test_fetch_all_extrinsics_from_address(api):
    
    config = {
        "kusama":{
            "_api": api,
            "extrinsics": None,
            "_params": {
                "address": "EGP7XztdTosm1EmaATZVMjSWujGEj9nNidhjqA2zZtttkFg"
            }
        }
    }
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("wiping storage")
    subscrape.wipe_cache()
    logging.info("scraping")
    await subscrape.scrape(config)
    logging.info("transforming")

    db = SubscrapeDB.sqliteInstanceForPath("sqlite:///data/cache/test_fetch_all_extrinsics_from_address.db")
    extrinsics_storage = db.storage_manager_for_extrinsics_call("balances", "transfer_keep_alive")
    extrinsics = dict(extrinsics_storage.get_iter())

    assert "15067802-4" not in extrinsics
    extrinsic = extrinsics["14815834-2"]
    assert extrinsic["extrinsic_hash"] == '0xc015e661ce5a763d2377d5216037677f5e16fe1a5ec4471de3acbd6be683461b'
