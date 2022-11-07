from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import pytest

@pytest.mark.asyncio
async def test_extrinsics():
        
    config = {
        "kusama":{
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

    db = SubscrapeDB()
    extrinsic = db.read_extrinsic_metadata("12935940-3")
    assert extrinsic is not None
    assert extrinsic.extrinsic_hash == '0x28b3e9dc097036a98b43b9792745be89d3fecbbca71200b45a2aba901c7cc5af'
    
    db.close()

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

    db = SubscrapeDB()
    extrinsic = db.read_extrinsic_metadata("12935940-3")
    assert extrinsic.extrinsic_hash == '0x28b3e9dc097036a98b43b9792745be89d3fecbbca71200b45a2aba901c7cc5af'

    db.close()

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

    db = SubscrapeDB()
    extrinsic = db.read_extrinsic_metadata("14815834-2")
    assert extrinsic.extrinsic_hash == '0xc015e661ce5a763d2377d5216037677f5e16fe1a5ec4471de3acbd6be683461b'

    db.close()
