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
    extrinsics = db.extrinsics_query("bounties", "propose_bounty").all()

    extrinsics = [e for e in extrinsics if e.id == "14061443-2"]
    assert len(extrinsics) == 1, "Expected 1 extrinsic"
    extrinsic = extrinsics[0]
    assert extrinsic.extrinsic_hash == '0x9f2a81d8d92884122d122d806276da7ff9b440a0a273bc3898cbd4072d5f62e1'
    assert extrinsic.params is not None, "Hydrated extrinsic should have params"
    
    db.close()

@pytest.mark.asyncio
async def test_fetch_all_extrinsics_from_module():
    
    config = {
        "kusama":{
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
    extrinsic = db.extrinsics_query("bounties", "propose_bounty").get("12935940-3")
    assert extrinsic is not None
    assert extrinsic.extrinsic_hash == '0x28b3e9dc097036a98b43b9792745be89d3fecbbca71200b45a2aba901c7cc5af'
    assert extrinsic.params is not None

    extrinsic = db.extrinsics_query("bounties", "extend_bounty_expiry").get("14534356-3")
    assert extrinsic is not None
    assert extrinsic.extrinsic_hash == '0xf02b930789a35b4b942006c60ae6c83daee4d87237e213bab4ce0e7d93cfb0f4'
    assert extrinsic.params is not None

    db.close()

@pytest.mark.asyncio
async def test_fetch_all_extrinsics_from_address():
    
    config = {
        "kusama":{
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
    extrinsic = db.extrinsics_query.get("14815834-2")
    assert extrinsic.extrinsic_hash == '0xc015e661ce5a763d2377d5216037677f5e16fe1a5ec4471de3acbd6be683461b'

    db.close()
