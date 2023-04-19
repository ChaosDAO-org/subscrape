from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import asyncio
import pandas as pd
from substrateinterface.utils.ss58 import ss58_encode

async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    db_connection_string="sqlite:///data/cache/sample_crowdloan_contributions.db"
    chain = "kusama"
    module_name = "crowdloan"
    event_name = "contributed"

    config = {
        chain: {
            "_db_connection_string": db_connection_string,
            "_auto_hydrate": True,
            "events": {
                "crowdloan": ["contributed", "memoupdated"]
            },
            "_params":{
                "block_range": "16271109-16668600"
            }
        }
    }

    logging.info("scraping...")
    await subscrape.scrape(config)

    logging.info("transforming...")
    db = SubscrapeDB(db_connection_string)

    contributions = db.query_events(chain=chain, module="crowdloan", event="contributed").all()
    contribution_map = {}
    #extrinsics_list = []

    for event in contributions:
        if event.params[1]["value"] != 2256:
            continue
        contribution_map[event.extrinsic_id] = {
            "event_id": event.id,
            "account": ss58_encode(event.params[0]["value"],2),
            "amount": event.params[2]["value"],
            "params": event.params,
        }
        #extrinsics_list.append(event.extrinsic_id)

    df = pd.DataFrame(contribution_map.values(), index=contribution_map.keys())

    memos = db.query_events(chain=chain, module="crowdloan", event="memoupdated").all()
    
    for memo in memos:
        if memo.params[1]["value"] != 2256:
            continue
        df.loc[memo.extrinsic_id, "memo"] = ss58_encode(memo.params[2]["value"])

    df.to_csv("data/cache/sample_crowdloan_contributions.csv", index=False)
    '''
    config = {
        chain: {
            "_db_connection_string": db_connection_string,
            "extrinsics-list": extrinsics_list
        }
    }

    await subscrape.scrape(config)    

    extrinsics = db.query_extrinsics(chain=chain, extrinsic_ids=extrinsics_list).all()
    extrinsics_transformed = []

    for extrinsic in extrinsics:
        if extrinsic.module == "crowdloan" or extrinsic.call == "contribute":
            event = extrinsic.events[0]
            extrinsics_transformed.append({
                "extrinsic_id": extrinsic.id,
                "account": ss58_encode(event.params[0]["value"],2),
                "amount": event.params[1]["value"],
            })
        else:
            assert False

    df = pd.DataFrame(extrinsics_transformed)
    df.to_csv("data/cache/sample_crowdloan_contributions_extrinsics.csv", index=False)
    '''
    db.close()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
