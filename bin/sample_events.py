from subscrape.db.subscrape_db import SubscrapeDB
import logging
import subscrape
import asyncio


async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    db_connection_string="sqlite:///data/cache/sample_events.db"
    chain = "kusama"
    module_name = "auctions"
    event_name = "auctionclosed"

    config = {
        chain: {
            "_db_connection_string": db_connection_string,
            "_auto_hydrate": False,
            "events": {
                module_name: [event_name]
            }
        }
    }

    logging.info("scraping...")
    await subscrape.scrape(config)

    logging.info("transforming...")
    db = SubscrapeDB(db_connection_string)
    
    events = db.query_events(chain = chain, module = module_name, event = event_name).all()

    # print all event ids
    for event in events:
        print(event.id)

    db.close()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
