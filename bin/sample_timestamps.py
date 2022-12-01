import logging
import subscrape


def test():

    config = {
        "mangatax": {
            "extrinsics": {
                "timestamp": ["set"]
            },
        }
    }

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logging.info("scraping")
    items_scraped = subscrape.scrape(config)


test()
