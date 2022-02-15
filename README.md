# subscrape
A Python scraper for substrate chains that uses Subscan.

# Architecture

## General
We use the following methods in the projects:
- Logging: https://docs.python.org/3/howto/logging.html
- Async Operations: https://docs.python.org/3/library/asyncio-task.html

## SubscanWrapper
There is a class SubscanWrapper that encapsulates the logic around calling Subscan.
API: https://docs.api.subscan.io/
If you have a Subscan API key, you can put it in the main folder in a file called "subscan-key" and it will be applied to your calls.

## ParachainScraper
This is a scraoer that knows how to use the SubscanWrapper to fetch data for a parachain and serialize it to disk.

Currently it knows how to fetch addresses and extrinsics.