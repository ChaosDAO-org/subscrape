# subscrape
A Python scraper for substrate chains that uses Subscan.

## Usage
- copy config/sample_scrape_config.json to config/scrape_config.json and configure to your desire.
- make sure there is a data/parachains folder
- run
- corresponding files will be created in data/

If a file already exists in data/, that operation will be skipped in subsequent runs.

### Configuration

To query extrinsics from Substrate chains, only the module and call is needed.

Utility.batch() and Utility.batch_all() calls are also recursively searched for the extrinsic.

Filters can be applied:

"_filter": [{"block_timestamp": [{"<":1644796800}]}],


To query transactions from Moonbeam chains, the contract address and the hex-formatted method id is needed. This can be found by scrolling to the input of a transaction on moonbeam and copying the method id. Example: https://moonriver.moonscan.io/tx/0x35c7d7bdd33c77c756e7a9b41b587a6b6193b5a94956d2c8e4ea77af1df2b9c3


## Architecture
On overview is given in this Twitter thread: https://twitter.com/alice_und_bob/status/1493714489014956037

### General
We use the following methods in the projects:
- Logging: https://docs.python.org/3/howto/logging.html
- Async Operations: https://docs.python.org/3/library/asyncio-task.html

### SubscanWrapper
There is a class SubscanWrapper that encapsulates the logic around calling Subscan.
API: https://docs.api.subscan.io/
If you have a Subscan API key, you can put it in the main folder in a file called "subscan-key" and it will be applied to your calls.

### ParachainScraper
This is a scraoer that knows how to use the SubscanWrapper to fetch data for a parachain and serialize it to disk.

Currently it knows how to fetch addresses and extrinsics.