# subscrape
A Python scraper for substrate chains that uses Subscan.


The basic workflow if `scrape.py` considers the configuration presented in `data/scrape_config.json`
to traverse through the given chains and perform the operations for each chain.
Currently, only scraping extrinsics is supported.

Data is stored locally using `SubscanDB`. It can then be queried. An example is provided with `transform.py`.

The application works in a way that subsequent runs will only fetch deltas.

## Limitations
Error handling is not very sophisticated, so if the scrape is interrupted by an uncaught exception,
the delta might be incomplete and subsequent runs might miss some data. To remedy the issue,
the delta must be deleted and the scraper run again.

## Usage
- If you have a Subscan API key, drop it in a file named `config/subscan-key`
- copy `config/sample_scrape_config.json` to `config/scrape_config.json` and configure to your desire.
- run `scrape.py`
- corresponding files will be created in data/

## Configuration

To query extrinsics from Substrate chains, only the module and call is needed. Filters can be applied.

### Scraper: extrinsics
Scrapes extrinsics by using their `call_module` and `call_name`.

### Filter: _skip
Will skip the current scope of the config.

### Filter: _filter
`"_filter": [{"block_timestamp": [{"<":1644796800}]}],`

To query transactions from Moonbeam chains, the contract address and the hex-formatted method id is needed. This can be found by scrolling to the input of a transaction on moonbeam and copying the method id. Example: https://moonriver.moonscan.io/tx/0x35c7d7bdd33c77c756e7a9b41b587a6b6193b5a94956d2c8e4ea77af1df2b9c3


## Architecture
On overview is given in this Twitter thread: https://twitter.com/alice_und_bob/status/1493714489014956037

### General
We use the following methods in the projects:
- Logging: https://docs.python.org/3/howto/logging.html
- Async Operations: https://docs.python.org/3/library/asyncio-task.html

### SubscanWrapper
There is a class `SubscanWrapper` that encapsulates the logic around calling Subscan.
API: https://docs.api.subscan.io/
If you have a Subscan API key, you can put it in the main folder in a file called "subscan-key" and it will be applied to your calls.

### ParachainScraper
`ParachainScraper` knows how to use the `SubscanWrapper` to fetch data for a parachain and serialize it to disk.

Currently it knows how to fetch extrinsics.

### MoonscanWrapper
Analoguous to `SubscanWrapper`

### MoonbeamScraper
Analoguous to `ParachainScraper`. Can scan for `transactions` of a method from a contract or `account_transactions`.

### SubscanDB
`SubscanDB` serializes extracted data to disk and unserializes it later.