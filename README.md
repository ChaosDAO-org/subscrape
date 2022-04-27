# subscrape
This is a growing library of Python facilities to scrape Substrate and Moonbeam-based chains. Substrate chains are scraped using Subscan while EVM-based chains use Moonscan.io and Blockscout.io.

The basic workflow of `bin/scrape.py` considers the configuration presented in `data/scrape_config.json` 
to traverse through the given chains and perform the operations for each chain.
For Substrate, scraping extrinsics and transfers is supported.

Data is stored locally using `SubscanDB` and can be used to use the data and transform it. The application works in a way that subsequent runs will only fetch deltas.

`bin/transfers_all_chains.py` takes addresses and chains from `data/transfers_config.json` to create a CSV of
 all transfers associated with that accounts.

## Links
- [v2.0 Milestones](https://github.com/ChaosDAO-org/subscrape/milestone/1)
- [Roadmap](https://app.subsocial.network/@alice_und_bob/subscrape-releasing-v1-0-and-roadmap-for-v2-0-32075)
- [Initial announcement and important updates](https://twitter.com/alice_und_bob/status/1493714489014956037)

## Documentation
- [Configuration](docs/configuration.md)
- [Architecture](docs/architecture.md)

## Limitations
Error handling is not very sophisticated, so if the scrape is interrupted by an uncaught exception,
the delta might be incomplete and subsequent runs might miss some data. To remedy the issue,
the delta must be deleted and the scraper run again.

## Usage

### Installation
> virtualenv venv
> venv\scripts\activate
> pip install -Ur .\PipRequirements.txt
> bin\scrape.py

### Subscan API Key
If you have a Subscan API key, drop it in a file named `config/subscan-key`. Similarly, if you have a Moonscan.io API key, drop it in a file named  `config/moonscan-key`.

### Example applications
Take a look at the `/bin/` folder. There are some sample applications there.

### Using scrape.py as application
- copy `config/sample_scrape_config.json` to `config/scrape_config.json`
- configure to your desire. See [configuration](docs/configuration.md)
- run `scrape.py`
- corresponding files will be created in data/

### Consuming scrape.py as helper
- from scrape import scrape
- create a dict analogouos to `config/sample_scrape_config.json`
- call it inline via `scrape(config)`

### Extracting all transfers across chains for an address
- copy `config/sample_transfers_config.json` to `config/transfers_config.json`
- configure as needed.
- run `bin/transfers_all_chains.py`
- output is in `data/transfers.csv`
