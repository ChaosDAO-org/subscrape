# subscrape
A Python scraper for substrate chains that uses Subscan.

The basic workflow if `scrape.py` considers the configuration presented in `data/scrape_config.json`
to traverse through the given chains and perform the operations for each chain.
Currently, only scraping extrinsics is supported.

Data is stored locally using `SubscanDB`. It can then be queried. An example is provided with `transform.py`.

The application works in a way that subsequent runs will only fetch deltas.

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
- If you have a Subscan API key, drop it in a file named `config/subscan-key`
- copy `config/sample_scrape_config.json` to `config/scrape_config.json`
- configure to your desire. See [configuration](docs/configuration.md)
- run `scrape.py`
- corresponding files will be created in data/
