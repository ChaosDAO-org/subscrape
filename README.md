# subscrape
This is a growing library of Python facilities to scrape Substrate and Moonbeam-based chains. Substrate chains are scraped using Subscan while EVM-based chains use Moonscan.io and Blockscout.io.

Roadmap: https://app.subsocial.network/@alice_und_bob/subscrape-releasing-v1-0-and-roadmap-for-v2-0-32075

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
- [Grant Proposal](https://github.com/orgs/ChaosDAO-org/projects/2/views/1)

## Documentation
- [Configuration](docs/configuration.md)
- [Architecture](docs/architecture.md)

## Limitations
Error handling is not very sophisticated, so if the scrape is interrupted by an uncaught exception,
the delta might be incomplete and subsequent runs might miss some data. To remedy the issue,
the delta must be deleted and the scraper run again.

## Usage

### Installation
```sh
virtualenv venv
venv\scripts\activate
pip install -Ur .\PipRequirements.txt
```

### API Keys
If you have a Subscan API key, drop it in a file named `config/subscan-key`. If you have a Moonscan.io API key, note that they are network-specific, so place it either in a file named `config/moonscan-moonriver-key` or `config/moonscan-moonbeam-key`. Blockscout does not need an API key.

### Example applications
Here are several specific examples of how the `subscrape` library has been used in the past. There are several example application scripts in the `/bin/` folder.
* Prepare your taxable transactions - Use `scrape.py` and specify a list of accounts in your `scrape_config.json` (using operation `account_transactions`) to extract all transactions for an account. Multiple accounts and chains can be specified in the config file. Results are currently dumped to a JSON file in the `data` folder but later we can export this to a CSV file so you can import it into your favorite tax software which would match up transactions to calculate your capital gains.
* Determine all accounts that have used a DEX - Use `scrape.py` and operation `transactions` in your `scrape_config.json` to extract a list of all addresses which have called a particular smart contract operation, such as making a trade on `solarbeam.io`. (In `sample_scrape_config.json`, see the Moonriver config to extract all transactions with contract `0xaa30ef758139ae4a7f798112902bf6d65612045f`'s method `0xe8e33700`.)
* Extract a list of all RMRK Skybreach land deeds which have been burned, and what Moonriver address they were redeemed to - See `sample_rmrk_burns.py` for an example script that looks for system remark calls which append a memo with an EVM (`0x`) address.
* Extract all transfers across chains for an address - copy `config/sample_transfers_config.json` to `config/transfers_config.json`, customize as needed, and then run `bin/transfers_all_chains.py` to extract a list of all transfers for an address across all chains, which will be dumped into `data/transfers.csv`.

### Using scrape.py as an application
- copy `config/sample_scrape_config.json` to `config/scrape_config.json`
- configure to your desire. See [configuration](docs/configuration.md)
- run `scrape.py`
- corresponding files will be created in data/

### Consuming scrape.py as a helper
- from scrape import scrape
- create a dict analogouos to `config/sample_scrape_config.json`
- call it inline via `scrape(config)`

