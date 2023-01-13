An overview with practical examples is given in this Twitter thread: https://twitter.com/alice_und_bob/status/1493714489014956037

## General
We use the following methods in the projects:
- Logging: https://docs.python.org/3/howto/logging.html

## SubscanWrapper
There is a class `SubscanWrapper` that encapsulates the logic around calling Subscan.
API: https://docs.api.subscan.io/
If you have a Subscan API key, you can put it in `config\subscan-key` and it will be applied to your calls. This significantly increases the number of queries per second that can be submitted.

## ParachainScraper
`ParachainScraper` uses `SubscanWrapper` to fetch data for a parachain and serialize it to disk.

## MoonscanWrapper
Analoguous to `SubscanWrapper` but for scraping transactions from Moonscan.io. If you have a Moonscan API key, you can put it in `config\moonscan-key` and it will be applied to your calls.

## BlockscoutWrapper
Analoguous to `SubscanWrapper` but for scraping transactions from Blockscout.io. Blockscout.io does not use API keys. The methods in Blockscout.io are meant to compliment those in MoonscanWrapper instead of entirely replicating them. Specifically, Blockscout supports `getToken` which Moonscan doesn't. Likewise, Moonscan.io supports `eth_getTransactionReceipt` which Blockscout doesn't. Therefore using the two APIs in combination provides the widest functionality.

## MoonbeamScraper
Analoguous to `ParachainScraper`. Can scan for `transactions` of a method from a contract or `account_transactions`. For transactions involving contract interactions, MoonbeamScraper will retrieve the ABI (interface) for the contract, decode the transaction input data, decode what tokens are involved, and then retrieve the transaction receipts/logs for the transactions to determine what the exact final token values were for the operation.

## SubscanDB
`SubscanDB` serializes extracted data to disk and unserializes it later.

## ScrapeConfig
`ScrapeConfig` is a helper class that helps bubble configuration properties from the outermost configuration elements to the innermost. It is fairly well integrated into the code, so usually the steps to add new config parameters are:
- Add documentation to `docs/configuration.md`
- Add a unit test to validate the behavior of the new config
- Query the param in code and use it