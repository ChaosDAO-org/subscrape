On overview with practical examples is given in this Twitter thread: https://twitter.com/alice_und_bob/status/1493714489014956037

## General
We use the following methods in the projects:
- Logging: https://docs.python.org/3/howto/logging.html
- Async Operations: https://docs.python.org/3/library/asyncio-task.html

## SubscanWrapper
There is a class `SubscanWrapper` that encapsulates the logic around calling Subscan.
API: https://docs.api.subscan.io/
If you have a Subscan API key, you can put it in the main folder in a file called "subscan-key" and it will be applied to your calls.

## ParachainScraper
`ParachainScraper` knows how to use the `SubscanWrapper` to fetch data for a parachain and serialize it to disk.

Currently it knows how to fetch extrinsics.

## MoonscanWrapper
Analoguous to `SubscanWrapper`

## MoonbeamScraper
Analoguous to `ParachainScraper`. Can scan for `transactions` of a method from a contract or `account_transactions`.

## SubscanDB
`SubscanDB` serializes extracted data to disk and unserializes it later.

## ScrapeConfig
`ScrapeConfig` is a helper class that helps bubble configuration properties from the outermost configuration elements to the innermost.