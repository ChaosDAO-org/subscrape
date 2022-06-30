## Configuration

Users define a `scrape_config.json` file in the `config` folder to instruct `subscrape` what accounts or types of info they are interested in. To get started, just rename `sample_scrape_config.json` to `scrape_config.json` since it already shows you what syntax to use.

```
{
    "_version": 1,
    "kusama": {
        "transfers": {
            "F3opxRbN5ZbjJNU511Kj2TLuzFcDq9BGduA9TgiECafpg29": "Treasury"
        },
        "extrinsics": {
            "_filter": [{"block_timestamp": [{"<":1644796800}]}],
            "system": [
                "remark"
            ],
            "utility":{
                "_skip": true,
                "batch":{},
                "batch_all":{}
            }
        }
    },
    "moonriver": {
        "transactions": {
            "0xaa30ef758139ae4a7f798112902bf6d65612045f": [
                "0xe8e33700"
            ]
        },
        "account_transactions": {
            "accounts": [
                "0x1a93b23281cc1cde4c4741353f3064709a16197d"
            ]
        }
    }
}
```

### Config for scraping Substrate chains:

To query extrinsics from Substrate chains, only the module and call is needed. Filters can be applied.

### Scraper: extrinsics
Scrapes extrinsics by using their `call_module` and `call_name`.

### Config for scraping Moonriver or Moonbeam:

`subscrape` currently supports the following top-level "operations" when analysing Moonriver/beam activities:
* `transactions` operation
  * For each contract and contract method, `subscrape` will build a list of all accounts that have called that contract method. This is useful for collecting a list of wallets who have added liquidity to a DEX.
  * Inside `transactions` should be a list of contract addresses. For each contract address, one or more hex-formatted contract method ids should be specified. These can be found by scrolling to the input of a transaction on Moonscan and copying the method id. Example: https://moonriver.moonscan.io/tx/0x35c7d7bdd33c77c756e7a9b41b587a6b6193b5a94956d2c8e4ea77af1df2b9c3
* `account_transactions` operation
  * For each account listed, extract a list of all transactions by that wallet. The script will determine what type of activity has occurred and extract additional information if possible. For instance, if the transaction was a contract interaction with a DEX swapping tokens, `subscrape` can determine the names of the tokens and what exact quantities were swapped. Not yet supported are basic ERC-20 token transfers, adding DEX liquidty, and staking. But these can easily be added in the future without requiring any changes to your config file. Once these additional analysis features are incorporated, the data for each transaction will be updated to include a richer set of information about the transaction. Eventually, that can be pumped out to a spreadsheet to create a clean list of taxable events.

### General configuration:

When scraping either Substrate chains or EVM chains, the following additional modifiers can be applied at any level to help curate what data is extracted.

#### Filter: _skip
Will skip the current scope of the config.

#### Filter: _filter
`"_filter": [{"block_timestamp": [{"<":1644796800}]}],`

#### `_version` identifier
This will be useful in the future if breaking changes are needed. But for now, just leave it as `1`.









