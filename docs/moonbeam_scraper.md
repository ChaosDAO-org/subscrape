## Moonbeam_scraper.py


### Supported Transaction Methods

Contract methods currently supported:
* DEX swap methods: `swapExactTokensForTokens`, `swapExactTokensForETH`, `swapExactTokensForTokensSupportingFeeOnTransferTokens`, `swapExactTokensForETHSupportingFeeOnTransferTokens`
  * Note: `moonbeam_scraper` validates that all decoded DEX trace transaction quantities are within 20% of the original transaction request, to ensure the validity of our decoding logic. However, the DEX 'slippage' can vary a lot, especially for Tokens charging a tax when being sold (see `SupportingFeeOnTransferTokens` methods) which can result in 4-10% slippage, or during high transaction volume events such as initial token/contract launches. Therefore a warning is printed out if slippage is >=20% but the swap is still considered valid and included in the output list of transactions.

Currently not supported:
* DEX swap methods: `swapETHForTokens`   (only has one Tx input. Needs more research.)
* Staking
* Liquidity Provision
* everything else


### JSON Output Data Format
While subscrape is still being developed, all extracted transactions are currently dumped to an output JSON file instead of a carefully structure spreadsheet. Normal (non-contract) transactions will have a basic format like:
```
    "1633543326": {
        "utcdatetime": "2021-10-06 18:02:06",
        "hash": "0x8bc0d70495c963e0c09d8c9c35e84b44a74e021315a267e678c28bf2601873c4",
        "from": "0x97c98d2f4a587942aa8c069b30e17895c6e7957a",
        "to": "0x120999312896f36047fbcc44ad197b7347f499d6",
        "valueInWei": "8000000000000000000",
        "value": 8,
        "gas": "347787",
        "gasPrice": "1000000000",
        "gasUsed": "97005"
    },
```
The `1633543326` key is simply the unix timestamp so that all of the transactions are listed in chronological order. The `value` is the human-readable decimal value of native currency in the transaction (MOVR, GLMR) whereas `valueInWei` is the raw blockchain value.

For DEX token swaps, additional data is decoded and the output format expands to:
```
    "1633155912": {
        "utcdatetime": "2021-10-02 06:25:12",
        "hash": "0x0e005163ef61ad6f4979da99c69931e19982cc008c35bb8a7f76de8b1d51068f",
        "from": "0x97c98d2f4a587942aa8c069b30e17895c6e7957a",
        "to": "0xaa30ef758139ae4a7f798112902bf6d65612045f",
        "valueInWei": "0",
        "value": 0,
        "gas": "756248",
        "gasPrice": "1000000000",
        "gasUsed": "125865",
        "input_token_name": "SolarBeam Token",
        "input_symbol": "SOLAR",
        "output_token_name": "MOONKAFE",
        "output_symbol": "KAFE",
        "input_quantity": 48.940982204694954,
        "output_quantity": 73.31949903615119
    },
```
Here you can see that transaction `0x0e005163ef61ad6f4979da99c69931e19982cc008c35bb8a7f76de8b1d51068f` involved swapping SOLAR tokens for KAFE tokens. The `input_quantity` and `output_quantity` are the exact values of the swap transaction after moonbeam_scraper decodes the DEX trace transactions.

### Error handling
Since the Moonbeam scraper has become so featureful now, there are a lot of corner cases and error conditions that it can encounter. Here are some of them and what (if anything) you should know about them.


* `INFO - ABI not retrievable for 0x0000000000000000000000000000000000000800 because "Contract source code not verified"`
  * This means that the ABI (contract interface) could not be retrieved for the contract. As a result, moonbeam_scraper will be unable to decode the input data for the transaction. Native token transfers will still be included in the subscrape output data, but no contract interactions will be logged for this contract. This message is only printed once for the contract, instead of once per transaction involving this contract.

* `WARNING - Unable to decode contract interaction with contract 0xbcc8a3022f69a5cdddc22c068049cd07581b1aa5 in transaction:`
  * Sometimes people create intentionally malformed contracts to play games. When you try to decode the input data to these contracts, you may receive this error. The transaction details and the stacktrace will be printed out for informational purposes and then the script will continue processing the rest of the scraped transactions for the account.

