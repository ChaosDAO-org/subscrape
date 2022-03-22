## Configuration

To query extrinsics from Substrate chains, only the module and call is needed. Filters can be applied.

### Scraper: extrinsics
Scrapes extrinsics by using their `call_module` and `call_name`.

### Filter: _skip
Will skip the current scope of the config.

### Filter: _filter
`"_filter": [{"block_timestamp": [{"<":1644796800}]}],`

To query transactions from Moonbeam chains, the contract address and the hex-formatted method id is needed. This can be found by scrolling to the input of a transaction on moonbeam and copying the method id. Example: https://moonriver.moonscan.io/tx/0x35c7d7bdd33c77c756e7a9b41b587a6b6193b5a94956d2c8e4ea77af1df2b9c3


