from audioop import add
import logging
import os
import asyncio
from subscrape.subscan_wrapper import SubscanWrapper

# Setup
## Logging
log_level = logging.INFO
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger()
log.info("Hello World!")

#import http.client
#http.client.HTTPConnection.debuglevel = 1
#requests_log = logging.getLogger("requests.packages.urllib3")
#requests_log.setLevel(logging.DEBUG)
#requests_log.propagate = True



# Methods
def process_account(account):
        # example result: 
        # {"code":0,"message":"Success","generated_at":1644941702,"data":
        #   {"count":238607,"list":[
        #       {"account_display":{"account_index":"54TxD","address":"DgCKZyuptaHJeVaA9X6o7vdgPRXUSXo8sQwpe9hMyi8TVYB","display":"","identity":false,"judgements":null,"parent":null},
        #        "address":"DgCKZyuptaHJeVaA9X6o7vdgPRXUSXo8sQwpe9hMyi8TVYB","balance":"13.762980093164","balance_lock":"13.5","derive_token":null,"is_erc20":false,"is_evm_contract":false,"lock":"13.5","registrar_info":null}
        #   ]}
        #  }
    account_display = account["account_display"]
    address = account_display["address"]
    # we could put them in a list now or return them.


async def main():
    ## Load Subscan API Key
    api_key = None
    if os.path.exists("subscan-key"):
        f = open("subscan-key")
        api_key = f.read()
    log.debug(api_key)

    subscan = SubscanWrapper(api_key)
    endpoint = "https://kusama.api.subscan.io"
    method_transfers = "/api/scan/transfers"
    method_accounts_list = "/api/v2/scan/accounts"

    url = endpoint + method_accounts_list
    await subscan.iterate_pages(url, process_account)
    

asyncio.run(main())