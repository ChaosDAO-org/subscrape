import io
import json
import logging

# A fat model of a Parachain
# Knows how to interpret Subscan data
# Knows about serialization to disk
class Parachain:

    def __init__(self, db_path, endpoint):
        self.logger = logging.getLogger("Parachain")

        self.db_path = db_path
        self.endpoint = endpoint

        self.addresses = []

    async def fetch_addresses(self, subscan):
        assert(len(self.addresses) == 0)
        self.logger.info("Fetching accounts from " + self.endpoint)

        method = "/api/v2/scan/accounts"
        url = self.endpoint + method

        await subscan.iterate_pages(url, self.process_account)

        file_path = self.db_path + "adresses.json"
        payload = json.dumps(self.addresses)
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

    def process_account(self, account):
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

        self.addresses.append(address)

