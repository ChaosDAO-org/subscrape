from concurrent.futures import process
import io
import os
import json
import logging

# A generic scraper for parachains
class ParachainScraper:

    def __init__(self, db_path, endpoint, subscan):
        self.logger = logging.getLogger("Parachain")

        self.db_path = db_path
        self.endpoint = endpoint
        self.subscan = subscan

        self.addresses = []
        self.extrinsics = {}

    async def perform_operation(self, operation, payload):
        if operation == "extrinsics":
            for module in payload:
                calls = payload[module]
                for call in calls:
                    await self.fetch_extrinsics(module, call)
        elif operation == "addresses":
            await self.fetch_addresses()
        else:
            self.logger.error(f"config contained an operation that does not exist: {operation}")            
            exit

    async def fetch_addresses(self):
        assert(len(self.addresses) == 0)

        file_path = self.db_path + "adresses.json"
        if os.path.exists(file_path):
            self.logger.warn(f"{file_path} already exists. Skipping.")
            return

        self.logger.info("Fetching accounts from " + self.endpoint)

        method = "/api/v2/scan/accounts"
        url = self.endpoint + method

        await self.subscan.iterate_pages(url, self.process_account,
            list_key = "list")

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
        self.addresses.append(address)

    async def fetch_extrinsics(self, module, call):
        module_call = f"{module}_{call}"
        assert(module_call not in self.extrinsics)

        file_path = self.db_path + f"extrinsics_{module}_{call}.json"
        if os.path.exists(file_path):
            self.logger.warn(f"{file_path} already exists. Skipping.")
            return

        self.logger.info(f"Fetching extrinsics {module_call} from {self.endpoint}")

        self.extrinsics[module_call] = {}

        method = "/api/scan/extrinsics"
        url = self.endpoint + method

        body = {"module": module, "call": call}
        processor = self.process_extrinsic_factory(module_call)

        await self.subscan.iterate_pages(
            url,
            processor,
            list_key="extrinsics",
            body=body
            )

        file_path = self.db_path + f"extrinsics_{module}_{call}.json"
        payload = json.dumps(self.extrinsics[module_call])
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

    def process_extrinsic_factory(self, module_call):
        def process_extrinsic(extrinsic):
            address = extrinsic["account_id"]
            if address not in self.extrinsics[module_call]:
                self.extrinsics[module_call][address] = 1
            else:
                self.extrinsics[module_call][address] += 1
        
        return process_extrinsic