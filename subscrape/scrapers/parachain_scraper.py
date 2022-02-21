from ast import Dict
from concurrent.futures import process
import io
import os
import json
import logging
from tkinter import NONE

# A generic scraper for parachains
class ParachainScraper:

    def __init__(self, db_path, api):
        self.logger = logging.getLogger("ParachainScraper")

        self.db_path = db_path
        self.api = api

        self.addresses = []
        self.extrinsics = {}

    async def perform_operation(self, operation, payload):
        if operation == "extrinsics":
            filter = payload.pop("_filter", None)
            for module in payload:
                calls = payload[module]
                for call in calls:
                    await self.fetch_extrinsics(module, call, filter)
        elif operation == "addresses":
            await self.fetch_addresses()
        else:
            self.logger.error(f"config contained an operation that does not exist: {operation}")            
            exit

    # returns true if the extrinsic should be skipped because it hits a filter condition
    def filter_factory(self, conditions):
        def filter(extrinsic):
            for group in conditions:
                for key in group:
                    if key not in extrinsic:
                        return True
                    actual_value = extrinsic[key]
                    predicates = group[key]
                    for predicate in predicates:
                        if "<" in predicate:
                            value = predicate["<"]
                            if actual_value < value:
                                continue
                            else:
                                return True
            return False
        return filter

    async def fetch_addresses(self):
        assert(len(self.addresses) == 0)

        file_path = self.db_path + "adresses.json"
        if os.path.exists(file_path):
            self.logger.warn(f"{file_path} already exists. Skipping.")
            return

        self.logger.info("Fetching accounts from " + self.endpoint)

        method = "/api/v2/scan/accounts"
        url = self.endpoint + method

        await self.api.iterate_pages(url, self.process_account,
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

    async def fetch_extrinsics(self, call_module, call_name, filter_conditions):
        call_string = f"{call_module}_{call_name}"
        assert(call_string not in self.extrinsics)

        file_path = self.db_path + f"extrinsics_{call_module}_{call_name}.json"
        if os.path.exists(file_path):
            self.logger.warn(f"{file_path} already exists. Skipping.")
            return

        self.logger.info(f"Fetching extrinsics {call_string} from {self.api.endpoint}")

        self.extrinsics[call_string] = {}

        method = "/api/scan/extrinsics"
        filter = None
        if filter_conditions is not None:
            filter = self.filter_factory(filter_conditions)

        def process_extrinsic_hit(extrinsic):
            address = extrinsic["account_id"]
            if address not in self.extrinsics[call_string]:
                self.extrinsics[call_string][address] = 1
            else:
                self.extrinsics[call_string][address] += 1
        
        # recursively goes through batched calls to check for an actual hit
        def process_batch_hit(extrinsic):
            params = json.loads(extrinsic["params"])
            assert(len(params) == 1)
            calls = params[0]["value"]
            for call in calls:
                actual_call_module = call["call_module"].lower()
                actual_call_name = call["call_name"].lower()
                if actual_call_module == call_module and actual_call_name == call_name:
                    process_extrinsic_hit(extrinsic)
                elif actual_call_module == "utility" and (actual_call_name == "batch" or actual_call_name == "batch_all"):
                    process_batch_hit(call)                    


        body = {"module": call_module, "call": call_name}
        await self.api.iterate_pages(
            method,
            process_extrinsic_hit,
            list_key="extrinsics",
            body=body,
            filter=filter
            )

        body = {"module": "utility", "call": "batch"}
        await self.api.iterate_pages(
            method,
            process_batch_hit,
            list_key="extrinsics",
            body=body,
            filter=filter
            )

        body = {"module": "utility", "call": "batch_all"}
        await self.api.iterate_pages(
            method,
            process_batch_hit,
            list_key="extrinsics",
            body=body,
            filter=filter
            )

        file_path = self.db_path + f"extrinsics_{call_module}_{call_name}.json"
        payload = json.dumps(self.extrinsics[call_string])
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

