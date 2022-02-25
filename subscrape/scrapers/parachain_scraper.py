from ast import Dict
from concurrent.futures import process
import os
import json
import logging
from tkinter import NONE
from typing import List
from subscrape.scrapers.parachain_scrape_config import ParachainScrapeConfig
from subscrape.db.subscrape_db import SubscrapeDB

# A generic scraper for parachains
class ParachainScraper:

    def __init__(self, db, api):
        self.logger = logging.getLogger("ParachainScraper")
        self.db = db
        self.api = api

    async def perform_operation(self, operation, modules):
        if operation == "extrinsics":

            extrinsic_config = ParachainScrapeConfig(modules)

            for module in modules:
                # ignore metadata
                if module.startswith("_"):
                    continue

                calls = modules[module]
                module_config = extrinsic_config.create_inner_config(calls)
                
                for call in calls:
                    # ignore metadata
                    if call.startswith("_"):
                        continue

                    # deduce config
                    if type(calls) is dict:
                        call_config = module_config.create_inner_config(calls[call])
                    else:
                        call_config = module_config

                    # config wants us to skip this call?
                    if call_config.skip:
                        self.logger.info(f"Config asks to skip {module} {call}")
                        continue

                    # create the proper processor
                    call_string = f"{module}_{call}"
                    processor = self.processor_factory(call_config.processor_name, call_string)

                    # go
                    await self.fetch_extrinsics(module, call, call_config.filter, processor, call_config.include_batch_calls)
        elif operation == "addresses":
            await self.fetch_addresses()
        else:
            self.logger.error(f"config contained an operation that does not exist: {operation}")            
            exit

    def processor_factory(self, name, call_string):
        processor = None
        if name == "count_from_addresses" or name is None:
            processor = self.dispatch_address_count_processor_factory(call_string)
        elif name == "params":
            processor = self.params_processor_factory(call_string)
        else:
            self.logger.error(f"unknown processor given for {call_string}: {name}")
            raise Exception()
        return processor

    def dispatch_address_count_processor_factory(self, call_string):
        def processor(extrinsic):
            address = extrinsic["account_id"]
            if address not in self.extrinsics[call_string]:
                self.extrinsics[call_string][address] = 1
            else:
                self.extrinsics[call_string][address] += 1
            return True
        return processor

    def params_processor_factory(self, call_string):
        def params_processor(extrinsic):
            params = json.loads(extrinsic["params"])
            
            result = {"account_id": extrinsic["account_id"]}

            for param in params:
                name = param["name"]
                value = param["value"]
                result[name] = value

            extrinsic_index = extrinsic["extrinsic_index"]
            should_continue = self.db.set_extrinsic(call_string, extrinsic_index, result)
            return should_continue
        return params_processor

    async def fetch_extrinsics(self, call_module, call_name, filter, processor, include_batch_calls):
        call_string = f"{call_module}_{call_name}"

        self.db.warmup_extrinsics(call_string)

        self.logger.info(f"Fetching extrinsics {call_string} from {self.api.endpoint}")

        method = "/api/scan/extrinsics"
        
        # recursively goes through batched calls to check for an actual hit
        def process_batch_hit(extrinsic):
            params = extrinsic["params"]
            if type(params) is str:
                params = json.loads(params)
            assert(len(params) == 1)
            calls = params[0]["value"]
            
            # check for empty batch
            if calls is None:
                return

            for call in calls:
                actual_call_module = call["call_module"].lower()
                actual_call_name = call["call_name"].lower()
                if actual_call_module == call_module and actual_call_name == call_name:
                    processor(extrinsic)
                elif actual_call_module == "utility" and (actual_call_name == "batch" or actual_call_name == "batch_all"):
                    process_batch_hit(call)                    


        body = {"module": call_module, "call": call_name}
        await self.api.iterate_pages(
            method,
            processor,
            list_key="extrinsics",
            body=body,
            filter=filter
            )

        if include_batch_calls:
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

        self.db.flush_extrinsics()

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
        account_display = account["account_display"]
        address = account_display["address"]
        self.addresses.append(address)
        return True

