from ast import Dict
from concurrent.futures import process
import io
import os
import json
import logging
import copy
from tkinter import NONE
from typing import List

class ParachainScrapeConfig:
    def __init__(self, config):
        self.filter = None
        self.processor_name = None
        self.skip = False

    def _set_config(self, config):
        if type(config) is list:
            return

        filter_conditions = config.get("_filter", None)
        if filter_conditions is not None:
            self.filter = self.filter_factory(filter_conditions)

        processor_name = config.get("_processor", None)
        if processor_name is not None:
            self.processor_name = processor_name

        skip = config.get("_skip", None)
        if skip is not None:
            self.skip = skip

    # creates a config that can be nested to lower layers
    def create_inner_config(self, config):
        result = copy.deepcopy(self)
        result._set_config(config)
        return result


# A generic scraper for parachains
class ParachainScraper:

    def __init__(self, db_path, api):
        self.logger = logging.getLogger("ParachainScraper")

        self.db_path = db_path
        self.api = api

        self.addresses = []
        self.extrinsics = {}

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
                    await self.fetch_extrinsics(module, call, call_config.filter, processor)
        elif operation == "addresses":
            await self.fetch_addresses()
        else:
            self.logger.error(f"config contained an operation that does not exist: {operation}")            
            exit

    def processor_factory(self, name, call_string):
        processor = None
        if name == "count_from_addresses" or name is None:
            processor = self.dispatch_address_count_processor_factory(call_string)
        elif name == "rmrk":
            processor = self.remark_processor_factory(call_string)
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
        return processor

    def remark_processor_factory(self, call_string):
        def processor(extrinsic):
            params = json.loads(extrinsic["params"])
            assert(len(params) == 1)
            value = params[0]["value"]
            extrinsic_index = extrinsic["extrinsic_index"]
            self.extrinsics[call_string][extrinsic_index] = value
        return processor

    def params_processor_factory(self, call_string):
        def params_processor(extrinsic):
            params = json.loads(extrinsic["params"])
            
            result = {"account_id": extrinsic["account_id"]}

            for param in params:
                type_name = param["name"]
                value = param["value"]
                result[type_name] = value

            extrinsic_index = extrinsic["extrinsic_index"]
            self.extrinsics[call_string][extrinsic_index] = result
        return params_processor


    # returns true if the extrinsic should be skipped because it hits a filter condition
    def filter_factory(self, conditions):
        if conditions is None:
            return None
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
        account_display = account["account_display"]
        address = account_display["address"]
        self.addresses.append(address)

    async def fetch_extrinsics(self, call_module, call_name, filter, processor):
        call_string = f"{call_module}_{call_name}"
        assert(call_string not in self.extrinsics)

        file_path = self.db_path + f"extrinsics_{call_module}_{call_name}.json"
        if os.path.exists(file_path):
            self.logger.warn(f"{file_path} already exists. Skipping.")
            return

        self.logger.info(f"Fetching extrinsics {call_string} from {self.api.endpoint}")

        self.extrinsics[call_string] = {}

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