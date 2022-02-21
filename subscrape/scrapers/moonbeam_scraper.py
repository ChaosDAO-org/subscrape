import os
import logging
import json
import io
from eth_utils import keccak

class MoonbeamScraper:
    def __init__(self, db_path, api):
        self.logger = logging.getLogger("MoonbeamScraper")
        self.db_path = db_path
        self.api = api
        self.transactions = {}

    async def scrape(self, operations):
        for operation in operations:
            payload = operations[operation]
            if operation == "transactions":
                for contract in payload:
                    methods = payload[contract]
                    for method in methods:
                        await self.fetch_transactions(contract, method)
            else:
                self.logger.error(f"config contained an operation that does not exist: {operation}")            
                exit

    async def fetch_transactions(self, contract, method):
        contract_method = f"{contract}_{method}"
        assert(contract_method not in self.transactions)

        file_path = self.db_path + f"transactions_{contract_method}.json"
        if os.path.exists(file_path):
            self.logger.warn(f"{file_path} already exists. Skipping.")
            return

        self.logger.info(f"Fetching transactions {contract_method} from {self.api.endpoint}")

        self.transactions[contract_method] = {}

        processor = self.process_methods_in_transaction_factory(contract_method, method)

        params = {}
        params["module"] = "account"
        params["action"] = "txlist"
        params["address"] = contract
        params["startblock"] = "1"
        params["endblock"] = "99999999"
        params["sort"] = "asc"

        await self.api.iterate_pages(processor, params=params)

        payload = json.dumps(self.transactions[contract_method])
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

    def process_methods_in_transaction_factory(self, contract_method, method):
        def process_method_in_transaction(transaction):
            if transaction["input"][0:10] == method:
                address = transaction["from"]
                if address not in self.transactions[contract_method]:
                    self.transactions[contract_method][address] = 1
                else:
                    self.transactions[contract_method][address] += 1
        return process_method_in_transaction