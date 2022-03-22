from datetime import datetime
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

    async def scrape(self, operations, chain_config):
        for operation in operations:
            # ignore metadata
            if operation.startswith("_"):
                continue

            if operation == "transactions":
                contracts = operations[operation]
                transactions_config = chain_config.create_inner_config(contracts)
                if transactions_config.skip:
                    self.logger.info(f"Config asks to skip transactions.")
                    continue

                for contract in contracts:
                    # ignore metadata
                    if operation.startswith("_"):
                        continue

                    methods = contracts[contract]
                    contract_config = transactions_config.create_innerconfig(methods)
                    if contract_config.skip:
                        self.logger.info(f"Config asks to skip transactions of contract {contract}.")
                        continue


                    for method in methods:
                        # ignore metadata
                        if operation.startswith("_"):
                            continue

                        # deduce config
                        if type(methods) is dict:
                            method_config = contract_config.create_inner_config(methods[method])
                        else:
                            method_config = contract_config

                        # config wants us to skip this call?
                        if method_config.skip:
                            self.logger.info(f"Config asks to skip contract {contract} method {method}")
                            continue

                        contract_method = f"{contract}_{method}"
                        assert(contract_method not in self.transactions)
                        self.transactions[contract_method] = {}
                        processor = self.process_methods_in_transaction_factory(contract_method, method)
                        await self.fetch_transactions(contract, processor, contract_method)
            elif operation == "account_transactions":
                account_transactions_payload = operations[operation]
                account_transactions_config = chain_config.create_inner_config(contracts)
                if "accounts" in account_transactions_payload:
                    accounts = account_transactions_payload['accounts']
                    for account in accounts:
                        # ignore metadata
                        if account.startswith("_"):
                            continue

                        # deduce config
                        if type(accounts) is dict:
                            account_config = contract_config.create_inner_config(methods[method])
                        else:
                            account_config = contract_config

                        if account_config.skip:
                            self.logger.info(f"Config asks to skip account {account}")
                            continue

                        self.transactions[account] = {}
                        processor = self.process_transactions_on_account_factory(account)
                        await self.fetch_transactions(account, processor)
                else:
                    self.logger.error(f"'accounts' not listed in config for operation '{operation}'.")
            else:
                self.logger.error(f"config contained an operation that does not exist: {operation}")            
                exit

    async def fetch_transactions(self, address, processor, reference=None):
        """Fetch all transactions for a given address (account/contract) and use the given processor method to filter
        or post-process each transaction as we work through them. Optionally, use 'reference' to uniquely identify this
        set of post-processed transaction data.

        :param address: the moonriver/moonbeam account number of interest. This could be a basic account, or a contract
        address, depending on the kind of transactions being analyzed.
        :type address: str
        :param processor: a method that is used to post-process every transaction for the given address as it is
        retrieved from the API. Processing transactions as they come in, instead of storing all transaction data helps
        cut down on required storage.
        :type processor: function
        :param reference: (optional) Unique identifier for this set of post-processed transaction data being created,
        if necessary.
        :type reference: str
        """
        if reference is None:
            reference = address
        else:
            reference = reference.replace(" ", "_")
        file_path = self.db_path + f"{reference}.json"
        if os.path.exists(file_path):
            self.logger.warning(f"{file_path} already exists. Skipping.")
            return

        self.logger.info(f"Fetching transactions for {reference} from {self.api.endpoint}")

        params = {}
        params["module"] = "account"
        params["action"] = "txlist"
        params["address"] = address
        params["startblock"] = "1"
        params["endblock"] = "99999999"
        params["sort"] = "asc"

        await self.api.iterate_pages(processor, params=params)

        payload = json.dumps(self.transactions[reference], indent=4, sort_keys=False)
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

    def process_methods_in_transaction_factory(self, contract_method, method):
        def process_method_in_transaction(transaction):
            """Process each transaction from a specific method of a contract, counting the number of transactions for
            each account.

            :param transaction: all details for a specific transaction on the specified contract.
            :type transaction: dict
            """
            if transaction["input"][0:10] == method:
                address = transaction["from"]
                if address not in self.transactions[contract_method]:
                    self.transactions[contract_method][address] = 1
                else:
                    self.transactions[contract_method][address] += 1
        return process_method_in_transaction

    def process_transactions_on_account_factory(self, account):
        def process_transaction_on_account(transaction):
            """Process each transaction for an account, capturing the necessary info.

            :param transaction: all details for a specific transaction on the specified account.
            :type transaction: dict
            """
            timestamp = transaction['timeStamp']
            acct_tx = {'utcdatetime': str(datetime.utcfromtimestamp(int(timestamp))), 'hash': transaction['hash'],
                       'from': transaction['from'], 'to': transaction['to'], 'valueInWei': transaction['value'],
                       'value': float(transaction['value'])/1000000000000000000, 'gas': transaction['gas'],
                       'gasPrice': transaction['gasPrice'], 'gasUsed': transaction['gasUsed']}
            self.transactions[account][timestamp] = acct_tx
            # todo: interpret dex token swaps
            # todo: interpret liquidity provisioning
        return process_transaction_on_account

