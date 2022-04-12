from datetime import datetime
import os
import logging
from pathlib import Path
import simplejson as json
import io
from eth_utils import keccak, from_wei
from subscrape.decode.decode_evm_transaction import decode_tx


class MoonbeamScraper:
    def __init__(self, db_path, moonscan_api, blockscout_api):
        self.logger = logging.getLogger("MoonbeamScraper")
        self.db_path = db_path
        self.moonscan_api = moonscan_api
        self.blockscout_api = blockscout_api
        self.transactions = {}
        self.abis = {}      # cache of contract ABI interface definitions
        self.tokens = {}    # cache of token contract basic info

    def scrape(self, operations, chain_config):
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
                    contract_config = transactions_config.create_inner_config(methods)
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
                        self.fetch_transactions(contract, processor, contract_method)

            elif operation == "account_transactions":
                account_transactions_payload = operations[operation]
                account_transactions_config = chain_config.create_inner_config(account_transactions_payload)
                if account_transactions_config.skip:
                    self.logger.info(f"Config asks to skip account_transactions.")
                    continue

                if "accounts" in account_transactions_payload:
                    accounts = account_transactions_payload['accounts']
                    for account in accounts:
                        # ignore metadata
                        if account.startswith("_"):
                            continue

                        # deduce config
                        if type(accounts) is dict:
                            account_config = account_transactions_config.create_inner_config(methods[method])
                        else:
                            account_config = account_transactions_config

                        if account_config.skip:
                            self.logger.info(f"Config asks to skip account {account}")
                            continue

                        self.transactions[account] = {}
                        processor = self.process_transactions_on_account_factory(account)
                        self.fetch_transactions(account, processor)
                else:
                    self.logger.error(f"'accounts' not listed in config for operation '{operation}'.")
            else:
                self.logger.error(f"config contained an operation that does not exist: {operation}")            
                exit

    def fetch_transactions(self, address, processor, reference=None):
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

        self.logger.info(f"Fetching transactions for {reference} from {self.moonscan_api.endpoint}")
        self.moonscan_api.fetch_and_process_transactions(address, processor)

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
                       'value': from_wei(int(transaction['value']), 'ether'), 'gas': transaction['gas'],
                       'gasPrice': transaction['gasPrice'], 'gasUsed': transaction['gasUsed']}

            if 'input' in transaction and len(transaction['input']) >= 8:
                # assume this was a call to a contract since input data was provided
                contract_address = transaction['to']

                # retrieve and cache the abi for the contract
                if contract_address not in self.abis and contract_address != account:
                    self.abis[contract_address] = self.moonscan_api.get_contract_abi(contract_address)
                    # if self.abis[contract_address] is not None:
                    #     self.logger.info(f'Contract abi found for {contract_address}.')

                if contract_address in self.abis and self.abis[contract_address] is not None:
                    decoded_transaction = decode_tx(contract_address, transaction['input'], self.abis[contract_address])

                    if decoded_transaction[0] == 'decode error':
                        known_contracts_with_decode_errors = {
                            "0xbcc8a3022f69a5cdddc22c068049cd07581b1aa5",  # 0xTaylor "puzzle"
                            "0xf48ea3bc302f6c0585eceddba70a1bc12d67e76f",  # DPSDocks v1.0
                            "0x421bff16bba3bca1720638482c647eb832fd9de4",  # DPSDocks v1.5
                            "0x77da4f1d66004bebfda4d5a42931388cecaf81c5"   # DPSPlunderersGuild
                        }
                        if contract_address not in known_contracts_with_decode_errors:
                            self.logger.warning(f'Unable to decode contract interaction from transaction={transaction}\r\n'
                                                f'    abi={self.abis[contract_address]}\r\n'
                                                f'    and decoded_transaction={decoded_transaction}\r\n\r\n')
                    else:
                        # successfully decoded the input data to the contract interaction
                        contract_method_name = decoded_transaction[0]
                        decoded_tx = json.loads(decoded_transaction[1])
                        if transaction['to'] == '0xaa30ef758139ae4a7f798112902bf6d65612045f':
                            print('solarbeam function called: ', contract_method_name)
                            print('arguments: ', json.dumps(decoded_tx, indent=2))

                        # todo: add support for lots of dex swap methods
                        # todo: interpret liquidity provisioning and other events (like SwapExactTokensForETH)
                        if contract_method_name == "swapExactTokensForTokens":
                            token_path = decoded_tx['path']
                            # retrieve and cache the token info for all tokens
                            for token in token_path:
                                if token not in self.tokens:
                                    self.tokens[token] = self.blockscout_api.get_token_info(token)
                                    # if self.tokens[token] is not None:
                                    #     self.logger.info(f'Token info found for {token} = {self.tokens[token]}')

                            input_token = token_path[0]
                            input_token_info = self.tokens[input_token]
                            acct_tx['input_token_name'] = input_token_info['name']
                            acct_tx['input_symbol'] = input_token_info['symbol']
                            acct_tx['input_quantity'] = \
                                decoded_tx['amountIn'] / (10 ** int(input_token_info['decimals']))
                            output_token = token_path[len(token_path) - 1]
                            output_token_info = self.tokens[output_token]
                            acct_tx['output_token_name'] = output_token_info['name']
                            acct_tx['output_symbol'] = output_token_info['symbol']
                            acct_tx['output_quantity'] = \
                                decoded_tx['amountOutMin'] / (10 ** int(output_token_info['decimals']))

                            #  We only have an estimate so far based on the inputs.
                            # todo: find the event logs that the dex router emits, to figure out exactly how much was swapped.

            self.transactions[account][timestamp] = acct_tx
        return process_transaction_on_account

