__author__ = 'spazcoin@gmail.com @spazvt'
__author__ = 'Tommi Enenkel @alice_und_bob'

from datetime import datetime
import os
import logging
from pathlib import Path
import simplejson as json
import io
import eth_utils
from subscrape.decode.decode_evm_transaction import decode_tx
from subscrape.decode.decode_evm_log import decode_log


class MoonbeamScraper:
    def __init__(self, db_path, moonscan_api, blockscout_api):
        self.logger = logging.getLogger("MoonbeamScraper")
        self.db_path = db_path
        self.moonscan_api = moonscan_api
        self.blockscout_api = blockscout_api
        self.transactions = {}
        self.abis = {}      # cache of contract ABI interface definitions
        self.contracts_with_known_decode_errors = []
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
                       'value': eth_utils.from_wei(int(transaction['value']), 'ether'), 'gas': transaction['gas'],
                       'gasPrice': transaction['gasPrice'], 'gasUsed': transaction['gasUsed']}

            if 'input' in transaction and len(transaction['input']) >= 8:
                # assume this was a call to a contract since input data was provided
                contract_address = transaction['to']

                self.retrieve_and_cache_contract_abi(contract_address)

                if contract_address in self.abis and self.abis[contract_address] is not None:
                    decoded_transaction = decode_tx(contract_address, transaction['input'], self.abis[contract_address])

                    if decoded_transaction[0] == 'decode error':
                        if contract_address not in self.contracts_with_known_decode_errors:
                            self.contracts_with_known_decode_errors.append(contract_address)
                            decode_traceback = decoded_transaction[1]
                            self.logger.warning(f'Unable to decode contract interaction with contract '
                                                f'{contract_address} in transaction:\r\n'
                                                f'{transaction}\r\n\r\n'
                                                f'{decode_traceback}\r\n'
                                                f'---- Now continuing processing the rest of the transactions ----\r\n')
                    else:
                        # successfully decoded the input data to the contract interaction
                        contract_method_name = decoded_transaction[0]
                        decoded_func_params = json.loads(decoded_transaction[1])
                        # if transaction['to'] == '0xaa30ef758139ae4a7f798112902bf6d65612045f':
                        #     print('solarbeam function called: ', contract_method_name)
                        #     print('arguments: ', json.dumps(decoded_func_params, indent=2))

                        # todo: add support for "swapETHForTokens" methods (which don't specify an input quantity?)
                        # todo: interpret liquidity provisioning and other events (like SwapExactTokensForETH)
                        if contract_method_name in {"swapExactTokensForTokens", "swapTokensForExactTokens",
                                                    "swapExactTokensForETH",    "swapTokensForExactETH",
                                                    "swapExactTokensForTokensSupportingFeeOnTransferTokens",
                                                    "swapExactTokensForETHSupportingFeeOnTransferTokens"}:
                            token_path = decoded_func_params['path']
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
                            output_token = token_path[len(token_path) - 1]
                            output_token_info = self.tokens[output_token]
                            acct_tx['output_token_name'] = output_token_info['name']
                            acct_tx['output_symbol'] = output_token_info['symbol']
                            if contract_method_name in {"swapExactTokensForTokens", "swapExactTokensForETH",
                                                        "swapExactTokensForTokensSupportingFeeOnTransferTokens",
                                                        "swapExactTokensForETHSupportingFeeOnTransferTokens"}:
                                amount_in = decoded_func_params['amountIn']
                                amount_out = decoded_func_params['amountOutMin']
                            elif contract_method_name in {"swapTokensForExactTokens", "swapTokensForExactETH"}:
                                amount_in = decoded_func_params['amountInMax']
                                amount_out = decoded_func_params['amountOut']
                            else:
                                self.logger.error(f'contract method {contract_method_name} not recognized')
                            requested_input_quantity_float = amount_in / (10 ** int(input_token_info['decimals']))
                            requested_output_quantity_float = amount_out / (10 ** int(output_token_info['decimals']))

                            #  We only have an estimate so far based on the inputs so far. Use the trace logs to find
                            #      the exact swap quantities
                            tx_hash = transaction['hash']
                            receipt = self.moonscan_api.get_transaction_receipt(tx_hash)
                            logs = receipt["logs"]
                            for log in logs:
                                contract_address = log["address"]
                                contract_abi = self.retrieve_and_cache_contract_abi(contract_address)
                                (evt_name, decoded_data, schema) = decode_log(log['data'], log['topics'], contract_abi)
                                if evt_name == 'Swap':
                                    decoded_swap_params = json.loads(decoded_data)
                                    # Assume that there's only one input quantity and output quantity.
                                    #     The other input/output must be zero.
                                    exact_amount_in = decoded_swap_params['amount0In']
                                    if exact_amount_in == 0:
                                        exact_amount_in = decoded_swap_params['amount1In']
                                    elif decoded_swap_params['amount1In'] != 0:
                                        self.logger.warning(f"Expected one of the swap input amounts to be zero for "
                                                            f"transaction {tx_hash} with contract {contract_address} "
                                                            f"but amount0In={decoded_swap_params['amount0In']} and "
                                                            f"amount1In={decoded_swap_params['amount1In']}")
                                    exact_amount_out = decoded_swap_params['amount0Out']
                                    if exact_amount_out == 0:
                                        exact_amount_out = decoded_swap_params['amount1Out']
                                    elif decoded_swap_params['amount1Out'] != 0:
                                        self.logger.warning(f"Expected one of the swap output amounts to be zero for "
                                                            f"transaction {tx_hash} with contract {contract_address} "
                                                            f"but amount0Out={decoded_swap_params['amount0Out']} and "
                                                            f"amount1Out={decoded_swap_params['amount1Out']}")
                                    exact_amount_in_float = exact_amount_in / (10 ** int(input_token_info['decimals']))
                                    exact_amount_out_float = exact_amount_out \
                                                             / (10 ** int(output_token_info['decimals']))

                                    # validate that the exact amounts are somewhat similar to the contract input values
                                    #     (to make sure we're matching up the right values).
                                    input_tolerance = requested_input_quantity_float * 0.5  # 50% each side
                                    acct_tx['input_quantity'] = exact_amount_in_float
                                    if (exact_amount_in_float > requested_input_quantity_float + input_tolerance)\
                                            and (exact_amount_in_float < requested_input_quantity_float
                                                                          - input_tolerance):
                                        self.logger.warning(f"For transaction {tx_hash} with contract "
                                                            f"{contract_address}, expected log decoded input quantity "
                                                            f"{exact_amount_in_float} to be within 50% of the tx input"
                                                            f"quantity {requested_input_quantity_float} but it's not.")
                                    output_tolerance = requested_output_quantity_float * 0.5    # 50% each side
                                    acct_tx['output_quantity'] = exact_amount_out_float
                                    if (exact_amount_out_float > requested_output_quantity_float + output_tolerance)\
                                            and (exact_amount_out_float < requested_output_quantity_float
                                                                          - output_tolerance):
                                        self.logger.warning(f"For transaction {tx_hash} with contract "
                                                            f"{contract_address}, expected log decoded output quantity "
                                                            f"{exact_amount_out_float} to be within 50% of the tx "
                                                            f"output quantity {requested_output_quantity_float} but "
                                                            f"it's not.")

                                # elif evt_name == 'Withdrawal':
                                #     decoded_withdrawal_params = json.loads(decoded_data)

            self.transactions[account][timestamp] = acct_tx
        return process_transaction_on_account

    def retrieve_and_cache_contract_abi(self, contract_address):
        """Retrieve and cache the abi for a contract

        :param contract_address: contract address
        :type contract_address: str
        """
        if contract_address not in self.abis:
            self.abis[contract_address] = self.moonscan_api.get_contract_abi(contract_address)
            # if self.abis[contract_address] is not None:
            #     self.logger.info(f'Contract abi found for {contract_address}.')
        return self.abis[contract_address]
