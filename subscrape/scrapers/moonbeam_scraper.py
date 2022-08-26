__author__ = 'spazcoin@gmail.com @spazvt, Tommi Enenkel @alice_und_bob'

from datetime import datetime
import os
import logging
import simplejson as json
import io
import eth_utils
from numpy.core.defchararray import lower

from subscrape.decode.decode_evm_transaction import decode_tx
from subscrape.decode.decode_evm_log import decode_log


class MoonbeamScraper:
    """Scrape the Moonbeam or Moonriver chains for transactions/accounts of interest."""
    def __init__(self, db_path, moonscan_api, blockscout_api):
        self.logger = logging.getLogger("MoonbeamScraper")
        self.db_path = db_path
        self.moonscan_api = moonscan_api
        self.blockscout_api = blockscout_api
        self.transactions = {}
        self.abis = {}  # cache of contract ABI interface definitions
        self.contracts_with_known_decode_errors = []
        self.tokens = {}  # cache of token contract basic info

    def scrape(self, operations, chain_config):
        """According to the operations specified, parse the blockchain specified to extract useful info/transactions.

        :param operations: a dict of operations specified in the config file. Each operation specifies the type of
        analysis to perform while scraping data from the chain. Each contains a sub-dictionary specifying options for
        the operation.
            operation `transactions` is used to extract all transactions interacting with specific methods of specific
            smart contracts.
            operation 'account_transactions' is used to extract all transactions from a specific account.
        :type operations: dict
        :param chain_config: structure specifying whether to skip or filter certain sections of the operations to be performed.
        :type chain_config: object
        """
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
                            #todo: for this operation, 'method' hasn't been defined yet.
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
            self.transactions[account][timestamp] = acct_tx

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

                        if contract_method_name in {'swapExactTokensForTokens', 'swapTokensForExactTokens',
                                                    'swapExactTokensForETH', 'swapTokensForExactETH',
                                                    'swapExactTokensForTokensSupportingFeeOnTransferTokens',
                                                    'swapExactTokensForETHSupportingFeeOnTransferTokens',
                                                    'swapExactETHForTokens', 'swapETHForExactTokens'}:
                            self.decode_token_swap_transaction(account, transaction, contract_method_name,
                                                               decoded_func_params)
                        elif contract_method_name in {'addLiquidity', 'addLiquidityETH'}:
                            self.decode_add_liquidity_transaction(account, transaction, contract_method_name,
                                                                  decoded_func_params)
                        elif contract_method_name in {'removeLiquidity', 'removeLiquidityETH',
                                                      'removeLiquidityETHWithPermit'}:
                            self.decode_remove_liquidity_transaction(account, transaction, contract_method_name,
                                                                     decoded_func_params)
                        else:
                            # todo: handle (and don't ignore) 'deposit' and 'withdraw' contract methods:
                            # 'claim' and 'collect' probably remain ignored for DPS.
                            # 'approve', 'nominate', 'revoke_nomination' permanently ignore because not financially related.
                            if contract_method_name not in {'deposit', 'withdraw',
                                                            'approve', 'claim', 'collect',
                                                            'nominate', 'revoke_nomination'}:
                                self.logger.info(f'contract method {contract_method_name} not yet supported for '
                                                 f'contract {contract_address}.')

            # todo: add liquidity.
            # todo: handle staking rewards
            # todo: handle deposit/withdraw single-sided liquidity (like WMOVR pool on Solarbeam)
            # todo: handle deposits that essentially buy tokens (ROME)
            # todo: handle simple contract token transfers to other accounts
            # todo: export data in a csv format to easily read into Excel.

        return process_transaction_on_account

    def retrieve_and_cache_contract_abi(self, contract_address):
        """Retrieve and cache the abi for a contract

        :param contract_address: contract address
        :type contract_address: str
        """
        if contract_address not in self.abis:
            self.abis[contract_address] = self.moonscan_api.get_contract_abi(contract_address)
        return self.abis[contract_address]

    def decode_logs(self, transaction):
        """Decode transaction receipts/logs from a contract interaction

        :param transaction: dict containing details of the blockchain transaction
        :type transaction: dict
        :returns: list of tuples containing decoded transaction receipts/logs
        """
        tx_hash = transaction['hash']
        contract_address = transaction['to']
        receipt = self.moonscan_api.get_transaction_receipt(tx_hash)
        if type(receipt) is not dict or 'logs' not in receipt or len(receipt['logs']) == 0:
            self.logger.warning(f"For transaction {tx_hash} with contract {contract_address}, no"
                                f" logs/traces present for transaction receipt: {receipt}")
            return []
        logs = receipt['logs']
        decoded_logs = []
        for log in logs:
            contract_address = log['address']
            contract_abi = self.retrieve_and_cache_contract_abi(contract_address)

            if contract_address in self.abis and self.abis[contract_address] is not None:
                (evt_name, decoded_event_data, schema) = decode_log(log['data'], log['topics'],
                                                                    contract_abi)

                if evt_name == 'decode error':
                    if contract_address not in self.contracts_with_known_decode_errors:
                        self.contracts_with_known_decode_errors.append(contract_address)
                        self.logger.warning(f'Unable to decode event log with contract '
                                            f'{contract_address} in transaction:\r\n'
                                            f'{transaction}\r\n\r\n'
                                            f'---- Now continuing processing the rest of the'
                                            f' transactions ----\r\n')
                elif evt_name == 'no matching abi':
                    pass
                else:
                    decoded_logs.append((evt_name, decoded_event_data, schema))
        return decoded_logs

    def decode_token_swap_transaction(self, account, transaction, contract_method_name, decoded_func_params):
        """Decode transaction receipts/logs from a token swap contract interaction

        :param account: the 'owner' account that we're analyzing transactions for
        :type account: str
        :param transaction: dict containing details of the blockchain transaction
        :type transaction: dict
        :param contract_method_name: name of the contract method called during this transaction
        :type contract_method_name: str
        :param decoded_func_params: dict containing details of the parameters passed to the contract method
        :type decoded_func_params: dict
        """
        tx_hash = transaction['hash']
        contract_address = transaction['to']
        timestamp = transaction['timeStamp']
        token_path = decoded_func_params['path']
        self.transactions[account][timestamp]['action'] = 'token swap'
        # retrieve and cache the token info for all tokens
        for token in token_path:
            if token not in self.tokens:
                self.tokens[token] = self.blockscout_api.get_token_info(token)

        input_token = token_path[0]
        input_token_info = self.tokens[input_token]
        self.transactions[account][timestamp]['input_token_name'] = input_token_info['name']
        self.transactions[account][timestamp]['input_token_symbol'] = input_token_info['symbol']
        output_token = token_path[len(token_path) - 1]
        output_token_info = self.tokens[output_token]
        self.transactions[account][timestamp]['output_token_name'] = output_token_info['name']
        self.transactions[account][timestamp]['output_token_symbol'] = output_token_info['symbol']
        if contract_method_name in {'swapExactTokensForTokens', 'swapExactTokensForETH',
                                    'swapExactTokensForTokensSupportingFeeOnTransferTokens',
                                    'swapExactTokensForETHSupportingFeeOnTransferTokens'}:
            amount_in = decoded_func_params['amountIn']
            amount_out = decoded_func_params['amountOutMin']
        elif contract_method_name in {"swapTokensForExactTokens", "swapTokensForExactETH"}:
            amount_in = decoded_func_params['amountInMax']
            amount_out = decoded_func_params['amountOut']
        elif contract_method_name in {"swapExactETHForTokens"}:
            amount_in = int(transaction['value'])
            amount_out = decoded_func_params['amountOutMin']
        elif contract_method_name in {"swapETHForExactTokens"}:
            amount_in = int(transaction['value'])
            amount_out = decoded_func_params['amountOut']
        else:
            self.logger.error(f'contract method {contract_method_name} not recognized')
            return
        requested_input_quantity_float = amount_in / (10 ** int(input_token_info['decimals']))
        requested_output_quantity_float = amount_out / (10 ** int(output_token_info['decimals']))

        #  We only have an estimate based on the inputs so far. Use the trace logs to find
        #      the exact swap quantities
        decoded_logs = self.decode_logs(transaction)
        if not decoded_logs or len(decoded_logs) == 0:
            self.logger.warning(f"No logs/traces present for transaction {tx_hash}. Therefore defaulting to"
                                f" transaction input/output values.")
            self.transactions[account][timestamp]['input_quantity'] = float(amount_in)
            self.transactions[account][timestamp]['output_quantity'] = float(amount_out)
            return

        exact_input_quantity_int = 0
        exact_output_quantity_int = 0
        for (evt_name, decoded_event_data, schema) in decoded_logs:
            decoded_event_params = json.loads(decoded_event_data)

            if evt_name not in {'Transfer', 'Withdrawal', 'Deposit'}:
                continue

            (decoded_event_quantity_int, decoded_event_source_address, decoded_event_destination_address) = \
                self.extract_dex_event_quantity_and_addresses(transaction, evt_name, decoded_event_params)

            if evt_name == 'Transfer':
                if lower(decoded_event_source_address) == lower(transaction['from']):
                    # Transfers from source acct to one or more swap LP pair contracts in
                    # order to perform swaps
                    exact_input_quantity_int += decoded_event_quantity_int
                elif lower(decoded_event_destination_address) == lower(transaction['from']):
                    # Transfers from one or more swap LP pair contracts back to the original
                    # address (after swap has occurred)
                    exact_output_quantity_int += decoded_event_quantity_int
            elif evt_name == 'Deposit' and \
                    lower(decoded_event_source_address) == lower(transaction['from']):
                # Initial deposit tx to contract addr.
                exact_input_quantity_int += decoded_event_quantity_int
            elif evt_name == 'Withdrawal' and \
                    lower(decoded_event_source_address) == lower(transaction['to']):
                # Final withdrawal tx back to source addr. Not used on all DEXs.
                exact_output_quantity_int += decoded_event_quantity_int

            continue

        exact_amount_in_float = exact_input_quantity_int / (10 ** int(input_token_info['decimals']))
        exact_amount_out_float = exact_output_quantity_int / (10 ** int(output_token_info['decimals']))
        self.transactions[account][timestamp]['input_quantity'] = exact_amount_in_float
        self.transactions[account][timestamp]['output_quantity'] = exact_amount_out_float

        # validate that the exact amounts are somewhat similar to the contract input values
        #     (to make sure we're matching up the right values).
        input_tolerance = requested_input_quantity_float * 0.2  # 20% each side
        if (exact_amount_in_float > requested_input_quantity_float + input_tolerance) \
                or (exact_amount_in_float < requested_input_quantity_float - input_tolerance):
            self.logger.warning(f"For transaction {tx_hash} with contract "
                                f"{contract_address}, expected log decoded input quantity "
                                f"{exact_amount_in_float} to be within 20% of the tx input"
                                f"quantity {requested_input_quantity_float} but it's not.")
        output_tolerance = requested_output_quantity_float * 0.2  # 20% each side
        if (exact_amount_out_float > requested_output_quantity_float + output_tolerance) \
                or (exact_amount_out_float < requested_output_quantity_float - output_tolerance):
            self.logger.warning(f"For transaction {tx_hash} with contract "
                                f"{contract_address}, expected log decoded output quantity "
                                f"{exact_amount_out_float} to be within 20% of the tx "
                                f"output quantity {requested_output_quantity_float} but "
                                f"it's not.")

    def decode_add_liquidity_transaction(self, account, transaction, contract_method_name, decoded_func_params):
        """Decode transaction receipts/logs from a liquidity adding contract interaction

        :param account: the 'owner' account that we're analyzing transactions for
        :type account: str
        :param transaction: dict containing details of the blockchain transaction
        :type transaction: dict
        :param contract_method_name: name of the contract method called during this transaction
        :type contract_method_name: str
        :param decoded_func_params: dict containing details of the parameters passed to the contract method
        :type decoded_func_params: dict
        """
        tx_hash = transaction['hash']
        timestamp = transaction['timeStamp']
        contract_address = transaction['to']
        self.transactions[account][timestamp]['action'] = 'add liquidity'
        if contract_method_name == 'addLiquidity':
            input_token_a = decoded_func_params['tokenA']
            input_token_b = decoded_func_params['tokenB']
            amount_in_a = decoded_func_params['amountADesired']
            amount_in_b = decoded_func_params['amountBDesired']
        elif contract_method_name == 'addLiquidityETH':
            input_token_a = decoded_func_params['token']
            input_token_b = '0x98878B06940aE243284CA214f92Bb71a2b032B8A'  # WMOVR
            amount_in_a = decoded_func_params['amountTokenDesired']
            amount_in_b = decoded_func_params['amountETHMin']
        else:
            self.logger.error(f'contract method {contract_method_name} not recognized')

        if input_token_a not in self.tokens:
            self.tokens[input_token_a] = self.blockscout_api.get_token_info(input_token_a)
        if input_token_b not in self.tokens:
            self.tokens[input_token_b] = self.blockscout_api.get_token_info(input_token_b)
        input_token_a_info = self.tokens[input_token_a]
        input_token_b_info = self.tokens[input_token_b]
        self.transactions[account][timestamp]['input_tokenA_name'] = input_token_a_info['name']
        self.transactions[account][timestamp]['input_tokenA_symbol'] = input_token_a_info['symbol']
        self.transactions[account][timestamp]['input_token2_name'] = input_token_b_info['name']
        self.transactions[account][timestamp]['input_token2_symbol'] = input_token_b_info['symbol']

        requested_input_a_quantity_float = amount_in_a / (10 ** int(input_token_a_info['decimals']))
        requested_input_b_quantity_float = amount_in_b / (10 ** int(input_token_b_info['decimals']))

        #  We only have an estimate based on the inputs so far. Use the trace logs to find
        #      the exact swap quantities
        decoded_logs = self.decode_logs(transaction)
        if not decoded_logs or len(decoded_logs) == 0:
            self.logger.warning(f"No logs/traces present for transaction {tx_hash}. Therefore defaulting to"
                                f" transaction input/output values.")
            self.transactions[account][timestamp]['input_a_quantity'] = requested_input_a_quantity_float
            self.transactions[account][timestamp]['input_b_quantity'] = requested_input_b_quantity_float
            # output quantity is unknown in the original request. Therefore, nothing to store.
            return

        exact_input_a_quantity_int = 0
        exact_input_b_quantity_int = 0
        exact_output_quantity_int = 0
        for (evt_name, decoded_event_data, schema) in decoded_logs:
            decoded_event_params = json.loads(decoded_event_data)

            (decoded_event_quantity_int, decoded_event_source_address, decoded_event_destination_address) = \
                self.extract_dex_event_quantity_and_addresses(transaction, evt_name, decoded_event_params)

            if evt_name == 'Mint':
                exact_input_a_quantity_int = decoded_event_params['amount0']
                exact_input_b_quantity_int = decoded_event_params['amount1']
            elif evt_name == 'Transfer':
                if decoded_event_source_address == account:
                    # Depositing token A into the LP token contract. Grab the LP token contract address as output.
                    output_token = decoded_event_destination_address
                    if output_token not in self.tokens:
                        self.tokens[output_token] = self.blockscout_api.get_token_info(output_token)
                    output_token_info = self.tokens[output_token]
                elif decoded_event_destination_address == account:
                    exact_output_quantity_int = decoded_event_quantity_int
                else:
                    pass    # ignore all the other Transfer events
            else:
                continue

        exact_amount_in_a_float = exact_input_a_quantity_int / (10 ** int(input_token_a_info['decimals']))
        exact_amount_in_b_float = exact_input_b_quantity_int / (10 ** int(input_token_b_info['decimals']))
        exact_amount_out_float = exact_output_quantity_int / (10 ** int(output_token_info['decimals']))
        self.transactions[account][timestamp]['input_a_quantity'] = exact_amount_in_a_float
        self.transactions[account][timestamp]['input_b_quantity'] = exact_amount_in_b_float
        self.transactions[account][timestamp]['output_quantity'] = exact_amount_out_float

        # validate that the exact amounts are somewhat similar to the contract input values
        #     (to make sure we're matching up the right values).
        input_tolerance = exact_amount_in_a_float * 0.2  # 20% each side
        if (exact_amount_in_a_float > requested_input_a_quantity_float + input_tolerance) \
                or (exact_amount_in_a_float < requested_input_a_quantity_float - input_tolerance):
            self.logger.warning(f"For transaction {tx_hash} with contract "
                                f"{contract_address}, expected log decoded input quantity "
                                f"{exact_amount_in_a_float} to be within 20% of the tx input"
                                f"quantity {requested_input_a_quantity_float} but it's not.")
        input_tolerance = exact_amount_in_b_float * 0.2  # 20% each side
        if (exact_amount_in_b_float > requested_input_b_quantity_float + input_tolerance) \
                or (exact_amount_in_b_float < requested_input_b_quantity_float - input_tolerance):
            self.logger.warning(f"For transaction {tx_hash} with contract "
                                f"{contract_address}, expected log decoded input quantity "
                                f"{exact_amount_in_b_float} to be within 20% of the tx input"
                                f"quantity {requested_input_b_quantity_float} but it's not.")
        # There was no output info in the original request. Therefore, nothing to compare to.

    def decode_remove_liquidity_transaction(self, account, transaction, contract_method_name, decoded_func_params):
        """Decode transaction receipts/logs from a liquidity removing contract interaction

        :param account: the 'owner' account that we're analyzing transactions for
        :type account: str
        :param transaction: dict containing details of the blockchain transaction
        :type transaction: dict
        :param contract_method_name: name of the contract method called during this transaction
        :type contract_method_name: str
        :param decoded_func_params: dict containing details of the parameters passed to the contract method
        :type decoded_func_params: dict
        """
        tx_hash = transaction['hash']
        timestamp = transaction['timeStamp']
        contract_address = transaction['to']
        self.transactions[account][timestamp]['action'] = 'remove liquidity'
        # 'removeLiquidity', 'removeLiquidityETH', 'removeLiquidityETHWithPermit'}
        if contract_method_name == 'removeLiquidity':
            output_token_a = decoded_func_params['tokenA']
            output_token_b = decoded_func_params['tokenB']
            input_liquidity_amount = decoded_func_params['liquidity']
            amount_out_a = decoded_func_params['amountAMin']
            amount_out_b = decoded_func_params['amountBMin']
        elif contract_method_name == 'removeLiquidityETH':
            output_token_a = decoded_func_params['token']
            output_token_b = '0x98878B06940aE243284CA214f92Bb71a2b032B8A'  # WMOVR
            amount_out_a = decoded_func_params['amountTokenMin']
            amount_out_b = decoded_func_params['amountETHMin']
        elif contract_method_name == 'removeLiquidityETHWithPermit':
            # todo: debug to see what this method is.
            pass
        else:
            self.logger.error(f'contract method {contract_method_name} not recognized')

        if output_token_a not in self.tokens:
            self.tokens[output_token_a] = self.blockscout_api.get_token_info(output_token_a)
        if output_token_b not in self.tokens:
            self.tokens[output_token_b] = self.blockscout_api.get_token_info(output_token_b)
        output_token_a_info = self.tokens[output_token_a]
        output_token_b_info = self.tokens[output_token_b]
        self.transactions[account][timestamp]['output_tokenA_name'] = output_token_a_info['name']
        self.transactions[account][timestamp]['output_tokenA_symbol'] = output_token_a_info['symbol']
        self.transactions[account][timestamp]['output_token2_name'] = output_token_b_info['name']
        self.transactions[account][timestamp]['output_token2_symbol'] = output_token_b_info['symbol']

        requested_input_a_quantity_float = amount_out_a / (10 ** int(output_token_a_info['decimals']))
        requested_input_b_quantity_float = amount_out_b / (10 ** int(output_token_b_info['decimals']))

        #  We only have an estimate based on the inputs so far. Use the trace logs to find
        #      the exact swap quantities
        decoded_logs = self.decode_logs(transaction)
        if not decoded_logs or len(decoded_logs) == 0:
            self.logger.warning(f"No logs/traces present for transaction {tx_hash}. Therefore defaulting to"
                                f" transaction input/output values.")
            self.transactions[account][timestamp]['input_a_quantity'] = requested_input_a_quantity_float
            self.transactions[account][timestamp]['input_b_quantity'] = requested_input_b_quantity_float
            # output quantity is unknown in the original request. Therefore, nothing to store.
            return

        exact_input_a_quantity_int = 0
        exact_input_b_quantity_int = 0
        exact_output_quantity_int = 0
        for (evt_name, decoded_event_data, schema) in decoded_logs:
            decoded_event_params = json.loads(decoded_event_data)

            (decoded_event_quantity_int, decoded_event_source_address, decoded_event_destination_address) = \
                self.extract_dex_event_quantity_and_addresses(transaction, evt_name, decoded_event_params)

            if evt_name == 'Mint':
                exact_input_a_quantity_int = decoded_event_params['amount0']
                exact_input_b_quantity_int = decoded_event_params['amount1']
            elif evt_name == 'Transfer':
                if decoded_event_source_address == account:
                    # Depositing token A into the LP token contract. Grab the LP token contract address as output.
                    output_token = decoded_event_destination_address
                    if output_token not in self.tokens:
                        self.tokens[output_token] = self.blockscout_api.get_token_info(output_token)
                    output_token_info = self.tokens[output_token]
                elif decoded_event_destination_address == account:
                    exact_output_quantity_int = decoded_event_quantity_int
                else:
                    pass    # ignore all the other Transfer events
            else:
                continue

        exact_amount_in_a_float = exact_input_a_quantity_int / (10 ** int(output_token_a_info['decimals']))
        exact_amount_in_b_float = exact_input_b_quantity_int / (10 ** int(output_token_b_info['decimals']))
        exact_amount_out_float = exact_output_quantity_int / (10 ** int(output_token_info['decimals']))
        self.transactions[account][timestamp]['input_a_quantity'] = exact_amount_in_a_float
        self.transactions[account][timestamp]['input_b_quantity'] = exact_amount_in_b_float
        self.transactions[account][timestamp]['output_quantity'] = exact_amount_out_float

        # validate that the exact amounts are somewhat similar to the contract input values
        #     (to make sure we're matching up the right values).
        input_tolerance = exact_amount_in_a_float * 0.2  # 20% each side
        if (exact_amount_in_a_float > requested_input_a_quantity_float + input_tolerance) \
                or (exact_amount_in_a_float < requested_input_a_quantity_float - input_tolerance):
            self.logger.warning(f"For transaction {tx_hash} with contract "
                                f"{contract_address}, expected log decoded input quantity "
                                f"{exact_amount_in_a_float} to be within 20% of the tx input"
                                f"quantity {requested_input_a_quantity_float} but it's not.")
        input_tolerance = exact_amount_in_b_float * 0.2  # 20% each side
        if (exact_amount_in_b_float > requested_input_b_quantity_float + input_tolerance) \
                or (exact_amount_in_b_float < requested_input_b_quantity_float - input_tolerance):
            self.logger.warning(f"For transaction {tx_hash} with contract "
                                f"{contract_address}, expected log decoded input quantity "
                                f"{exact_amount_in_b_float} to be within 20% of the tx input"
                                f"quantity {requested_input_b_quantity_float} but it's not.")
        # There was no output info in the original request. Therefore, nothing to compare to.









    def extract_dex_event_quantity_and_addresses(self, transaction, evt_name, decoded_event_params):
        """Different DEXs might name their event parameters differently, so we have to be flexible in what dictionary
        keywords we use.

        :param transaction: dict containing details of the blockchain transaction
        :type transaction: dict
        :param evt_name: name of the event occurring during this decoded contract trace event
        :type evt_name: str
        :param decoded_event_params: dict containing decoded parameter details of the contract trace event
        :type decoded_event_params: dict
        :returns: (quantity, source_addr, destination_addr) tuple extracted from this specific event
        """
        event_quantity_keywords = {'value', 'input', 'amount', 'wad'}
        event_source_address_keywords = {'from', 'src'}
        event_destination_address_keywords = {'to', 'dst'}
        tx_hash = transaction['hash']
        contract_address = transaction['to']

        keyword_found = False
        decoded_event_quantity_int = 0
        for key in event_quantity_keywords:
            if key in decoded_event_params:
                decoded_event_quantity_int = decoded_event_params[key]
                keyword_found = True
                continue
        if not keyword_found:
            self.logger.warning(f"For transaction {tx_hash} with contract {contract_address},"
                                f" no event keyword found for quantity. This indicates"
                                f" subscrape doesn't handle this particular contract"
                                f" implementation yet."
                                f" decoded_event_params={decoded_event_params}")
        keyword_found = False
        decoded_event_source_address = None
        for key in event_source_address_keywords:
            if key in decoded_event_params:
                decoded_event_source_address = decoded_event_params[key]
                keyword_found = True
                continue
        if not keyword_found and evt_name == 'Deposit':
            # There's no "source" for the "Deposit" event
            decoded_event_source_address = transaction['from']
            keyword_found = True
        if not keyword_found:
            self.logger.warning(f"For transaction {tx_hash} with contract {contract_address},"
                                f" no event keyword found for source address. This indicates"
                                f" subscrape doesn't handle this particular contract"
                                f" implementation yet."
                                f" decoded_event_params={decoded_event_params}")
        decoded_event_destination_address = None
        if evt_name == 'Transfer':
            keyword_found = False
            for key in event_destination_address_keywords:
                if key in decoded_event_params:
                    decoded_event_destination_address = decoded_event_params[key]
                    keyword_found = True
                    continue
            if not keyword_found:
                self.logger.warning(f"For transaction {tx_hash} with contract"
                                    f" {contract_address}, no Transfer event keyword found for"
                                    f" destination address. This indicates subscrape doesn't"
                                    f" handle this particular contract implementation yet."
                                    f" decoded_event_params={decoded_event_params}")

        return decoded_event_quantity_int, decoded_event_source_address, decoded_event_destination_address

