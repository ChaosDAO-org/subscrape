__author__ = 'spazcoin@gmail.com @spazvt, Tommi Enenkel @alice_und_bob'

from datetime import datetime
from pathlib import Path

import eth_utils
import logging
from numpy.core.defchararray import lower
import pandas
import simplejson as json

from subscrape.decode.decode_evm_transaction import decode_tx
from subscrape.decode.decode_evm_log import decode_log


class MoonbeamScraper:
    """Scrape the Moonbeam or Moonriver chains for transactions/accounts of interest."""

    def __init__(self, db_path, moonscan_api, blockscout_api, chain_name):
        self.logger = logging.getLogger(__name__)
        self.db_path = db_path
        self.chain_name = chain_name
        self.moonscan_api = moonscan_api
        self.blockscout_api = blockscout_api
        self.transactions = {}
        self.abis = {}  # cache of contract ABI interface definitions
        self.contracts_with_known_decode_errors = []
        self.tokens = {}  # cache of token contract basic info
        self.contracts_that_arent_tokens = []  # cache of addresses not recognized as tokens

    async def scrape(self, operations, chain_config) -> list:
        """According to the operations specified, parse the blockchain specified to extract useful info/transactions.

        :param operations: a dict of operations specified in the config file. Each operation specifies the type of
        analysis to perform while scraping data from the chain. Each contains a sub-dictionary specifying options for
        the operation.
            operation `transactions` is used to extract all transactions interacting with specific methods of specific
            smart contracts.
            operation 'account_transactions' is used to extract all transactions from a specific account.
        :type operations: dict
        :param chain_config: structure specifying whether to skip or filter certain sections of the operations to be
        performed.
        :type chain_config: object
        """
        items_scraped = []
        for operation in operations:
            # ignore metadata
            if operation.startswith("_"):
                continue

            if operation == "transactions":
                contracts = operations[operation]
                transactions_config = chain_config.create_inner_config(contracts)
                if transactions_config.skip:
                    self.logger.info("Config asks to skip transactions.")
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
                        assert (contract_method not in self.transactions)
                        self.transactions[contract_method] = {}
                        processor = self._process_methods_in_transaction_factory(contract_method, method)
                        self.logger.info(f"Fetching transactions for {contract_method} from"
                                         f" {self.moonscan_api.endpoint}")
                        self.moonscan_api.fetch_and_process_transactions(contract, processor)
                        self._export_transactions(contract, contract_method)

            elif operation == "account_transactions":
                account_transactions_payload = operations[operation]
                account_transactions_config = chain_config.create_inner_config(account_transactions_payload)
                if account_transactions_config.skip:
                    self.logger.info("Config asks to skip account_transactions.")
                    continue

                if "accounts" in account_transactions_payload:
                    accounts = account_transactions_payload['accounts']
                    for account in accounts:
                        # ignore metadata
                        if account.startswith("_"):
                            continue

                        # deduce config
                        if type(accounts) is dict:
                            # todo: for this operation, 'method' hasn't been defined yet.
                            account_config = account_transactions_config.create_inner_config(methods[method])
                        else:
                            account_config = account_transactions_config

                        if account_config.skip:
                            self.logger.info(f"Config asks to skip account {account}")
                            continue

                        self.transactions[account] = {}
                        processor = self._process_transactions_on_account_factory(account)
                        self.logger.info(f"Fetching transactions for {account} from {self.moonscan_api.endpoint}")
                        self.moonscan_api.fetch_and_process_transactions(account, processor)
                        self._export_transactions(account)
                else:
                    self.logger.error(f"'accounts' not listed in config for operation '{operation}'.")
            else:
                self.logger.error(f"config contained an operation that does not exist: {operation}")
                exit
            items_scraped.extend(len(self.transactions[account]))
        return items_scraped

    def _export_transactions(self, address, reference=None):
        """Fetch all transactions for a given address (account/contract) and use the given processor method to filter
        or post-process each transaction as we work through them. Optionally, use 'reference' to uniquely identify this
        set of post-processed transaction data.

        :param address: the moonriver/moonbeam account number of interest. This could be a basic account, or a contract
        address, depending on the kind of transactions being analyzed.
        :type address: str
        :param reference: (optional) Unique identifier for this set of post-processed transaction data being created,
        if necessary.
        :type reference: str
        """
        if reference is None:
            reference = address
        else:
            reference = reference.replace(" ", "_")

        # Export the transactions to a JSON file
        json_file_path = Path(self.db_path.parent) / f'{self.db_path.stem}{reference}.json'
        if json_file_path.exists():     # delete and recreate the file
            json_file_path.unlink()
        with json_file_path.open('w', encoding="UTF-8") as output_file:
            json.dump(self.transactions[reference], output_file, indent=4, sort_keys=False)

        # Export the transactions to an XLSX file
        xlsx_file_path = Path(self.db_path.parent) / f'{self.db_path.stem}{reference}.xlsx'
        if xlsx_file_path.exists():     # delete and recreate the file
            xlsx_file_path.unlink()
        data_frame = pandas.read_json(json_file_path).transpose()
        (num_rows, num_columns) = data_frame.shape
        writer = pandas.ExcelWriter(xlsx_file_path, engine='xlsxwriter')
        tx_sheet_name = 'Transactions'
        data_frame.to_excel(writer, sheet_name=tx_sheet_name, index=False, freeze_panes=(1, 1))
        worksheet = writer.sheets[tx_sheet_name]
        worksheet.autofilter(0, 0, num_rows, num_columns - 1)

        # Auto-adjust columns' width
        for column in data_frame:
            column_width = max(data_frame[column].astype(str).map(len).max(), len(column))
            col_idx = data_frame.columns.get_loc(column)
            worksheet.set_column(col_idx, col_idx, column_width)

        # hide column 'valueInWei' because we prefer the 'value' float value.
        worksheet.set_column('E:E', None, None, {'hidden': True})
        # hide columns for gas, since tx gas isn't a concern on Moonbeam/Moonriver
        worksheet.set_column('G:I', None, None, {'hidden': True})
        # Shorten the 'hash', 'from', and 'to' address columns
        worksheet.set_column('B:D', width=20)

        writer.close()

        self.logger.info(f'All transactions exported in JSON to {json_file_path}.\n'
                         f'    and in XLSX format to {xlsx_file_path}')

    def _process_methods_in_transaction_factory(self, contract_method, method):
        def _process_method_in_transaction(transaction):
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

        return _process_method_in_transaction

    def _process_transactions_on_account_factory(self, account):
        def _process_transaction_on_account(transaction):
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

            if len(self.transactions[account]) == 1:
                # This is the first entry/row. Initialize the rest of the custom/optional fields in the order that they
                # should be visualized when exported to a spreadsheet. (Don't init every entry to keep JSON cleaner.)
                self.transactions[account][timestamp]['contract_method_name'] = ''
                self.transactions[account][timestamp]['action'] = ''
                self.transactions[account][timestamp]['input_a_token_name'] = ''
                self.transactions[account][timestamp]['input_a_token_symbol'] = ''
                self.transactions[account][timestamp]['input_a_quantity'] = ''
                self.transactions[account][timestamp]['input_b_token_name'] = ''
                self.transactions[account][timestamp]['input_b_token_symbol'] = ''
                self.transactions[account][timestamp]['input_b_quantity'] = ''
                self.transactions[account][timestamp]['output_a_token_name'] = ''
                self.transactions[account][timestamp]['output_a_token_symbol'] = ''
                self.transactions[account][timestamp]['output_a_quantity'] = ''
                self.transactions[account][timestamp]['output_b_token_name'] = ''
                self.transactions[account][timestamp]['output_b_token_symbol'] = ''
                self.transactions[account][timestamp]['output_b_quantity'] = ''

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
                        self.transactions[account][timestamp]['contract_method_name'] = contract_method_name
                        decoded_func_params = json.loads(decoded_transaction[1])

                        if contract_method_name in {'swapExactTokensForTokens', 'swapTokensForExactTokens',
                                                    'swapExactTokensForETH', 'swapTokensForExactETH',
                                                    'swapExactTokensForTokensSupportingFeeOnTransferTokens',
                                                    'swapExactTokensForETHSupportingFeeOnTransferTokens',
                                                    'swapExactETHForTokens', 'swapETHForExactTokens',
                                                    'addLiquiditySingleNativeCurrency'}:
                            self._decode_token_swap_transaction(account, transaction, contract_method_name,
                                                                decoded_func_params)
                        elif contract_method_name in {'addLiquidity', 'addLiquidityETH'}:
                            self._decode_add_liquidity_transaction(account, transaction, contract_method_name,
                                                                   decoded_func_params)
                        elif contract_method_name in {'removeLiquidity', 'removeLiquidityETH',
                                                      'removeLiquidityETHWithPermit'}:
                            self._decode_remove_liquidity_transaction(account, transaction, contract_method_name,
                                                                      decoded_func_params)
                        elif contract_method_name in {'deposit', 'depositWithPermit', 'depositEth', 'depositETH'}:
                            self._decode_deposit_transaction(account, transaction, contract_method_name,
                                                             decoded_func_params)
                        elif contract_method_name in {'withdraw', 'leave'}:
                            self._decode_withdraw_transaction(account, transaction, contract_method_name,
                                                              decoded_func_params)
                        elif contract_method_name in {'redeem'}:
                            self._decode_redeem_transaction(account, transaction, contract_method_name,
                                                            decoded_func_params)
                        else:
                            # todo: handle (and don't ignore) 'stake' contract methods
                            # 'claim' and 'collect' probably remain ignored for DPS.
                            # 'approve', 'nominate', 'revoke_nomination' permanently ignore because not financially related.
                            self.transactions[account][timestamp]['action'] = contract_method_name
                            if contract_method_name not in {'enter', 'leave',
                                                            'increaseAmountWithPermit',
                                                            'approve', 'claim', 'collect',
                                                            'stake', 'unstake', 'delegate',
                                                            'nominator_bond_more', 'nominator_bond_less',
                                                            'execute_delegation_request', 'cancel_delegation_request',
                                                            'schedule_delegator_bond_less',
                                                            'schedule_revoke_delegation',
                                                            'delegator_bond_more', 'delegator_bond_less',
                                                            'nominate', 'revoke_nomination', 'createProxyWithNonce',
                                                            'exchangeOldForCanonical', 'createProfile',
                                                            'deactivateProfile', 'transfer',
                                                            'claimFlagship', 'buyVoyage', 'repairFlagships',
                                                            'lockVoyageItems', 'claimRewards', 'setApprovalForAll',
                                                            'lockToClaimRewards', 'claimLockedRewards', 'increaseLock',
                                                            'standard_vote', 'flipCoin', 'listToken', 'delistToken',
                                                            'transferFrom', 'safeTransferFrom', 'createWithPermit'}:
                                self.logger.info(f'contract method {contract_method_name} not yet supported for '
                                                 f'contract {contract_address}.')

            # todo: handle staking rewards
            # todo: handle deposit/withdraw single-sided liquidity (like WMOVR pool on Solarbeam)
            # todo: handle deposits that essentially buy tokens (ROME)
            # todo: handle simple contract token transfers to other accounts
            # todo: export data in a csv format to easily read into Excel.

        return _process_transaction_on_account

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
        # receipt = self.blockscout_api.get_transaction_receipt(tx_hash)    # todo: test blockscout receipts
        if type(receipt) is not dict or 'logs' not in receipt or len(receipt['logs']) == 0:
            self.logger.warning(f"For transaction {tx_hash} with contract {contract_address}, no"
                                f" logs/traces present for transaction receipt: {receipt}")
            return []
        logs = receipt['logs']
        decoded_logs = []
        for log in logs:
            token_address = log['address']
            contract_abi = self.retrieve_and_cache_contract_abi(token_address)

            if token_address in self.abis and self.abis[token_address] is not None:
                (evt_name, decoded_event_data, schema) = decode_log(log['data'], log['topics'], contract_abi)

                if evt_name == 'decode error':
                    if token_address not in self.contracts_with_known_decode_errors:
                        self.contracts_with_known_decode_errors.append(token_address)
                        self.logger.warning(f'Unable to decode event log with contract '
                                            f'{contract_address} (token_addr {token_address}) in transaction:\r\n'
                                            f'{transaction}\r\n\r\n'
                                            f'---- Now continuing processing the rest of the'
                                            f' transactions ----\r\n')
                elif evt_name == 'no matching abi':
                    pass
                else:
                    decoded_logs.append((evt_name, decoded_event_data, schema, token_address))
        return decoded_logs

    def _decode_token_swap_transaction(self, account, transaction, contract_method_name, decoded_func_params):
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
            self._retrieve_and_cache_token_info_from_contract_address(token)

        input_token = token_path[0]
        input_token_info = self.tokens[input_token]
        self.transactions[account][timestamp]['input_a_token_name'] = input_token_info['name']
        self.transactions[account][timestamp]['input_a_token_symbol'] = input_token_info['symbol']
        output_token = token_path[len(token_path) - 1]
        output_token_info = self.tokens[output_token]
        self.transactions[account][timestamp]['output_a_token_name'] = output_token_info['name']
        self.transactions[account][timestamp]['output_a_token_symbol'] = output_token_info['symbol']
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
        elif contract_method_name == 'addLiquiditySingleNativeCurrency':
            amount_in = decoded_func_params['nativeCurrencySwapInMax']
            amount_out = decoded_func_params['amountSwapOut']
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
            self.transactions[account][timestamp]['input_a_quantity'] = float(amount_in)
            self.transactions[account][timestamp]['output_a_quantity'] = float(amount_out)
            return

        exact_input_quantity_int = 0
        exact_output_quantity_int = 0
        for (evt_name, decoded_event_data, schema, token_address) in decoded_logs:
            decoded_event_params = json.loads(decoded_event_data)

            if evt_name not in {'Transfer', 'Withdrawal', 'Deposit'}:
                continue

            decoded_event_quantity_int = self._extract_quantity_from_params(transaction, evt_name, decoded_event_params)
            decoded_event_source_address = self._extract_source_address_from_params(transaction, evt_name,
                                                                                    decoded_event_params)
            decoded_event_destination_address = self._extract_destination_address_from_params(transaction, evt_name,
                                                                                              decoded_event_params)

            if evt_name == 'Transfer':
                if lower(decoded_event_source_address) == lower(transaction['from']):
                    # Transfers from source acct to one or more swap LP pair contracts in
                    # order to perform swaps
                    exact_input_quantity_int += decoded_event_quantity_int
                    if lower(token_address) != lower(input_token):
                        self.logger.warning(f"For transaction {tx_hash}, contract_method {contract_method_name}"
                                            f" transfer from decoded_event_source_address"
                                            f" {decoded_event_source_address}, token_address {token_address} doesn't"
                                            f" match original input_token {input_token} from token path.")
                elif lower(decoded_event_destination_address) == lower(transaction['from']):
                    # Transfers from one or more swap LP pair contracts back to the original
                    # address (after swap has occurred)
                    exact_output_quantity_int += decoded_event_quantity_int
                    if lower(token_address) != lower(output_token):
                        self.logger.warning(f"For transaction {tx_hash}, contract_method {contract_method_name}"
                                            f" transfer to decoded_event_destination_address"
                                            f" {decoded_event_destination_address}, token_address {token_address}"
                                            f" doesn't match original output_token {output_token} from token path.")
            elif evt_name == 'Deposit' and \
                    lower(decoded_event_source_address) == lower(transaction['from']):
                # Initial deposit tx to contract addr.
                exact_input_quantity_int += decoded_event_quantity_int
            elif evt_name == 'Withdrawal' and \
                    lower(decoded_event_source_address) == lower(transaction['to']):
                # Final withdrawal tx back to source addr. Not used on all DEXs.
                exact_output_quantity_int += decoded_event_quantity_int

            continue

        # If there wasn't an explicit event for how much was transferred, use the original requested amount.
        if exact_input_quantity_int == 0:
            self.logger.warning(f"For transaction {tx_hash}, contract_method {contract_method_name}, there was no"
                                f" explicit input quantity transfer event. Therefore defaulting to requested tx amount"
                                f" from original function params.")
            exact_input_quantity_int = amount_in
        if exact_output_quantity_int == 0:
            self.logger.warning(f"For transaction {tx_hash}, contract_method {contract_method_name}, there was no"
                                f" explicit output quantity transfer event. Therefore defaulting to requested tx amount"
                                f" from original function params.")
            exact_output_quantity_int = amount_out

        exact_amount_in_float = exact_input_quantity_int / (10 ** int(input_token_info['decimals']))
        exact_amount_out_float = exact_output_quantity_int / (10 ** int(output_token_info['decimals']))
        self.transactions[account][timestamp]['input_a_quantity'] = exact_amount_in_float
        self.transactions[account][timestamp]['output_a_quantity'] = exact_amount_out_float

        # validate that the exact amounts are somewhat similar to the contract input values
        #     (to make sure we're matching up the right values).
        input_tolerance = requested_input_quantity_float * 0.2  # 20% each side
        if (exact_amount_in_float > requested_input_quantity_float + input_tolerance) \
                or (exact_amount_in_float < requested_input_quantity_float - input_tolerance):
            self.logger.warning(f"For transaction {tx_hash} with contract {contract_address} method"
                                f" {contract_method_name}, expected log decoded input quantity"
                                f" {exact_amount_in_float} to be within 20% of the tx input quantity"
                                f" {requested_input_quantity_float} but it's not.")
        output_tolerance = requested_output_quantity_float * 0.2  # 20% each side
        if (exact_amount_out_float > requested_output_quantity_float + output_tolerance) \
                or (exact_amount_out_float < requested_output_quantity_float - output_tolerance):
            self.logger.warning(f"For transaction {tx_hash} with contract {contract_address} method"
                                f" {contract_method_name}, expected log decoded output quantity"
                                f" {exact_amount_out_float} to be within 20% of the tx output quantity"
                                f" {requested_output_quantity_float} but it's not.")

    def _decode_add_liquidity_transaction(self, account, transaction, contract_method_name, decoded_func_params):
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
            if 'tokenA' in decoded_func_params:
                input_token_a = decoded_func_params['tokenA']
                input_token_b = decoded_func_params['tokenB']
                amount_in_a = decoded_func_params['amountADesired']
                amount_in_b = decoded_func_params['amountBDesired']
            elif 'amounts' in decoded_func_params:
                return  # todo: Solarbeam stable AMM pairs not yet supported
                amount_in_a = decoded_func_params['amounts'][0]
                amount_in_b = decoded_func_params['amounts'][1]
                amount_out = decoded_func_params['minToMint']
                # we don't know what the tokens are yet. But this occurs for WBTC/xcKBTC stable pair on solarbeam.io
                input_token_a = None
                input_token_b = None
            else:
                pass
        elif contract_method_name == 'addLiquidityETH':
            input_token_a = decoded_func_params['token']
            input_token_b = self._get_custom_token_info('WMOVR')['address']
            amount_in_a = decoded_func_params['amountTokenDesired']
            amount_in_b = decoded_func_params['amountETHMin']
        else:
            self.logger.error(f'contract method {contract_method_name} not recognized')

        requested_input_a_quantity_float = None
        requested_input_b_quantity_float = None
        if input_token_a is not None:
            input_token_a_info = self._retrieve_and_cache_token_info_from_contract_address(input_token_a)
            input_token_b_info = self._retrieve_and_cache_token_info_from_contract_address(input_token_b)
            self.transactions[account][timestamp]['input_a_token_name'] = input_token_a_info['name']
            self.transactions[account][timestamp]['input_a_token_symbol'] = input_token_a_info['symbol']
            self.transactions[account][timestamp]['input_b_token_name'] = input_token_b_info['name']
            self.transactions[account][timestamp]['input_b_token_symbol'] = input_token_b_info['symbol']

            requested_input_a_quantity_float = amount_in_a / (10 ** int(input_token_a_info['decimals']))
            requested_input_b_quantity_float = amount_in_b / (10 ** int(input_token_b_info['decimals']))

        #  We only have an estimate based on the inputs so far. Use the trace logs to find
        #      the exact liquidity quantities
        decoded_logs = self.decode_logs(transaction)
        if not decoded_logs or len(decoded_logs) == 0:
            self.logger.warning(f"No logs/traces present for transaction {tx_hash}. Therefore defaulting to"
                                f" transaction input/output values.")
            if requested_input_a_quantity_float is not None:
                self.transactions[account][timestamp]['input_a_quantity'] = requested_input_a_quantity_float
                self.transactions[account][timestamp]['input_b_quantity'] = requested_input_b_quantity_float
                # output quantity is unknown in the original request. Therefore, nothing to store.
            return

        exact_mint_input_a_quantity_int = 0
        exact_mint_input_b_quantity_int = 0
        exact_transfer_input_a_quantity_int = 0
        exact_transfer_input_b_quantity_int = 0
        exact_output_quantity_int = 0
        output_token = None
        for (evt_name, decoded_event_data, schema, token_address) in decoded_logs:
            decoded_event_params = json.loads(decoded_event_data)

            if evt_name == 'Mint':
                exact_mint_input_a_quantity_int = decoded_event_params['amount0']
                exact_mint_input_b_quantity_int = decoded_event_params['amount1']
                output_token = token_address  # LP token transferred as a result of minting it
                output_token_info = self._retrieve_and_cache_token_info_from_contract_address(output_token)
            elif evt_name == 'Transfer':
                decoded_event_quantity_int = self._extract_quantity_from_params(transaction, evt_name,
                                                                                decoded_event_params)
                decoded_event_source_address = self._extract_source_address_from_params(transaction, evt_name,
                                                                                        decoded_event_params)
                decoded_event_destination_address = self._extract_destination_address_from_params(transaction, evt_name,
                                                                                                  decoded_event_params)
                if decoded_event_source_address == account:
                    # Depositing token A or B into the LP token contract
                    if lower(token_address) == lower(input_token_a):
                        exact_transfer_input_a_quantity_int = decoded_event_quantity_int
                    elif lower(token_address) == lower(input_token_b):
                        exact_transfer_input_b_quantity_int = decoded_event_quantity_int
                    else:
                        self.logger.warning(f"For transaction {tx_hash}, contract_method {contract_method_name}"
                                            f" transfer from decoded_event_source_address"
                                            f" {decoded_event_source_address}, token_address {token_address}"
                                            f" doesn't match original input_token_a {input_token_a} or input_token_b"
                                            f" {input_token_b} from token path.")
                elif decoded_event_destination_address == account:
                    exact_output_quantity_int = decoded_event_quantity_int
                else:
                    pass  # ignore all the other Transfer events
            else:
                continue

        # There's at least one contract that reverses the order of the tokens when it uses them to mint the LP token.
        # Therefore compare the Transfer log event quantity to the Mint event quantity to see if they're reversed.
        # If A & B transferred are >1% different, but Mint B is within 1% of Transfer A AND Mint A is within 1% of
        # Transfer B then they must be swapped.
        # DEX fee usually 0.3%, so comparison tolerance of 1% is sufficiently wide for that.
        input_tolerance_a_int = int(exact_transfer_input_a_quantity_int * 0.01)
        input_tolerance_b_int = int(exact_transfer_input_b_quantity_int * 0.01)
        if ((exact_transfer_input_a_quantity_int > exact_transfer_input_b_quantity_int + input_tolerance_b_int)
            or (exact_transfer_input_a_quantity_int < exact_transfer_input_b_quantity_int - input_tolerance_b_int)) \
                and (exact_mint_input_b_quantity_int > exact_transfer_input_a_quantity_int - input_tolerance_a_int) \
                and (exact_mint_input_b_quantity_int < exact_transfer_input_a_quantity_int + input_tolerance_a_int) \
                and (exact_mint_input_a_quantity_int > exact_transfer_input_b_quantity_int - input_tolerance_b_int) \
                and (exact_mint_input_a_quantity_int < exact_transfer_input_b_quantity_int + input_tolerance_b_int):
            # tokens A & B were flipped during the Mint event.
            temp = exact_mint_input_a_quantity_int
            exact_mint_input_a_quantity_int = exact_mint_input_b_quantity_int
            exact_mint_input_b_quantity_int = temp

        exact_amount_in_a_float = exact_mint_input_a_quantity_int / (10 ** int(input_token_a_info['decimals']))
        exact_amount_in_b_float = exact_mint_input_b_quantity_int / (10 ** int(input_token_b_info['decimals']))
        exact_amount_out_float = exact_output_quantity_int / (10 ** int(output_token_info['decimals']))
        self.transactions[account][timestamp]['input_a_quantity'] = exact_amount_in_a_float
        self.transactions[account][timestamp]['input_b_quantity'] = exact_amount_in_b_float
        self.transactions[account][timestamp]['output_a_quantity'] = exact_amount_out_float

        if input_token_a == '0xf37626e2284742305858052615e94b380b23b3b7' \
                or input_token_a_info['name'] == 'TreasureMaps':
            # ignore tolerances for transactions of TMAPS
            return

        # validate that the exact amounts are somewhat similar to the contract input values
        #     (to make sure we're matching up the right values).
        input_tolerance = exact_amount_in_a_float * 0.2  # 20% each side
        if (exact_amount_in_a_float > requested_input_a_quantity_float + input_tolerance) \
                or (exact_amount_in_a_float < requested_input_a_quantity_float - input_tolerance):
            self.logger.warning(f"For transaction {tx_hash} with contract {contract_address} method"
                                f" '{contract_method_name}', expected log decoded LP input A quantity"
                                f" {exact_amount_in_a_float} to be within 20% of the tx input quantity"
                                f" {requested_input_a_quantity_float} but it's not.")
        input_tolerance = exact_amount_in_b_float * 0.2  # 20% each side
        if (exact_amount_in_b_float > requested_input_b_quantity_float + input_tolerance) \
                or (exact_amount_in_b_float < requested_input_b_quantity_float - input_tolerance):
            self.logger.warning(f"For transaction {tx_hash} with contract {contract_address} method"
                                f" '{contract_method_name}', expected log decoded LP input B quantity"
                                f" {exact_amount_in_b_float} to be within 20% of the tx input quantity"
                                f" {requested_input_b_quantity_float} but it's not.")
        if 'amount_out' in locals():
            output_tolerance = exact_amount_out_float * 0.2  # 20% each side
            if (exact_amount_out_float > amount_out + output_tolerance) \
                    or (exact_amount_out_float < amount_out - output_tolerance):
                self.logger.warning(f"For transaction {tx_hash} with contract {contract_address} method"
                                    f" '{contract_method_name}', expected log decoded LP output quantity"
                                    f" {exact_amount_out_float} to be within 20% of the tx output quantity"
                                    f" {amount_out} but it's not.")

        else:
            # There was no output info in the original request. Therefore, nothing to compare to.
            pass

    def _decode_remove_liquidity_transaction(self, account, transaction, contract_method_name, decoded_func_params):
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
        if contract_method_name == 'removeLiquidity':
            output_token_a = decoded_func_params['tokenA']
            output_token_b = decoded_func_params['tokenB']
            input_liquidity_amount = decoded_func_params['liquidity']
            amount_out_a = decoded_func_params['amountAMin']
            amount_out_b = decoded_func_params['amountBMin']
        elif contract_method_name == 'removeLiquidityETH':
            output_token_a = decoded_func_params['token']
            output_token_b = self._get_custom_token_info('WMOVR')['address']
            input_liquidity_amount = decoded_func_params['liquidity']
            amount_out_a = decoded_func_params['amountTokenMin']
            amount_out_b = decoded_func_params['amountETHMin']
        elif contract_method_name == 'removeLiquidityETHWithPermit':
            output_token_a = decoded_func_params['token']
            output_token_b = self._get_custom_token_info('WMOVR')['address']
            input_liquidity_amount = decoded_func_params['liquidity']
            amount_out_a = decoded_func_params['amountTokenMin']
            amount_out_b = decoded_func_params['amountETHMin']
        else:
            self.logger.error(f'contract method {contract_method_name} not recognized')

        output_token_a_info = self._retrieve_and_cache_token_info_from_contract_address(output_token_a)
        output_token_b_info = self._retrieve_and_cache_token_info_from_contract_address(output_token_b)
        self.transactions[account][timestamp]['output_a_token_name'] = output_token_a_info['name']
        self.transactions[account][timestamp]['output_a_token_symbol'] = output_token_a_info['symbol']
        self.transactions[account][timestamp]['output_b_token_name'] = output_token_b_info['name']
        self.transactions[account][timestamp]['output_b_token_symbol'] = output_token_b_info['symbol']

        requested_output_a_quantity_float = amount_out_a / (10 ** int(output_token_a_info['decimals']))
        requested_output_b_quantity_float = amount_out_b / (10 ** int(output_token_b_info['decimals']))

        #  We only have an estimate based on the inputs so far. Use the trace logs to find
        #      the exact liquidity quantities
        decoded_logs = self.decode_logs(transaction)
        if not decoded_logs or len(decoded_logs) == 0:
            self.logger.warning(f"No logs/traces present for transaction {tx_hash}. Therefore defaulting to"
                                f" transaction input/output values.")
            self.transactions[account][timestamp]['output_a_quantity'] = requested_output_a_quantity_float
            self.transactions[account][timestamp]['output_b_quantity'] = requested_output_b_quantity_float
            # input type is unknown in the original request. Therefore, nothing to store.
            return

        exact_input_quantity_int = 0
        exact_output_a_quantity_int = 0
        exact_output_b_quantity_int = 0
        input_token_info = None
        for (evt_name, decoded_event_data, schema, token_address) in decoded_logs:
            decoded_event_params = json.loads(decoded_event_data)

            if evt_name == 'Burn':
                exact_output_a_quantity_int = decoded_event_params['amount0']
                exact_output_b_quantity_int = decoded_event_params['amount1']
                input_token = token_address  # LP token transferred as a result of minting it
                input_token_info = self._retrieve_and_cache_token_info_from_contract_address(input_token)
            elif evt_name == 'Transfer':
                decoded_event_quantity_int = self._extract_quantity_from_params(transaction, evt_name,
                                                                                decoded_event_params)
                decoded_event_source_address = self._extract_source_address_from_params(transaction, evt_name,
                                                                                        decoded_event_params)
                decoded_event_destination_address = self._extract_destination_address_from_params(transaction, evt_name,
                                                                                                  decoded_event_params)
                if decoded_event_destination_address == account:
                    # Receiving outputs from breaking up liquidity
                    if lower(token_address) != lower(output_token_a) and lower(token_address) != lower(output_token_b):
                        self.logger.warning(f"For transaction {tx_hash}, contract_method {contract_method_name}"
                                            f" transfer to decoded_event_destination_address"
                                            f" {decoded_event_destination_address}, token_address {token_address}"
                                            f" doesn't match either {output_token_a} or {output_token_b}.")
                elif decoded_event_source_address == account:
                    exact_input_quantity_int = decoded_event_quantity_int
                else:
                    pass  # ignore all the other Transfer events
            else:
                continue

        requested_input_quantity_float = input_liquidity_amount / (10 ** int(input_token_info['decimals']))
        exact_amount_in_float = exact_input_quantity_int / (10 ** int(input_token_info['decimals']))
        exact_amount_out_a_float = exact_output_a_quantity_int / (10 ** int(output_token_a_info['decimals']))
        exact_amount_out_b_float = exact_output_b_quantity_int / (10 ** int(output_token_b_info['decimals']))
        self.transactions[account][timestamp]['input_a_quantity'] = exact_amount_in_float
        self.transactions[account][timestamp]['output_a_quantity'] = exact_amount_out_a_float
        self.transactions[account][timestamp]['output_b_quantity'] = exact_amount_out_b_float

        # validate that the exact amounts are somewhat similar to the contract input values
        #     (to make sure we're matching up the right values).
        input_tolerance = exact_amount_in_float * 0.2  # 20% each side
        if (exact_amount_in_float > requested_input_quantity_float + input_tolerance) \
                or (exact_amount_in_float < requested_input_quantity_float - input_tolerance):
            self.logger.warning(f"For transaction {tx_hash} with contract {contract_address} method"
                                f" '{contract_method_name}', expected log decoded LP input quantity"
                                f" {exact_amount_in_float} to be within 20% of the tx input quantity"
                                f" {requested_input_quantity_float} but it's not.")
        output_tolerance = exact_amount_out_a_float * 0.2  # 20% each side
        if (exact_amount_out_a_float > requested_output_a_quantity_float + output_tolerance) \
                or (exact_amount_out_a_float < requested_output_a_quantity_float - output_tolerance):
            self.logger.warning(f"For transaction {tx_hash} with contract {contract_address} method"
                                f" '{contract_method_name}', expected log decoded LP output A quantity"
                                f" {exact_amount_out_a_float} to be within 20% of the tx output quantity"
                                f" {requested_output_a_quantity_float} but it's not.")
        output_tolerance = exact_amount_out_b_float * 0.2  # 20% each side
        if (exact_amount_out_b_float > requested_output_b_quantity_float + output_tolerance) \
                or (exact_amount_out_b_float < requested_output_b_quantity_float - output_tolerance):
            self.logger.warning(f"For transaction {tx_hash} with contract {contract_address} method"
                                f" '{contract_method_name}', expected log decoded LP output B quantity"
                                f" {exact_amount_out_b_float} to be within 20% of the tx output quantity"
                                f" {requested_output_b_quantity_float} but it's not.")

    def _decode_deposit_transaction(self, account, transaction, contract_method_name, decoded_func_params):
        """Decode transaction receipts/logs from a deposit contract interaction
        Possible contracts using a 'deposit' method:
        * Deposit MOVR into WMOVR token contract to receive WMOVR
        * Deposit LP tokens into LP farm (solarbeam)
        * Deposit LP tokens into auto-compounder services (Moon Kafe, Beefy, Kogefarm, etc)

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
        self.transactions[account][timestamp]['action'] = 'deposit'

        decoded_logs = self.decode_logs(transaction)
        if not decoded_logs or len(decoded_logs) == 0:
            self.logger.warning(f"No logs/traces present for transaction {tx_hash}.")
            return

        token_info = None
        for (evt_name, decoded_event_data, schema, token_address) in decoded_logs:
            decoded_event_params = json.loads(decoded_event_data)

            if evt_name not in {'Deposit', 'deposit', 'Transfer'}:
                # todo reduce to not emit a log message
                if evt_name not in {'Approval', 'Sync', 'Mint', 'Swap', 'PricePerShareUpdated', 'ReservesUpdated',
                                    'UpdatePool'} and contract_address != '0xf5791d77c5975610af1be35b423189a8f5eb6923':
                    self.logger.info(f"Transaction {tx_hash} uses contract_method_name {contract_method_name} but has"
                                     f" '{evt_name}' event in addition to a Deposit")
                continue

            decoded_event_quantity_int = self._extract_quantity_from_params(transaction, evt_name, decoded_event_params)
            decoded_event_source_address = self._extract_source_address_from_params(transaction, evt_name,
                                                                                    decoded_event_params)
            decoded_event_destination_address = self._extract_destination_address_from_params(transaction, evt_name,
                                                                                              decoded_event_params)
            if decoded_event_source_address == account:
                token_info = self._retrieve_and_cache_token_info_from_contract_address(token_address)
                if token_info is not None:
                    quantity_float = int(decoded_event_quantity_int) / (10 ** int(token_info['decimals']))
                    if 'input_a_quantity' in self.transactions[account][timestamp]:
                        timestamp_key = self._add_another_entry_for_transaction(account, transaction)
                    else:
                        timestamp_key = timestamp
                    self.transactions[account][timestamp_key]['input_a_quantity'] = quantity_float
                    self.transactions[account][timestamp_key]['input_a_token_name'] = token_info['name']
                    self.transactions[account][timestamp_key]['input_a_token_symbol'] = token_info['symbol']
            elif decoded_event_destination_address == account:
                token_info = self._retrieve_and_cache_token_info_from_contract_address(token_address)
                if token_info is not None:
                    quantity_float = int(decoded_event_quantity_int) / (10 ** int(token_info['decimals']))
                    if 'output_a_quantity' in self.transactions[account][timestamp]:
                        timestamp_key = self._add_another_entry_for_transaction(account, transaction)
                    else:
                        timestamp_key = timestamp
                    self.transactions[account][timestamp_key]['output_a_quantity'] = quantity_float
                    self.transactions[account][timestamp_key]['output_a_token_name'] = token_info['name']
                    self.transactions[account][timestamp_key]['output_a_token_symbol'] = token_info['symbol']

    def _decode_withdraw_transaction(self, account, transaction, contract_method_name, decoded_func_params):
        """Decode transaction receipts/logs from a withdraw contract interaction

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
        self.transactions[account][timestamp]['action'] = 'withdraw'

        # possible_token = transaction['to']
        # possible_token_info = self._retrieve_and_cache_token_info_from_contract_address(possible_token)
        # if possible_token_info is None:
        #     # Assume native currency must be used since no other info is available
        #     possible_token_info = self._get_custom_token_info(self.chain_name)
        #
        # if contract_address == '0xf50225a84382c74cbdea10b0c176f71fc3de0c4d':
        #     # this WMOVR contract burns WMOVR to get MOVR back, but in ways completely different from others, so go
        #     # ahead and process it and exit instead of trying to conform to the behavior of other withdraw transactions.
        #     self.transactions[account][timestamp]['action'] = 'token swap'
        #     self.transactions[account][timestamp]['input_a_token_name'] = possible_token_info['name']
        #     self.transactions[account][timestamp]['input_a_token_symbol'] = possible_token_info['symbol']
        #     decoded_func_quantity_int = self._extract_quantity_from_params(transaction, contract_method_name,
        #                                                                    decoded_func_params)
        #     quantity_float = decoded_func_quantity_int / (10 ** int(possible_token_info['decimals']))
        #     self.transactions[account][timestamp]['input_a_quantity'] = quantity_float
        #
        #     output_token_info = self._get_custom_token_info('MOVR')
        #     self.transactions[account][timestamp]['output_a_token_name'] = output_token_info['name']
        #     self.transactions[account][timestamp]['output_a_token_symbol'] = output_token_info['symbol']
        #     self.transactions[account][timestamp]['output_a_quantity'] = quantity_float   # same output as input
        #     return
        # else:
        #     self.transactions[account][timestamp]['output_a_token_name'] = possible_token_info['name']
        #     self.transactions[account][timestamp]['output_a_token_symbol'] = possible_token_info['symbol']
        #     if len(decoded_func_params) == 0:
        #         quantity_float = int(transaction['value']) / (10 ** int(possible_token_info['decimals']))
        #     else:
        #         decoded_func_quantity_int = self._extract_quantity_from_params(transaction, contract_method_name,
        #                                                                        decoded_func_params)
        #         quantity_float = decoded_func_quantity_int / (10 ** int(possible_token_info['decimals']))
        #     self.transactions[account][timestamp]['output_a_quantity'] = quantity_float

        decoded_logs = self.decode_logs(transaction)
        if not decoded_logs or len(decoded_logs) == 0:
            self.logger.warning(f"No logs/traces present for transaction {tx_hash}.")
            return

        exact_input_quantity_int = 0
        exact_output_quantity_int = 0
        input_token_info = None
        output_token_info = None
        lower_contract_address = lower(contract_address)
        quantities_from_contract_addr = {}
        for (evt_name, decoded_event_data, schema, token_address) in decoded_logs:
            decoded_event_params = json.loads(decoded_event_data)

            if evt_name not in {'Transfer', 'Withdraw', 'Withdrawal', 'Deposit'}:
                # todo reduce to not emit a log message
                if evt_name not in {'Approval', 'Sync', 'Mint', 'Swap', 'PricePerShareUpdated', 'CapitalZeroed',
                                    'DelegateVotesChanged'} \
                        and contract_address != '0xf5791d77c5975610af1be35b423189a8f5eb6923':
                    self.logger.info(f"Transaction {tx_hash} uses contract_method_name {contract_method_name} but has"
                                     f" '{evt_name}' event in addition to a Withdraw")
                continue

            decoded_event_quantity_int = self._extract_quantity_from_params(transaction, evt_name, decoded_event_params)
            decoded_event_source_address = self._extract_source_address_from_params(transaction, evt_name,
                                                                                    decoded_event_params)
            decoded_event_destination_address = self._extract_destination_address_from_params(transaction, evt_name,
                                                                                              decoded_event_params)
            if decoded_event_source_address == account:
                token_info = self._retrieve_and_cache_token_info_from_contract_address(token_address)
                if token_info is not None:
                    quantity_float = int(decoded_event_quantity_int) / (10 ** int(token_info['decimals']))
                    if 'input_a_quantity' in self.transactions[account][timestamp]:
                        timestamp_key = self._add_another_entry_for_transaction(account, transaction)
                    else:
                        timestamp_key = timestamp
                    self.transactions[account][timestamp_key]['input_a_quantity'] = quantity_float
                    self.transactions[account][timestamp_key]['input_a_token_name'] = token_info['name']
                    self.transactions[account][timestamp_key]['input_a_token_symbol'] = token_info['symbol']
            elif decoded_event_destination_address == account:
                token_info = self._retrieve_and_cache_token_info_from_contract_address(token_address)
                if token_info is not None:
                    quantity_float = int(decoded_event_quantity_int) / (10 ** int(token_info['decimals']))
                    if 'output_a_quantity' in self.transactions[account][timestamp]:
                        timestamp_key = self._add_another_entry_for_transaction(account, transaction)
                    else:
                        timestamp_key = timestamp
                    self.transactions[account][timestamp_key]['output_a_quantity'] = quantity_float
                    self.transactions[account][timestamp_key]['output_a_token_name'] = token_info['name']
                    self.transactions[account][timestamp_key]['output_a_token_symbol'] = token_info['symbol']

        #     if evt_name == 'Transfer':
        #         if lower(decoded_event_destination_address) == lower(transaction['from']):
        #             # when a contract interaction triggers tokens to be sent back from multiple addresses (like DEX LP
        #             # pair contracts) that's easy enough to track. But sometimes multiple tx events come back from a
        #             # single contract address (like withdrawing LP triggering token reward payout). Therefore we track
        #             # them separately, assuming we'll know what kind of tokens from each specific contract.
        #
        #             if decoded_event_source_address not in quantities_from_contract_addr:
        #                 quantities_from_contract_addr[decoded_event_source_address] = [decoded_event_quantity_int]
        #             else:
        #                 quantities_from_contract_addr[decoded_event_source_address].append(decoded_event_quantity_int)
        #     elif evt_name == 'Withdrawal' or evt_name == 'Withdraw':
        #         if decoded_event_source_address is None:
        #             continue
        #         lower_decoded_event_source_address = lower(decoded_event_source_address)
        #         if lower_decoded_event_source_address == lower(transaction['to']):
        #             # Final withdrawal tx back to source addr. Not used on all DEXs.
        #             quantities_from_contract_addr[decoded_event_source_address] = [decoded_event_quantity_int]
        #         elif lower_decoded_event_source_address == lower(transaction['from']):
        #             quantities_from_contract_addr[decoded_event_source_address] = [decoded_event_quantity_int]
        #         else:
        #             self.logger.info(f"For transaction {tx_hash}, event `{evt_name}` addr"
        #                              f" {lower_decoded_event_source_address} didn't match to or from addr.")
        #
        #     continue
        #
        # # We've gathered the withdraw transaction events just in case there are multiple tokens received.
        # # Now process them.
        # for contract_source_addr in quantities_from_contract_addr:
        #     event_list_length = len(quantities_from_contract_addr[contract_source_addr])
        #     for idx, quantity in enumerate(quantities_from_contract_addr[contract_source_addr]):
        #         if idx > 0:
        #             timestamp_key = self._add_another_entry_for_transaction(account, transaction)
        #         else:
        #             timestamp_key = timestamp
        #
        #         if contract_source_addr == '0xf03b75831397D4695a6b9dDdEEA0E578faa30907':  # SolarbeamDistributor
        #             exact_output_quantity_int = quantity
        #             if event_list_length == 1:
        #                 output_token_info = self._get_custom_token_info('SLP')
        #             else:
        #                 # in addition to the withdrawn LP, a DEX SOLAR token claim transaction must have been
        #                 # triggered also.
        #                 if idx == 0:
        #                     # assume SOLAR is always the first claim token type if there are multiple# reward tokens
        #                     # for an LP.
        #                     output_token_info = self._get_custom_token_info('SOLAR')
        #                     self.transactions[account][timestamp_key]['action'] = 'claim'
        #                 elif idx == event_list_length - 1:
        #                     # assume the Solarbeam LP token will be the last thing returned, after claims
        #                     self.transactions[account][timestamp_key]['action'] = 'withdraw'
        #                     output_token_info = self._get_custom_token_info('SLP')
        #                 else:
        #                     # other reward tokens can be issued (MOVR, CHAOS, etc) and user needs to check which.
        #                     output_token_info = None
        #                     self.transactions[account][timestamp_key]['action'] = 'claim'
        #                     self.transactions[account][timestamp_key]['notes'] = \
        #                         'Check block explorer for reward token type'
        #         else:
        #             # Transfers of the withdrawn token back into the original address
        #             exact_output_quantity_int = quantity
        #             output_token_info = self._retrieve_and_cache_token_info_from_contract_address(contract_source_addr)
        #
        #         if output_token_info is None:
        #             if exact_output_quantity_int == 0:
        #                 return  # don't care
        #             # we know a quantity, but don't know what type of token was sent.
        #             lower_contract_address = lower(contract_address)
        #             output_token_info = self._get_custom_token_info('??')
        #
        #         if input_token_info is not None:
        #             exact_amount_in_float = exact_input_quantity_int / (10 ** int(input_token_info['decimals']))
        #             self.transactions[account][timestamp_key]['input_a_quantity'] = exact_amount_in_float
        #             self.transactions[account][timestamp_key]['input_a_token_name'] = input_token_info['name']
        #             self.transactions[account][timestamp_key]['input_a_token_symbol'] = input_token_info['symbol']
        #         exact_amount_out_float = exact_output_quantity_int / (10 ** int(output_token_info['decimals']))
        #         self.transactions[account][timestamp_key]['output_a_quantity'] = exact_amount_out_float
        #         self.transactions[account][timestamp_key]['output_a_token_name'] = output_token_info['name']
        #         self.transactions[account][timestamp_key]['output_a_token_symbol'] = output_token_info['symbol']

    def _decode_redeem_transaction(self, account, transaction, contract_method_name, decoded_func_params):
        """Decode transaction receipts/logs from a redeem contract interaction

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
        self.transactions[account][timestamp]['action'] = 'redeem'
        if contract_method_name != 'redeem':
            self.logger.info("decode_redeem_transaction: Not yet handling methods other than 'redeem'.")

        decoded_logs = self.decode_logs(transaction)
        if not decoded_logs or len(decoded_logs) == 0:
            self.logger.warning(f"No logs/traces present for transaction {tx_hash}.")
            return

        exact_quantity_int = 0
        token_info = None
        for (evt_name, decoded_event_data, schema, token_address) in decoded_logs:
            decoded_event_params = json.loads(decoded_event_data)

            if evt_name not in {'Transfer', 'Withdrawal'}:
                continue

            decoded_event_quantity_int = self._extract_quantity_from_params(transaction, evt_name, decoded_event_params)
            decoded_event_source_address = self._extract_source_address_from_params(transaction, evt_name,
                                                                                    decoded_event_params)
            decoded_event_destination_address = self._extract_destination_address_from_params(transaction, evt_name,
                                                                                              decoded_event_params)

            if evt_name == 'Transfer':
                if lower(decoded_event_destination_address) == lower(transaction['from']):
                    # Transfers of the redeem token back into the original address
                    exact_quantity_int += decoded_event_quantity_int
                    token_info = self._retrieve_and_cache_token_info_from_contract_address(decoded_event_source_address)
            elif evt_name == 'Withdrawal' and \
                    lower(decoded_event_source_address) == lower(transaction['to']):
                # Final withdrawal tx back to source addr. Not used on all DEXs.
                exact_quantity_int += decoded_event_quantity_int

            continue

        if token_info is None:
            if exact_quantity_int == 0:
                return
            # we know a quantity, but don't know what type of token was sent.
            lower_contract_address = lower(contract_address)
            if lower_contract_address == '0x065588602bd7206b15f9630fdb2e81e4ca51ad8a'\
                    or lower_contract_address == '0x54c6afb58aa21d11aeafe6b199f9663e908345e4' \
                    or lower_contract_address == '0xe2f71c68db7ecc0c9a907ad2e40e2394c5cac367':
                # these contracts redeem ROME bonds to receive sROME
                token_info = self._get_custom_token_info('sROME')
            if lower_contract_address == '0xafaff19679ab6baf75ed8098227be189ba47ba0f':
                # this contract returns ZLK
                token_info = self._get_custom_token_info('ZLK')
            else:
                token_info = self._get_custom_token_info('??')

        exact_amount_out_float = exact_quantity_int / (10 ** int(token_info['decimals']))
        self.transactions[account][timestamp]['output_a_quantity'] = exact_amount_out_float
        self.transactions[account][timestamp]['output_a_token_name'] = token_info['name']
        self.transactions[account][timestamp]['output_a_token_symbol'] = token_info['symbol']

    def _extract_quantity_from_params(self, transaction, method_name, decoded_event_params, verbose=True) -> int:
        """Different DEXs might name their event parameters differently, so we have to be flexible in what dictionary
        keywords we use.

        :param transaction: dict containing details of the blockchain transaction
        :type transaction: dict
        :param method_name: name of the event or function occurring during this decoded contract trace event or contract
        method interaction
        :type method_name: str
        :param decoded_event_params: dict containing decoded parameter details of the contract trace event or contract
        method interaction
        :type decoded_event_params: dict
        :param verbose: should a warning be logged if no quantity keyword is found?
        :type verbose: bool
        :returns: quantity extracted from this specific event
        :rtype: str
        """
        event_quantity_keywords = {'value', 'input', 'amount', '_amount', 'wad', '_share', '_shares', 'amtSharesMinted',
                                   'amtTokRedemmed'}
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
            self.logger.warning(f"For transaction {tx_hash} with contract {contract_address} and method"
                                f" '{method_name}', no param keyword found for quantity. This indicates subscrape"
                                f" doesn't handle this particular contract implementation yet."
                                f" decoded_event_params={decoded_event_params}")

        return decoded_event_quantity_int

    def _extract_source_address_from_params(self, transaction, method_name, decoded_event_params, verbose=True):
        """Different DEXs might name their event parameters differently, so we have to be flexible in what dictionary
        keywords we use.

        :param transaction: dict containing details of the blockchain transaction
        :type transaction: dict
        :param method_name: name of the event or function occurring during this decoded contract trace event or contract
        method interaction
        :type method_name: str
        :param decoded_event_params: dict containing decoded parameter details of the contract trace event or contract
        method interaction
        :type decoded_event_params: dict
        :param verbose: should a warning be logged if no source address keyword is found?
        :type verbose: bool
        :returns: source_addr extracted from this specific event
        """
        event_source_address_keywords = {'from', 'src', 'user'}
        tx_hash = transaction['hash']
        contract_address = transaction['to']

        keyword_found = False
        decoded_event_source_address = None
        for key in event_source_address_keywords:
            if key in decoded_event_params:
                decoded_event_source_address = decoded_event_params[key]
                keyword_found = True
                continue
        if not keyword_found and method_name == 'Deposit':
            # There's no "source" for the "Deposit" event
            decoded_event_source_address = transaction['from']
            keyword_found = True
        if not keyword_found:
            self.logger.warning(f"For transaction {tx_hash} with contract {contract_address} and method"
                                f" '{method_name}', no param keyword found for source address. This indicates"
                                f" subscrape doesn't handle this particular contract implementation yet."
                                f" decoded_event_params={decoded_event_params}")

        return decoded_event_source_address

    def _extract_destination_address_from_params(self, transaction, method_name, decoded_event_params, verbose=True):
        """Different DEXs might name their event parameters differently, so we have to be flexible in what dictionary
        keywords we use.

        :param transaction: dict containing details of the blockchain transaction
        :type transaction: dict
        :param method_name: name of the event or function occurring during this decoded contract trace event or contract
        method interaction
        :type method_name: str
        :param decoded_event_params: dict containing decoded parameter details of the contract trace event or contract
        method interaction
        :type decoded_event_params: dict
        :param verbose: should a warning be logged if no destination address keyword is found?
        :type verbose: bool
        :returns: destination_addr extracted from this specific event
        """
        event_destination_address_keywords = {'to', 'dst'}
        tx_hash = transaction['hash']
        contract_address = transaction['to']

        decoded_event_destination_address = None
        if method_name == 'Transfer':
            keyword_found = False
            for key in event_destination_address_keywords:
                if key in decoded_event_params:
                    decoded_event_destination_address = decoded_event_params[key]
                    keyword_found = True
                    continue
            if not keyword_found:
                self.logger.warning(f"For transaction {tx_hash} with contract {contract_address} and method"
                                    f" '{method_name}', no param keyword found for destination address. This indicates"
                                    f" subscrape doesn't handle this particular contract implementation yet."
                                    f" decoded_event_params={decoded_event_params}")

        return decoded_event_destination_address

    def _retrieve_and_cache_token_info_from_contract_address(self, contract_address):
        """Get token info (from block explorer and contract interface) from a contract address. Cache the token info
        for future use and build up a list of contract addresses that are not tokens.

        :param contract_address: contract address which might represent a token
        :type contract_address: str
        :returns: dict of token info
        """
        token_info = None
        if contract_address in self.contracts_that_arent_tokens:
            token_info = None
        elif contract_address in self.tokens:
            token_info = self.tokens[contract_address]
        else:
            possible_token_info = self.blockscout_api.get_token_info(contract_address)
            if possible_token_info is None:
                self.contracts_that_arent_tokens.append(contract_address)
            elif possible_token_info['decimals'] == '':
                self.logger.info(f"Received malformed/empty token info from Blockscout for contract"
                                 f" {contract_address}.")
                self.contracts_that_arent_tokens.append(contract_address)
                possible_token_info = None
            else:
                self.tokens[contract_address] = possible_token_info
            token_info = possible_token_info
        return token_info

    def _add_another_entry_for_transaction(self, account, transaction):
        """When a transaction returns more than one type of token, represent that with multiple entries/lines. For
        example, creating or breaking up LP liquidity tokens, or when a DEX action triggers a redeem of accumulated
        rewards.
        Use the same transaction hash but slightly later timestamp as a unique dict key. Don't duplicate gas or tx fees
        and don't duplicate the inputs or outputs to avoid double-counting.

        :param account: the 'owner' account that we're analyzing transactions for
        :type account: str
        :param transaction: dict containing details of the blockchain transaction
        :type transaction: dict
        :returns: new timestamp (for users to know where to insert data)
        """
        # for each new entry for this transaction hash, increment the timestamp by one. In case other additional
        #  entries have already been created, check for the next integer timestamp after which isn't already a valid
        #  key/entry.
        orig_timestamp = transaction['timeStamp']
        new_timestamp = orig_timestamp
        for x in range(1, 100):     # 1-3 should be sufficient, but 100 ensures an empty slot should be found.
            new_timestamp = str(int(new_timestamp) + 1)
            if new_timestamp not in self.transactions[account]:
                break
        # we've found a new empty timestamp key slot to populate with our additional line of data. copy.
        self.transactions[account][new_timestamp] = {}
        self.transactions[account][new_timestamp]['utcdatetime'] \
            = str(datetime.utcfromtimestamp(int(new_timestamp)))
        self.transactions[account][new_timestamp]['hash'] = self.transactions[account][orig_timestamp]['hash']
        self.transactions[account][new_timestamp]['from'] = self.transactions[account][orig_timestamp]['from']
        self.transactions[account][new_timestamp]['to'] = self.transactions[account][orig_timestamp]['to']
        if 'contract_method_name' in self.transactions[account][orig_timestamp]:
            self.transactions[account][new_timestamp]['contract_method_name'] \
                = self.transactions[account][orig_timestamp]['contract_method_name']
        if 'action' in self.transactions[account][orig_timestamp]:
            self.transactions[account][new_timestamp]['action'] \
                = self.transactions[account][orig_timestamp]['action']
        return new_timestamp

    def _get_custom_token_info(self, token_name):
        """Get token info (from block explorer and contract interface) from a contract address. Cache the token info
        for future use and build up a list of contract addresses that are not tokens.

        :param token_name: general name for the token we'll provide basic info for
        :type token_name: str
        :returns: dict of token info
        """
        if token_name == 'moonriver':
            return {'name': 'MOVR?', 'symbol': 'MOVR?', 'decimals': '18'}
        elif token_name == 'moonbeam':
            return {'name': 'GLMR?', 'symbol': 'GLMR?', 'decimals': '18'}
        elif token_name == 'MOVR':
            return {'name': 'MOVR', 'symbol': 'MOVR', 'decimals': '18'}
        elif token_name == 'WMOVR':
            return {'name': 'Wrapped MOVR', 'symbol': 'WMOVR', 'decimals': '18',
                    'address': '0x98878B06940aE243284CA214f92Bb71a2b032B8A'}
        elif token_name == 'ROME':
            return {'name': 'ROME', 'symbol': 'ROME', 'decimals': '9',
                    'address': '0x4a436073552044D5f2f49B176853ad3Ad473d9d6'}
        elif token_name == 'sROME':
            return {'name': 'Staked ROME', 'symbol': 'sROME', 'decimals': '9',
                    'address': '0x89F52002E544585b42F8c7Cf557609CA4c8ce12A'}
        elif token_name == 'ZLK':
            return {'name': 'Zenlink Network Token', 'symbol': 'ZLK', 'decimals': '18',
                    'address': '0x0f47ba9d9Bde3442b42175e51d6A367928A1173B'}
        elif token_name == 'SOLAR':
            return {'name': 'SolarBeam Token', 'symbol': 'SOLAR', 'decimals': '18',
                    'address': '0x6bD193Ee6D2104F14F94E2cA6efefae561A4334B'}
        elif token_name == 'SLP':
            return {'name': 'SolarBeam LP Token', 'symbol': 'SLP', 'decimals': '18',
                    'address': '0x7eDA899b3522683636746a2f3a7814e6fFca75e1'}
        # elif token_name == '':
        #     return {'name': '', 'symbol': '', 'decimals': '', 'address': ''}
        # elif token_name == '':
        #     return {'name': '', 'symbol': '', 'decimals': '', 'address': ''}
        elif token_name == '??':
            return {'name': '??', 'symbol': '??', 'decimals': '0'}
        else:
            return None
