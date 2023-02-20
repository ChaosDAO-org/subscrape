__author__ = 'spazcoin@gmail.com @spazvt'
__author__ = 'Tommi Enenkel @alice_und_bob'

import asyncio
import math
from datetime import datetime
import httpx
import json
import logging
import time

# "Powered by https://moonbeam.moonscan.io APIs"
# https://moonbeam.moonscan.io/apis#contracts

MOONSCAN_MAX_CALLS_PER_SEC_WITHOUT_API_KEY = 0.195  # Empirically determined. Above this, API rate limit errors occur.
MOONSCAN_MAX_CALLS_PER_SEC_WITH_AN_API_KEY = 5      # "5 calls per sec/IP"


class MoonscanWrapper:
    """Interface for interacting with the API of explorer Moonscan.io for the Moonriver and Moonbeam chains."""
    def __init__(self, chain, api_key=None):
        self.logger = logging.getLogger(__name__)
        self.endpoint = f"https://api-{chain}.moonscan.io/api"
        self.api_key = api_key
        if api_key is None:
            self.max_calls_per_sec = MOONSCAN_MAX_CALLS_PER_SEC_WITHOUT_API_KEY
        else:
            self.max_calls_per_sec = MOONSCAN_MAX_CALLS_PER_SEC_WITH_AN_API_KEY
        self.min_wait_between_queries = 1 / self.max_calls_per_sec
        self.logger.info(f'Moonscan.io rate limit set to {self.max_calls_per_sec} API calls per second.'
                         f' Minimum wait time between queries is {self.min_wait_between_queries:.3f} seconds.')
        if api_key is None:
            api_key_sec_between_queries = 1 / MOONSCAN_MAX_CALLS_PER_SEC_WITH_AN_API_KEY
            self.logger.info(f'Moonscan.io rate limit could be {MOONSCAN_MAX_CALLS_PER_SEC_WITH_AN_API_KEY} calls/sec'
                             f' ({api_key_sec_between_queries:.3f} sec between queries) if you had an API key.')
        self.time_of_last_request = 0
        self.semaphore = asyncio.Semaphore(math.ceil(self.max_calls_per_sec))
        self.lock = asyncio.Lock()

    async def __query(self, params, client=None):
        """Rate limited call to fetch another page of data from the Moonscan.io block explorer website

        :param params: Moonscan.io API call params that filter which transactions are returned.
        :type params:
        :param client: client to use for sending http requests for blockchain data. If None, defaults to `httpx`.
        :type client: object
        :returns: JSON structure of response text
        :rtype: dict
        """
        if client is None:
            client = httpx.AsyncClient()

        if self.api_key is not None:
            params["apikey"] = self.api_key

        response = None
        should_request = True
        if self.time_of_last_request == 0:
            self.time_of_last_request = time.time()
        while should_request:  # loop until we get a response
            time_now = time.time()
            time_since_last_request = time_now - self.time_of_last_request
            self.time_of_last_request = time_now
            async with self.semaphore:
                self.logger.debug(f"sending httpx request at {datetime.now().strftime('%H:%M:%S.%f')[:-3]} and"
                                  f" {time_since_last_request:.3f} sec since the last query. {params=}")
                response = await client.get(self.endpoint, params=params, timeout=30.0)
            time_since_last_request = time.time() - self.time_of_last_request
            self.logger.debug(f"request took: {time_since_last_request:.3} seconds. {self.time_of_last_request=:.3f}")

            # lock to prevent multiple threads from trying to sleep at the same time
            await self.lock.acquire()
            try:
                if response.status_code == 429:
                    self.logger.warning("API rate limit exceeded. Waiting 1 second and retrying...")
                    await asyncio.sleep(1)
                elif response.status_code != 200:
                    self.logger.info(f"Status Code: {response.status_code}")
                    self.logger.info(response.headers)
                    raise Exception(f"Error: {response.status_code}")
                else:
                    response_json = json.loads(response.text)
                    if response_json['result'] == "Max rate limit reached, please use API Key for higher rate limit":
                        self.logger.warning("API rate limit exceeded. Waiting 30 seconds and retrying...")
                        await asyncio.sleep(30)
                    else:
                        should_request = False
                        if ('status' in response_json and response_json['status'] == "0") \
                                or ('message' in response_json and response_json['message'] == "NOTOK"):
                            self.logger.warning(f'Moonscan API query failed with response "{response_json["result"]}"'
                                                f' at {datetime.now().strftime("%H:%M:%S.%f")[:-3]} with {params=}')
                        else:
                            # We received a normal response.
                            # Check if we should sleep a little to avoid exceeding the rate limit
                            if time_since_last_request < self.min_wait_between_queries:
                                await asyncio.sleep(self.min_wait_between_queries - time_since_last_request)
            finally:
                self.lock.release()

        self.logger.debug(response)
        response_json = json.loads(response.text)
        return response_json

    async def __iterate_pages(self, element_processor, params={}, tx_filter=None):
        """Repeatedly fetch transactions from Moonscan.io matching a set of parameters, iterating one html page at a
        time. Perform post-processing of each transaction using the `element_processor` method provided.
        :param element_processor: method to process each transaction as it is received
        :type element_processor: function
        :param params: Moonscan.io API call params that filter which transactions are returned.
        :type params: function
        :param tx_filter: method that returns True if certain transactions should be filtered out of the results
        :type tx_filter: function
        """
        done = False             # keep crunching until we are done
        previous_block = 0       # to check if the iterator actually moved forward
        last_block_received = 0  # to compare most recent block to end block requested
        count = 0                # counter for how many items we queried already
        if 'startblock' in params:
            start_block = int(params['startblock'])
        else:
            start_block = 0

        while not done:
            params["startblock"] = str(start_block)
            response_obj = await self.__query(params)

            if response_obj["status"] == "0":
                self.logger.info(f"received empty result. message='{response_obj['message']}' and"
                                 f" result='{response_obj['result']}' at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
                return

            elements = response_obj["result"]

            # process the elements
            for element in elements:
                last_block_received = int(element['blockNumber'])
                if tx_filter is not None and tx_filter(element):
                    continue
                await element_processor(element)

            # update counters and check if we should exit
            count += len(elements)
            self.logger.debug(count)

            start_block = int(element["blockNumber"])
            end_block = int(params["endblock"])
            if start_block == previous_block or last_block_received == end_block:
                done = True
            previous_block = start_block

    async def fetch_and_process_transactions(self, address, element_processor, config=None):
        """Fetch all transactions for a given address (account/contract) and use the given processor method to filter
        or post-process each transaction as we work through them.

        :param address: the moonriver/moonbeam account number of interest. This could be a basic account, or a contract
        address, depending on the kind of transactions being analyzed.
        :type address: str
        :param element_processor: a method that is used to post-process every transaction for the given address as it is
        retrieved from the API. Processing transactions as they come in, instead of storing all transaction data helps
        cut down on required storage.
        :type element_processor: function
        :param config: the `ScrapeConfig`
        :type config: ScrapeConfig
        """
        start_block = 1
        end_block = 99999999
        if config.filter_conditions is not None:
            for group in config.filter_conditions:
                if 'blockNumber' in group:
                    predicates = group['blockNumber']
                    for predicate in predicates:
                        if '==' in predicate:
                            value = predicate['==']
                            if type(value) is not int:
                                value = int(value)
                            start_block = value
                            end_block = value
                        elif '<' in predicate:
                            value = predicate['<']
                            if type(value) is not int:
                                value = int(value)
                            end_block = value - 1
                        elif '<=' in predicate:
                            value = predicate['<=']
                            if type(value) is not int:
                                value = int(value)
                            end_block = value
                        elif '>' in predicate:
                            value = predicate['>']
                            if type(value) is not int:
                                value = int(value)
                            start_block = value + 1
                        elif '>=' in predicate:
                            value = predicate['>=']
                            if type(value) is not int:
                                value = int(value)
                            start_block = value
        params = {"module": "account", "action": "txlist", "address": address,
                  "startblock": str(start_block), "endblock": str(end_block), "sort": "asc"}
        if config and config.filter is not None:
            await self.__iterate_pages(element_processor, params=params, tx_filter=config.filter)
        else:
            await self.__iterate_pages(element_processor, params=params)

    async def get_contract_abi(self, contract_address):
        """Get a contract's ABI (so that its transactions can be decoded).

        :param contract_address: contract address
        :type contract_address: str
        :returns: string representing the contract's ABI, or None if not retrievable
        :rtype: str or None
        """
        params = {"module": "contract", "action": "getabi", "address": contract_address}
        response_dict = await self.__query(params)   # will add on the optional API key
        if response_dict['status'] == "0" or response_dict['message'] == "NOTOK":
            self.logger.info(f'ABI not retrievable for {contract_address} because "{response_dict["result"]}"'
                             f' at {datetime.now().strftime("%H:%M:%S.%f")[:-3]}')
            return None
        else:
            # response_dict['result'] should contain a long string representation of the contract abi.
            return response_dict['result']

    async def get_transaction_receipt(self, tx_hash):
        """Get a transaction's receipt (so that we can get the logs and figure out what exactly happened).

        :param tx_hash: transaction hash
        :type tx_hash: str
        :returns: dictionary representing the transaction receipt, or None if not retrievable
        :rtype: dict or None
        """
        params = {"module": "proxy", "action": "eth_getTransactionReceipt", "txhash": tx_hash}
        response_dict = await self.__query(params)   # will add on the optional API key
        # response_dict['result'] should contain a long string representation of the tx receipt.
        return response_dict['result']
