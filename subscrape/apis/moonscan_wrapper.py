__author__ = 'spazcoin@gmail.com @spazvt'
__author__ = 'Tommi Enenkel @alice_und_bob'

import asyncio
from datetime import datetime
import httpx
import json
import logging
from ratelimit import limits, sleep_and_retry
import time

# "Powered by https://moonbeam.moonscan.io APIs"
# https://moonbeam.moonscan.io/apis#contracts

MOONSCAN_MAX_CALLS_PER_SEC_WITHOUT_API_KEY = 1  # not published, but we ran into issues with higher values
MOONSCAN_MAX_CALLS_PER_SEC_WITH_AN_API_KEY = 3  # "5 calls per sec/IP" but occasionally rate limit hit with 4calls/sec
MAX_CALLS_PER_SEC = MOONSCAN_MAX_CALLS_PER_SEC_WITHOUT_API_KEY


class MoonscanWrapper:
    """Interface for interacting with the API of explorer Moonscan.io for the Moonriver and Moonbeam chains."""
    def __init__(self, chain, api_key=None):
        self.logger = logging.getLogger(__name__)
        self.endpoint = f"https://api-{chain}.moonscan.io/api"
        self.api_key = api_key
        global MAX_CALLS_PER_SEC
        if api_key is not None:
            MAX_CALLS_PER_SEC = MOONSCAN_MAX_CALLS_PER_SEC_WITH_AN_API_KEY
        self.logger.info(f'Moonscan.io rate limit set to {MAX_CALLS_PER_SEC} API calls per second.')
        self.semaphore = asyncio.Semaphore(MAX_CALLS_PER_SEC)
        self.lock = asyncio.Lock()

    @sleep_and_retry                # be patient and sleep this thread to avoid exceeding the rate limit
    # @limits(calls=MAX_CALLS_PER_SEC, period=1)
    async def _query(self, params, client=None):
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
        while should_request:  # loop until we get a response
            before = datetime.now()
            async with self.semaphore:
                response = await client.get(self.endpoint, params=params, timeout=30.0)
            after = datetime.now()
            self.logger.debug("request took: " + str(after - before))

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
                    should_request = False
            finally:
                self.lock.release()

        self.logger.debug(response)
        response_json = json.loads(response.text)
        return response_json

    async def _iterate_pages(self, element_processor, params={}, tx_filter=None):
        """Repeatedly fetch transactions from Moonscan.io matching a set of parameters, iterating one html page at a
        time. Perform post-processing of each transaction using the `element_processor` method provided.
        :param element_processor: method to process each transaction as it is received
        :type element_processor: function
        :param params: Moonscan.io API call params that filter which transactions are returned.
        :type params: function
        :param tx_filter: method that returns True if certain transactions should be filtered out of the results
        :type tx_filter: function
        """
        done = False            # keep crunching until we are done
        start_block = 0         # iterator for the page we want to query
        previous_block = 0      # to check if the iterator actually moved forward
        count = 0               # counter for how many items we queried already

        while not done:
            params["startblock"] = start_block
            response_obj = await self._query(params)

            if response_obj["status"] == "0":
                self.logger.info(f"received empty result. message='{response_obj['message']}' and result='{response_obj['result']}'"
                                 f" at {time.strftime('%X')}")
                return

            elements = response_obj["result"]

            # process the elements
            for element in elements:
                if tx_filter is not None and tx_filter(element):
                    continue
                await element_processor(element)

            # update counters and check if we should exit
            count += len(elements)
            self.logger.info(count)

            start_block = element["blockNumber"]
            if start_block == previous_block:
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
        params = {"module": "account", "action": "txlist", "address": address, "startblock": "1",
                  "endblock": "99999999", "sort": "asc"}
        if config and hasattr(config, 'filter'):
            await self._iterate_pages(element_processor, params=params, tx_filter=config.filter)
        else:
            await self._iterate_pages(element_processor, params=params)

    async def get_contract_abi(self, contract_address):
        """Get a contract's ABI (so that its transactions can be decoded).

        :param contract_address: contract address
        :type contract_address: str
        :returns: string representing the contract's ABI, or None if not retrievable
        :rtype: str or None
        """
        params = {"module": "contract", "action": "getabi", "address": contract_address}
        response_dict = await self._query(params)   # will add on the optional API key
        if response_dict['status'] == "0" or response_dict['message'] == "NOTOK":
            self.logger.info(f'ABI not retrievable for {contract_address} because "{response_dict["result"]}"'
                             f' at {time.strftime("%X")}')
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
        response_dict = await self._query(params)   # will add on the optional API key
        # response_dict['result'] should contain a long string representation of the tx receipt.
        return response_dict['result']
