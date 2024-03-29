__author__ = 'spazcoin@gmail.com @spazvt'
__author__ = 'Tommi Enenkel @alice_und_bob'

import asyncio
from datetime import datetime
import httpx
import json
import logging


class BlockscoutWrapper:
    """Interface for interacting with the API of the Blockscout explorer for the Moonriver and Moonbeam chains."""
    def __init__(self, chain):
        self.logger = logging.getLogger(__name__)
        self.endpoint = f"https://blockscout.{chain}.moonbeam.network/api"
        # No API limit stated on Blockscout website, so choose conservative 5 calls/sec
        self.semaphore = asyncio.Semaphore(5)
        self.lock = asyncio.Lock()

    async def __query(self, params, client=None):
        """Rate limited call to fetch another page of data from the Blockscout block explorer website

        :param params: Blockscout API call params that filter which transactions are returned.
        :type params:
        :param client: client to use for sending http requests for blockchain data. If None, defaults to `httpx`.
        :type client: object
        :returns: JSON structure of response text
        :rtype: dict
        """
        if client is None:
            client = httpx.AsyncClient()

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
                    self.logger.info(response)
                    raise Exception(f"Error: {response.status_code}")
                else:
                    should_request = False
            finally:
                self.lock.release()

        self.logger.debug(response)
        response_json = json.loads(response.text)
        return response_json

    async def __iterate_pages(self, element_processor, params={}, tx_filter=None):
        """Repeatedly fetch transactions from Blockscout matching a set of parameters, iterating one html page at a
        time. Perform post-processing of each transaction using the `element_processor` method provided.
        :param element_processor: method to process each transaction as it is received
        :type element_processor: function
        :param params: Blockscout API call params that filter which transactions are returned.
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
            response_obj = self.__query(params)

            if response_obj["status"] == "0":
                self.logger.info("received empty result")
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
            self.logger.info(count)

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
        params = {"module": "account", "action": "txlist", "address": address, "startblock": "1",
                  "endblock": "99999999", "sort": "asc"}
        if config and hasattr(config, 'filter'):
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
        response_dict = await self.__query(params)
        if response_dict['status'] == "0" or response_dict['message'] == "NOTOK":
            self.logger.info(f'ABI not retrievable for {contract_address} because "{response_dict["result"]}"')
            return None
        else:
            # response_dict['result'] should contain a long string representation of the contract abi.
            return response_dict['result']

    async def get_transaction_receipt(self, tx_hash, verbose=True):
        """Get a transaction's info, including the logs to figure out what exactly happened.

        :param tx_hash: transaction hash
        :type tx_hash: str
        :param verbose: should the "not retrievable" message be printed out?
        :type verbose: bool
        :returns: dictionary representing the transaction receipt, or None if not retrievable
        :rtype: dict or None
        """
        params = {"module": "transaction", "action": "gettxinfo", "txhash": tx_hash}
        response_dict = await self.__query(params)
        # response_dict['logs'] should contain a long string representation of the tx receipt.
        # return response_dict['logs']
        if response_dict['status'] == "0" or response_dict['message'] == "NOTOK":
            if verbose:
                self.logger.info(f'Transaction logs not retrievable for {tx_hash} because "{response_dict["result"]}"')
            return None
        else:
            # response_dict['result'] should contain the info about the token.
            return response_dict['result']

    async def get_token_info(self, token_address, verbose=True):
        """Get a token's basic info (name, ticker symbol, decimal places)

        :param token_address: token address
        :type token_address: str
        :param verbose: should the "not retrievable" message be printed out?
        :type verbose: bool
        :returns: dictionary of values about the token, or None if not retrievable
        :rtype: dict or None
        """
        params = {"module": "token", "action": "getToken", "contractaddress": token_address}
        response_dict = await self.__query(params)
        if response_dict['status'] == "0" or response_dict['message'] == "NOTOK":
            if verbose:
                self.logger.info(f'Token info not retrievable for {token_address} because "{response_dict["result"]}"')
            return None
        else:
            # response_dict['result'] should contain the info about the token.
            return response_dict['result']
