__author__ = 'spazcoin@gmail.com @spazvt'
__author__ = 'Tommi Enenkel @alice_und_bob'

import httpx
import json
import logging
from ratelimit import limits, sleep_and_retry


class BlockscoutWrapper:
    """Interface for interacting with the API of the Blockscout explorer for the Moonriver and Moonbeam chains."""
    def __init__(self, chain):
        self.logger = logging.getLogger("BlockscoutWrapper")
        self.endpoint = f"https://blockscout.{chain}.moonbeam.network/api"

    @sleep_and_retry                # be patient and sleep this thread to avoid exceeding the rate limit
    @limits(calls=5, period=1)      # No API limit stated on Blockscout website, so choose conservative 5 calls/sec
    def query(self, params):
        """Rate limited call to fetch another page of data from the Blockscout block explorer website

        :param params: Blockscout API call params that filter which transactions are returned.
        :type params: list
        """
        response = httpx.get(self.endpoint, params=params)
        self.logger.debug(response)
        return response.text

    def iterate_pages(self, element_processor, params={}):
        """Repeatedly fetch transactions from Blockscout matching a set of parameters, iterating one html page at a
        time. Perform post-processing of each transaction using the `element_processor` method provided.
        :param element_processor: method to process each transaction as it is received
        :type element_processor: function
        :param params: Blockscout API call params that filter which transactions are returned.
        :type params: function
        """
        done = False            # keep crunching until we are done
        start_block = 0         # iterator for the page we want to query
        previous_block = 0      # to check if the iterator actually moved forward
        count = 0               # counter for how many items we queried already
        limit = 0               # max amount of items to be queried. to be determined after the first call

        while not done:
            params["startblock"] = start_block
            response = self.query(params)

            # unpackage the payload
            obj = json.loads(response)
            if obj["status"] == "0":
                self.logger.info("received empty result")
                return
        
            elements = obj["result"]

            # process the elements
            for element in elements:
                element_processor(element)

            # update counters and check if we should exit
            count += len(elements)
            self.logger.info(count)

            start_block = element["blockNumber"]
            if start_block == previous_block:
                done = True
            previous_block = start_block

    def fetch_and_process_transactions(self, address, element_processor):
        """Fetch all transactions for a given address (account/contract) and use the given processor method to filter
        or post-process each transaction as we work through them.

        :param address: the moonriver/moonbeam account number of interest. This could be a basic account, or a contract
        address, depending on the kind of transactions being analyzed.
        :type address: str
        :param element_processor: a method that is used to post-process every transaction for the given address as it is
        retrieved from the API. Processing transactions as they come in, instead of storing all transaction data helps
        cut down on required storage.
        :type element_processor: function
        """
        params = {"module": "account", "action": "txlist", "address": address, "startblock": "1",
                  "endblock": "99999999", "sort": "asc"}
        self.iterate_pages(element_processor, params=params)

    def get_contract_abi(self, contract_address):
        """Get a contract's ABI (so that its transactions can be decoded).

        :param contract_address: contract address
        :type contract_address: str
        :returns: string representing the contract's ABI, or None if not retrievable
        :rtype: str or None
        """
        params = {"module": "contract", "action": "getabi", "address": contract_address}
        response = self.query(params)
        response_dict = json.loads(response)
        if response_dict['status'] == "0" or response_dict['message'] == "NOTOK":
            self.logger.info(f'ABI not retrievable for {contract_address} because "{response_dict["result"]}"')
            return None
        else:
            # response_dict['result'] should contain a long string representation of the contract abi.
            return response_dict['result']

    def get_token_info(self, token_address, verbose=True):
        """Get a token's basic info (name, ticker symbol, decimal places)

        :param token_address: token address
        :type token_address: str
        :param verbose: should the "not retrievable" message be printed out?
        :type verbose: bool
        :returns: dictionary of values about the token, or None if not retrievable
        :rtype: dict or None
        """
        params = {"module": "token", "action": "getToken", "contractaddress": token_address}
        response = self.query(params)
        response_dict = json.loads(response)
        if response_dict['status'] == "0" or response_dict['message'] == "NOTOK":
            if verbose:
                self.logger.info(f'Token info not retrievable for {token_address} because "{response_dict["result"]}"')
            return None
        else:
            # response_dict['result'] should contain the info about the token.
            return response_dict['result']
