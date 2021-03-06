__author__ = 'spazcoin@gmail.com @spazvt'
__author__ = 'Tommi Enenkel @alice_und_bob'

import httpx
import json
import logging
from ratelimit import limits, sleep_and_retry

# "Powered by https://moonbeam.moonscan.io APIs"
# https://moonbeam.moonscan.io/apis#contracts


class MoonscanWrapper:
    """Interface for interacting with the API of explorer Moonscan.io for the Moonriver and Moonbeam chains."""
    def __init__(self, chain, api_key=None):
        self.logger = logging.getLogger("MoonscanWrapper")
        self.endpoint = f"https://api-{chain}.moonscan.io/api"
        self.api_key = api_key

    @sleep_and_retry                # be patient and sleep this thread to avoid exceeding the rate limit
    @limits(calls=3, period=1)      # API limits us to 5 calls/second. Occasionally rate limit hit with 4calls/sec.
    def query(self, params):
        """Rate limited call to fetch another page of data from the Moonscan.io block explorer website

        :param params: Moonscan.io API call params that filter which transactions are returned.
        :type params: list
        """
        if self.api_key is not None:
            params["apikey"] = self.api_key
        response = httpx.get(self.endpoint, params=params)
        self.logger.debug(response)
        return response.text

    def iterate_pages(self, element_processor, params={}):
        """Repeatedly fetch transactions from Moonscan.io matching a set of parameters, iterating one html page at a
        time. Perform post-processing of each transaction using the `element_processor` method provided.
        :param element_processor: method to process each transaction as it is received
        :type element_processor: function
        :param params: Moonscan.io API call params that filter which transactions are returned.
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
        """Fetch all transactions for an address, and then pass them to the processor for processing.

        :param address: address to retrieve transactions for
        :type address: str
        :param element_processor: method to process each transaction as it is received
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
        response = self.query(params)   # will add on the optional API key
        response_dict = json.loads(response)
        if response_dict['status'] == "0" or response_dict['message'] == "NOTOK":
            self.logger.info(f'ABI not retrievable for {contract_address} because "{response_dict["result"]}"')
            return None
        else:
            # response_dict['result'] should contain a long string representation of the contract abi.
            return response_dict['result']

    def get_transaction_receipt(self, tx_hash):
        """Get a transaction's receipt (so that we can get the logs and figure what exactly happened).

        :param tx_hash: transaction hash
        :type tx_hash: str
        :returns: dictionary representing the transaction receipt, or None if not retrievable
        :rtype: dict or None
        """
        params = {"module": "proxy", "action": "eth_getTransactionReceipt", "txhash": tx_hash}
        response = self.query(params)   # will add on the optional API key
        response_dict = json.loads(response)
        # response_dict['result'] should contain a long string representation of the tx receipt.
        return response_dict['result']

