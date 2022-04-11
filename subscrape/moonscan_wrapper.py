from tracemalloc import start
import httpx
import json
import logging

#https://moonbeam.moonscan.io/apis#contracts


class MoonscanWrapper:
    def __init__(self, endpoint, api_key=None):
        self.logger = logging.getLogger("MoonscanWrapper")
        self.endpoint = endpoint
        self.api_key = api_key

    def query(self, params):
        params["apikey"] = self.api_key
        response = httpx.get(self.endpoint, params=params)
        self.logger.debug(response)
        return response.text

    def iterate_pages(self, element_processor, params={}):
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
