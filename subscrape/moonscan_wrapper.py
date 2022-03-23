from tracemalloc import start
import requests
import json
import logging

#https://moonbeam.moonscan.io/apis#contracts

class MoonscanWrapper:
    def __init__(self, endpoint):
        self.logger = logging.getLogger("MoonscanWrapper")
        self.endpoint = endpoint


    def query(self, params):
        params["apikey"] = "YourApiKeyToken"
        response = requests.get(self.endpoint, params=params)
        self.logger.debug(response)
        return response.text
        

    def iterate_pages(self, element_processor, params={}):
        done = False            # keep crunching until we are done
        startblock = 0          # iterator for the page we want to query
        previousblock = 0       # to check if the iterator actually moved forward
        count = 0               # counter for how many items we queried already
        limit = 0               # max amount of items to be queried. to be determined after the first call

        while not done:
            params["startblock"] = startblock
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

            startblock = element["blockNumber"]
            if startblock == previousblock:
                done = True
            previousblock = startblock