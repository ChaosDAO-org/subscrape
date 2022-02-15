import json
import requests
from datetime import datetime
import logging

#import http.client
#http.client.HTTPConnection.debuglevel = 1
#requests_log = logging.getLogger("requests.packages.urllib3")
#requests_log.setLevel(logging.DEBUG)
#requests_log.propagate = True

class SubscanWrapper:

    def __init__(self, api_key):
        self.logger = logging.getLogger("SubscanWrapper")
        self.api_key = api_key

    def query(self, url, headers = {}, body = {}):
        headers["Content-Type"] = "application/json"
        headers["x-api-key"] = self.api_key
        body = json.dumps(body)
        before = datetime.now()
        # TE: there seems to be an issue with the way the requests library handles the server response
        # the request will only conclude successfully after it timed out. one possible reason could be
        # that the server is sending no content-length header. I tried adding the timeout param and it
        # forces a faster timeout and successful conclusion of the request.
        # Possibly related discussion: https://github.com/psf/requests/issues/4023
        response = requests.post(url, headers = headers, data = body, timeout=2)
        after = datetime.now()
        self.logger.debug("request took: " + str(after - before))

        if response.status_code != 200:
            self.logger.info(f"Status Code: {response.status_code}")
            self.logger.info(response.headers)
            raise Exception()
        else:
            self.logger.debug(response.headers)

        return response.text

    async def iterate_pages(self, url, element_processor, list_key=None, body={}):
        assert(list_key is not None)

        done = False        # keep crunching until we are done
        page = 0            # iterator for the page we want to query
        rows_per_page = 100 # constant for the rows per page to query
        count = 0           # counter for how many items we queried already
        limit = 0           # max amount of items to be queried. to be determined after the first call

        body["row"] = rows_per_page

        while not done:
            body["page"] = page
            response = self.query(url, body=body)
            self.logger.debug(response)

            # unpackage the payload
            obj = json.loads(response)
            data = obj["data"]
            # determine the limit on the first run
            if limit == 0: 
                limit = data["count"]
                self.logger.info(f"About to fetch {limit} entries.")
            elements = data[list_key]

            # process the elements
            for element in elements:
                element_processor(element)

            # update counters and check if we should exit
            count += len(elements)
            self.logger.info(count)

            if count >= limit:
                done = True

            page += 1