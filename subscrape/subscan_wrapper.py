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

    def __init__(self, api_key, endpoint):
        self.logger = logging.getLogger("SubscanWrapper")
        self.api_key = api_key
        self.endpoint = endpoint

    def query(self, method, headers = {}, body = {}):
        headers["Content-Type"] = "application/json"
        headers["x-api-key"] = self.api_key
        body = json.dumps(body)
        before = datetime.now()
        url = self.endpoint + method
        response = requests.post(url, headers = headers, data = body)
        after = datetime.now()
        self.logger.debug("request took: " + str(after - before))

        if response.status_code != 200:
            self.logger.info(f"Status Code: {response.status_code}")
            self.logger.info(response.headers)
            raise Exception()
        else:
            self.logger.debug(response.headers)

        return response.text

    # iterates through all pages until it processed all elements
    # or gets False from the processor
    def iterate_pages(self, method, element_processor, list_key=None, body={}, filter=None):
        assert(list_key is not None)

        done = False        # keep crunching until we are done
        page = 0            # iterator for the page we want to query
        rows_per_page = 100 # constant for the rows per page to query
        count = 0           # counter for how many items we queried already
        limit = 0           # max amount of items to be queried. to be determined after the first call

        body["row"] = rows_per_page

        while not done:
            body["page"] = page
            response = self.query(method, body=body)
            self.logger.debug(response)

            # unpackage the payload
            obj = json.loads(response)
            data = obj["data"]
            # determine the limit on the first run
            if limit == 0: 
                limit = data["count"]
                self.logger.info(f"About to fetch {limit} entries.")
                if limit == 0:
                    break
            elements = data[list_key]

            if elements is None:
                self.logger.info("elements was empty. Stopping.")
                break

            # process the elements
            # Subscan has no cursor and so getting to a new page could yield results
            # that were already present on the previous page. we try to cope with
            # this by checking if any of the elements on the current page were new
            # and if they were, we continue
            found_new_elements = False
            for element in elements:
                if filter is not None:
                    should_skip = filter(element)
                    if should_skip:
                        continue
                # process the element and check if we should continue
                was_new_element = element_processor(element)
                if was_new_element:
                    found_new_elements = True

            if not found_new_elements:
                self.logger.info("We did not find any new elements on the latest page. Stopping.")
                break

            # update counters and check if we should exit
            count += len(elements)
            self.logger.debug(count)

            if count >= limit:
                done = True

            page += 1