__author__ = 'Tommi Enenkel @alice_und_bob'

from datetime import datetime
import httpx
import json
import logging
from ratelimit import limits, sleep_and_retry

#import http.client
#http.client.HTTPConnection.debuglevel = 1
#requests_log = logging.getLogger("requests.packages.urllib3")
#requests_log.setLevel(logging.DEBUG)
#requests_log.propagate = True

SUBSCAN_MAX_CALLS_PER_SEC_WITHOUT_API_KEY = 2
SUBSCAN_MAX_CALLS_PER_SEC_WITH_AN_API_KEY = 30
MAX_CALLS_PER_SEC = SUBSCAN_MAX_CALLS_PER_SEC_WITHOUT_API_KEY


class SubscanWrapper:
    """Interface for interacting with the API of explorer Subscan.io for the Moonriver and Moonbeam chains."""
    def __init__(self, chain, api_key=None):
        self.logger = logging.getLogger("SubscanWrapper")
        self.endpoint = f"https://{chain}.api.subscan.io"
        self.api_key = api_key
        global MAX_CALLS_PER_SEC
        if api_key is not None:
            MAX_CALLS_PER_SEC = SUBSCAN_MAX_CALLS_PER_SEC_WITH_AN_API_KEY
        self.logger.info(f'Subscan rate limit set to {MAX_CALLS_PER_SEC} API calls per second.')

    @sleep_and_retry                # be patient and sleep this thread to avoid exceeding the rate limit
    @limits(calls=MAX_CALLS_PER_SEC, period=1)     # API limits us to 30 calls every second
    def query(self, method, headers={}, body={}):
        """Rate limited call to fetch another page of data from the Subscan.io block explorer website

        :param method: Subscan.io API call method.
        :type method: str
        :param headers: Subscan.io API call headers.
        :type headers: list
        :param headers: Subscan.io API call body. Typically, used to specify each page being requested.
        :type body: list
        """
        headers["Content-Type"] = "application/json"
        if self.api_key is not None:
            headers["x-api-key"] = self.api_key
        body = json.dumps(body)
        before = datetime.now()
        url = self.endpoint + method
        response = httpx.post(url, headers=headers, data=body)
        after = datetime.now()
        self.logger.debug("request took: " + str(after - before))

        if response.status_code != 200:
            self.logger.info(f"Status Code: {response.status_code}")
            self.logger.info(response.headers)
            raise Exception()
        else:
            self.logger.debug(response.headers)

        self.logger.debug(response.text)
        # unpack the payload
        obj = json.loads(response.text)
        return obj["data"]        

    # iterates through all pages until it processed all elements
    # or gets False from the processor
    def iterate_pages(self, method, element_processor, list_key=None, body={}, filter=None):
        """Repeatedly fetch transactions from Subscan.io matching a set of parameters, iterating one html page at a
        time. Perform post-processing of each transaction using the `element_processor` method provided.
        :param method: Subscan.io API call method.
        :type method: str
        :param element_processor: method to process each transaction as it is received
        :type element_processor: function
        :param list_key: whether `events` or `extrinsics` should be looked for
        :type list_key: str or None
        :param body: Subscan.io API call body. Typically, used to specify each page being requested.
        :type body: list
        :param filter: method to determine whether certain extrinsics/events should be filtered out of the results
        :type filter: function
        """
        assert(list_key is not None)

        done = False        # keep crunching until we are done
        page = 0            # iterator for the page we want to query
        rows_per_page = 100 # constant for the rows per page to query
        count = 0           # counter for how many items we queried already
        limit = 0           # max amount of items to be queried. to be determined after the first call

        body["row"] = rows_per_page

        while not done:
            body["page"] = page
            data = self.query(method, body=body)
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
