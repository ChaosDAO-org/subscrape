__author__ = 'Tommi Enenkel @alice_und_bob'

from datetime import datetime
import httpx
import json
import logging
from ratelimit import limits, sleep_and_retry
from subscrape.db.subscrape_db import SubscrapeDB
from substrateinterface.utils import ss58

#import http.client
#http.client.HTTPConnection.debuglevel = 1
#requests_log = logging.getLogger("requests.packages.urllib3")
#requests_log.setLevel(logging.DEBUG)
#requests_log.propagate = True

SUBSCAN_MAX_CALLS_PER_SEC_WITHOUT_API_KEY = 2
SUBSCAN_MAX_CALLS_PER_SEC_WITH_AN_API_KEY = 30
MAX_CALLS_PER_SEC = SUBSCAN_MAX_CALLS_PER_SEC_WITHOUT_API_KEY


class SubscanBase:
    """
    Interface for interacting with the API of explorer Subscan.io for the Moonriver and Moonbeam chains.
    """

    def __init__(self, chain, db: SubscrapeDB, api_key):
        """
        Initializes the SubscanBase.
        :param chain: The chain to scrape.
        :type chain: str
        :param db: The database to write to.
        :type db: SubscrapeDB
        :param api_key: The api key to use. Use None, if no api key is to be used.
        :type api_key: str or None
        """
        self.logger = logging.getLogger("SubscanWrapper")
        self.endpoint = f"https://{chain}.api.subscan.io"
        self.db = db
        self.api_key = api_key
        global MAX_CALLS_PER_SEC
        if api_key is not None:
            MAX_CALLS_PER_SEC = SUBSCAN_MAX_CALLS_PER_SEC_WITH_AN_API_KEY
        self.logger.info(f'Subscan rate limit set to {MAX_CALLS_PER_SEC} API calls per second.')

    @sleep_and_retry                # be patient and sleep this thread to avoid exceeding the rate limit
    @limits(calls=MAX_CALLS_PER_SEC, period=1)     # API limits us to 30 calls every second
    def _query(self, method, headers={}, body={}):
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
            #self.logger.debug(response.headers)
            pass

        #self.logger.debug(response.text)
        # unpack the payload
        obj = json.loads(response.text)
        return obj["data"]        

    # iterates through all pages until it processed all elements
    # or gets False from the processor
    def _iterate_pages(
        self,
        method, 
        element_processor, 
        list_key=None, 
        body={}, 
        filter=None) -> int:
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
        :return: number of items processed
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
            data = self._query(method, body=body)
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
            count_new_elements = 0
            for element in elements:
                if filter is not None:
                    should_skip = filter(element)
                    if should_skip:
                        continue
                # process the element and check if we should continue
                was_new_element = element_processor(element)
                if was_new_element:
                    count_new_elements += 1

            if count_new_elements == 0:
                self.logger.info("We did not find any new elements on the latest page. Stopping.")
                break
            else:
                self.logger.debug(f"Found {count_new_elements} new elements on page {page}.")
                pass

            # update counters and check if we should exit
            count += len(elements)
            self.logger.debug(count)

            if count >= limit:
                done = True

            page += 1

        return count

    def _element_processor(self, element_processor, index_decucer):
        """
        Factory method for creating a function that can be used to process an element in the list.
        :param element_processor: The element processor function
        :type element_processor: function
        :param index_decucer: The index decucer function
        :type index_decucer: function
        :return: The function that can be used to process an element in the list
        """
        def process_element(element):
            index = index_decucer(element)
            return element_processor(index, element)
        return process_element


    def fetch_extrinsics(self, module, call, config) -> int:
        """
        Scrapes all extrinsics matching the specified module and call (like `utility.batchAll` or `system.remark`)

        :param module: extrinsic module to look for, like `system`, `utility`, etc
        :type module: str
        :param call: extrinsic module's specific 'call' or method, like system's `remark` call.
        :type call: str
        :param config: the `ScrapeConfig`
        :type config: ScrapeConfig
        :return: the number of items scraped
        """
        items_scraped = 0
        with self.db.storage_manager_for_extrinsics_call(module, call) as extrinsics_storage:
            if config.digits_per_sector is not None:
                extrinsics_storage.digits_per_sector = config.digits_per_sector

            self.logger.info(f"Fetching extrinsic {module}.{call} from {self.endpoint}")

            body = {"module": module, "call": call}
            if config.params is not None:
                body.update(config.params)

            items_scraped += self._iterate_pages(
                self._api_method_extrinsics,
                self._element_processor(
                    extrinsics_storage.write_item,
                    self._extrinsic_index_deducer),
                list_key="extrinsics",
                body=body,
                filter=config.filter
                )

            extrinsics_storage.flush()
        return items_scraped



    def fetch_events(self, module, call, config) -> int:
        """
        Scrapes all events matching the specified module and call (like `utility.batchAll` or `system.remark`)

        :param module: extrinsic module to look for, like `system`, `utility`, etc
        :type module: str
        :param call: extrinsic module's specific 'call' or method, like system's `remark` call.
        :type call: str
        :param config: the `ScrapeConfig`
        :type config: ScrapeConfig
        :return: the number of items scraped
        """
        items_scraped = 0
        sm = self.db.storage_manager_for_events_call(module, call)
        if config.digits_per_sector is not None:
            sm.digits_per_sector = config.digits_per_sector

        self.logger.info(f"Fetching events {module}.{call} from {self.endpoint}")

        body = {"module": module, self._api_method_events_call: call}
        if config.params is not None:
            body.update(config.params)

        items_scraped += self._iterate_pages(
            self._api_method_events,
            self._element_processor(
                sm.write_item,
                self._events_index_deducer),
            list_key="events",
            body=body,
            filter=config.filter
            )

        sm.flush()

        return items_scraped


    def fetch_transfers(self, account, chain_config) -> int:
        """
        Fetches the transfers for a single account and writes them to the db.

        :param account: The account to scrape
        :type account: str
        :param call_config: The call_config which has the filter set
        :type call_config: ScrapeConfig
        :return: the number of items scraped
        """
        items_scraped = 0
        # normalize to Substrate address
        public_key = ss58.ss58_decode(account)
        substrate_address = ss58.ss58_encode(public_key, ss58_format=42)

        self.db.set_active_transfers_account(substrate_address)

        self.logger.info(f"Fetching transfers for {substrate_address} from {self.api.endpoint}")

        body = {"address": substrate_address}
        items_scraped += self.api.iterate_pages(
            self._api_method_transfers,
            self.db.write_transfer,
            list_key="transfers",
            body=body,
            filter=chain_config.filter
            )

        self.db.flush_transfers()
        return items_scraped

    def fetch_extrinsic(self, extrinsic_index):
        """
        Fetches the extrinsic with the specified index and writes it to the db.
        :param extrinsic_index: The extrinsix index to fetch
        :type extrinsic_index: str
        :return: the number of items scraped
        """
        self.logger.info(f"Fetching extrinsic {extrinsic_index} from {self.endpoint}")

        method = self._api_method_extrinsic
        body = {"extrinsic_index": extrinsic_index}
        data = self._query(method, body=body)
        index = self._extrinsic_index_deducer(data)
        self.db.write_extrinsic(index, data)
        return 1