__author__ = 'Tommi Enenkel @alice_und_bob'

from datetime import datetime
import httpx
import json
import logging
from ratelimit import limits, sleep_and_retry
from subscrape.db.subscrape_db import SubscrapeDB, Extrinsic, Event
from substrateinterface.utils import ss58
import asyncio
from subscrape.scrapers.scrape_config import ScrapeConfig

#import http.client
#http.client.HTTPConnection.debuglevel = 1
#requests_log = logging.getLogger("requests.packages.urllib3")
#requests_log.setLevel(logging.DEBUG)
#requests_log.propagate = True

SUBSCAN_MAX_CALLS_PER_SEC_WITHOUT_API_KEY = 2
SUBSCAN_MAX_CALLS_PER_SEC_WITH_AN_API_KEY = 30
MAX_CALLS_PER_SEC = SUBSCAN_MAX_CALLS_PER_SEC_WITHOUT_API_KEY


class SubscanWrapper:
    """
    Interface for interacting with the API of explorer Subscan.io for the Moonriver and Moonbeam chains.
    """

    def __init__(self, chain: str, db: SubscrapeDB, api_key: str = None):
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
        self.chain = chain.lower()
        self.endpoint = f"https://{self.chain}.api.subscan.io"
        self.db: SubscrapeDB = db
        self.api_key = api_key
        global MAX_CALLS_PER_SEC
        if api_key is not None:
            MAX_CALLS_PER_SEC = 5
        self.logger.info(f'Subscan rate limit set to {MAX_CALLS_PER_SEC} API calls per second.')
        self.semaphore = asyncio.Semaphore(MAX_CALLS_PER_SEC)
        self.lock = asyncio.Lock()

        self._extrinsic_index_deducer = lambda e: e["extrinsic_index"]
        #self._events_index_deducer = lambda e: f"{e['event_index']}"
        self._event_index_deducer = lambda e: f"{e['block_num']}-{e['event_idx']}"
        #self._transfers_index_deducer = lambda e: f"{e['block_num']}-{e['event_idx']}"
        self._last_id_deducer = lambda e: e["id"]
        self._api_method_extrinsics = "/api/v2/scan/extrinsics"
        self._api_method_extrinsic = "/api/scan/extrinsic"
        self._api_method_events = "/api/v2/scan/events"
        self._api_method_event = "/api/scan/event"
        self._api_method_events_call = "event_id"


    @sleep_and_retry                # be patient and sleep this thread to avoid exceeding the rate limit
    #@limits(calls=MAX_CALLS_PER_SEC, period=1)     # API limits us to 30 calls every second
    async def _query(self, method, headers={}, body={}, client=None):
        """Rate limited call to fetch another page of data from the Subscan.io block explorer website

        :param method: Subscan.io API call method.
        :type method: str
        :param headers: Subscan.io API call headers.
        :type headers: list
        :param headers: Subscan.io API call body. Typically, used to specify each page being requested.
        :type body: list
        """
        if client is None:
            client = httpx.AsyncClient()

        headers["Content-Type"] = "application/json"
        if self.api_key is not None:
            headers["x-api-key"] = self.api_key
        body = json.dumps(body)
        url = self.endpoint + method

        response = None
        should_request = True
        while should_request: # loop until we get a response
            before = datetime.now()
            async with self.semaphore:
                response = await client.post(url, headers=headers, data=body)
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

        #self.logger.debug(response.text)
        # unpack the payload
        obj = json.loads(response.text)
        return obj["data"]        

    # iterates through all pages until it processed all elements
    # or gets False from the processor
    async def _iterate_pages(
        self,
        method, 
        element_processor, 
        list_key,
        last_id_deducer,
        body={}, 
        filter=None) -> list:
        """Repeatedly fetch transactions from Subscan.io matching a set of parameters, iterating one html page at a
        time. Perform post-processing of each transaction using the `element_processor` method provided.
        :param method: Subscan.io API call method.
        :type method: str
        :param element_processor: method to process each transaction as it is received
        :type element_processor: function
        :param list_key: what's the subkey in the response that contains the list of elements
        :type list_key: str
        :param last_id_deducer: method to deduce the last id from the last element in the list
        :type last_id_deducer: function
        :param body: Subscan.io API call body. Typically, used to specify each page being requested.
        :type body: list
        :param filter: method to determine whether certain extrinsics/events should be filtered out of the results
        :type filter: function
        :return: the items processed
        """

        done = False        # keep crunching until we are done
        rows_per_page = 100 # constant for the rows per page to query
        items = []          # the items we will return
        limit = 0           # max amount of items to be queried. to be determined after the first call

        body["row"] = rows_per_page
        last_id = None

        while not done:
            if last_id is not None:
                body["after_id"] = last_id

            data = await self._query(method, body=body)
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

            for element in elements:
                if filter is not None and filter(element):
                    continue
                item = element_processor(element)
                if item:
                    items.append(item)
                else:
                    raise Exception("we recently refactored the code and this case needs to be reavaluated")
                    # it is likely going to happen because we did not properly check if
                    # the item exists in the db already. Maybe we also made the assumption
                    # that it is okay to just throw new items here. Let's revisit our life decisions.

            num_items = len(items)
            self.logger.debug(num_items)

            if num_items >= limit:
                done = True

            last_id = last_id_deducer(elements[-1])
            self.logger.debug(f"Last ID: {last_id}")


        return items

    def _extrinsic_metadata_processor(self, raw_extrinsic_metadata):
        """
        Processes extrinsic metadata and stores it in the database.
        :param raw_extrinsic_metadata: raw extrinsic metadata
        :type raw_extrinsic_metadata: dict
        :return: The function that can be used to process an element in the list
        """
        if raw_extrinsic_metadata["account_display"] is not None:
            address = raw_extrinsic_metadata["account_display"]["address"]
        else:
            address = None

        extrinsic = Extrinsic(
            chain = self.chain,
            id = raw_extrinsic_metadata["extrinsic_index"],
            block_number = raw_extrinsic_metadata["block_num"],
            module = raw_extrinsic_metadata["call_module"].lower(),
            call = raw_extrinsic_metadata["call_module_function"].lower(),
            address = address,
            nonce = raw_extrinsic_metadata["nonce"],
            extrinsic_hash = raw_extrinsic_metadata["extrinsic_hash"],
            success = raw_extrinsic_metadata["success"],
            fee = raw_extrinsic_metadata["fee"],
            fee_used = raw_extrinsic_metadata["fee_used"],
            finalized = raw_extrinsic_metadata["finalized"],
        )

        self.db.write_item(extrinsic)
        return extrinsic

    def _event_metadata_processor(self, raw_event_metadata):
        """
        Processes event metadata and stores it in the database.
        :param raw_event_metadata: raw event metadata
        :type raw_event_metadata: dict
        :return: The function that can be used to process an element in the list
        """
            # block_number is the the string until the hyphen
        block_number = int(raw_event_metadata["event_index"].split("-")[0])
        event = Event(
            chain = self.chain,
            id=raw_event_metadata["event_index"],
            block_number=block_number,
            extrinsic_id=raw_event_metadata["extrinsic_index"],
            module=raw_event_metadata["module_id"].lower(),
            event=raw_event_metadata["event_id"].lower(),
            finalized=raw_event_metadata["finalized"],
        )

        self.db.write_item(event)
        return event

    def update_extrinsic_from_raw_extrinsic(self, extrinsic: Extrinsic, raw_extrinsic: dict):
        """
        Updates an extrinsic with the data from the raw extrinsic.
        
        :param extrinsic: The extrinsic to update
        :type extrinsic: Extrinsic
        :param raw_extrinsic: The raw extrinsic
        :type raw_extrinsic: dict
        """

        extrinsic.id = raw_extrinsic["extrinsic_index"]
        extrinsic.chain = self.chain
        extrinsic.block_number = raw_extrinsic["block_num"]
        extrinsic.module = raw_extrinsic["call_module"].lower()
        extrinsic.call = raw_extrinsic["call_module_function"].lower()
        if raw_extrinsic["account_display"] is not None:
            extrinsic.address = raw_extrinsic["account_display"]["address"]
        extrinsic.nonce = raw_extrinsic["nonce"]
        extrinsic.extrinsic_hash = raw_extrinsic["extrinsic_hash"]
        extrinsic.success = raw_extrinsic["success"]
        extrinsic.params = raw_extrinsic["params"]
        extrinsic.fee = raw_extrinsic["fee"]
        extrinsic.fee_used = raw_extrinsic["fee_used"]
        extrinsic.error = raw_extrinsic["error"]
        extrinsic.finalized = raw_extrinsic["finalized"]
        extrinsic.tip = raw_extrinsic["tip"]


    def update_event_from_raw_event(self, event: Event, raw_event: dict):
        """
        Updates an event with the data from the raw event.

        :param event: The event to update
        :type event: Event
        :param raw_event: The raw event
        :type raw_event: dict
        """

        # Subscan API is delivering the extrinsic id instead of the event id 
        # in the event_index field. So let's work around that.
        event.id = f'{raw_event["block_num"]}-{raw_event["event_idx"]}'
        event.chain = self.chain
        event.block_number = raw_event["block_num"]
        event.extrinsic_id = f'{raw_event["block_num"]}-{raw_event["extrinsic_idx"]}'
        event.module = raw_event["module_id"].lower()
        event.event = raw_event["event_id"].lower()
        event.params = raw_event["params"]
        event.finalized = raw_event["finalized"]

    async def fetch_extrinsic_metadata(self, module, call, config: ScrapeConfig) -> list:
        """
        Scrapes all extrinsics matching the specified module and call (like `utility.batchAll` or `system.remark`)

        :param module: extrinsic module to look for, like `system`, `utility`, etc
        :type module: str
        :param call: extrinsic module's specific 'call' or method, like system's `remark` call.
        :type call: str
        :param config: the `ScrapeConfig`
        :type config: ScrapeConfig
        :return: the extrinsics
        :rtype: list
        """
        self.logger.info(f"Fetching extrinsic {module}.{call} from {self.endpoint}")

        body = {"module": module, "call": call}
        if config.params is not None:
            body.update(config.params)

        items = await self._iterate_pages(
            self._api_method_extrinsics,
            self._extrinsic_metadata_processor,
            last_id_deducer=self._last_id_deducer,
            list_key="extrinsics",
            body=body,
            filter=config.filter
            )

        self.db.flush()

        if config.auto_hydrate == True:
            extrinsic_indexes = [e.id for e in items]
            items = await self.fetch_extrinsics(extrinsic_indexes)

        return items


    async def fetch_extrinsics(self, extrinsic_indexes: list, update_existing: bool = True) -> list:
        """
        Fetches the extrinsic with the specified index and writes it to the db.

        :param extrinsic_index: The extrinsix index to fetch
        :type extrinsic_index: str
        :param update_existing: Whether to update the extrinsic if it already exists in the db. Defaults to True.
        :type update_existing: bool
        :return: The extrinsics
        :rtype: list
        """

        items = []
        
        self.logger.info("Building list of extrinsics to fetch...")

        already_fetched_extrinsics = self.db.query_extrinsics(chain = self.chain, extrinsic_ids = extrinsic_indexes).all()
        already_fetched_extrinsic_ids = [e.id for e in already_fetched_extrinsics]

        # if we do not update existing items, we only need to fetch the ones that are not in the db
        if update_existing == False:
            extrinsic_indexes = already_fetched_extrinsic_ids

        self.logger.info(f"Fetching {len(extrinsic_indexes)} extrinsics from {self.endpoint}")

        method = self._api_method_extrinsic

        async with httpx.AsyncClient() as client:
            while len(extrinsic_indexes) > 0:
                # take up to 1000 extrinsics at a time
                batch = extrinsic_indexes[:1000]
                if len(batch) == 0:
                    break
                
                futures = []
                for extrinsic_index in batch:
                    body = {"extrinsic_index": extrinsic_index}
                    task = self._query(method, body=body, client=client)

                    self.logger.debug(f"Spawning task for {extrinsic_index}")
                    future = asyncio.ensure_future(task)
                    await asyncio.sleep(1/MAX_CALLS_PER_SEC)
                    futures.append(future)

                raw_extrinsics = await asyncio.gather(*futures)
                
                for raw_extrinsic in raw_extrinsics:
                    extrinsic_id = self._extrinsic_index_deducer(raw_extrinsic)
                    
                    if extrinsic_id in already_fetched_extrinsic_ids:
                        extrinsic = self.db.query_extrinsic(self.chain, extrinsic_id)
                    else:
                        extrinsic = Extrinsic()
                    self.update_extrinsic_from_raw_extrinsic(extrinsic, raw_extrinsic)

                    self.db.write_item(extrinsic)
                    items.append(extrinsic)

                self.db.flush()

                for index in batch:
                    extrinsic_indexes.remove(index)

        return items

    async def fetch_event_metadata(self, module, call, config) -> list:
        """
        Scrapes all events matching the specified module and call (like `utility.batchAll` or `system.remark`)

        :param module: extrinsic module to look for, like `system`, `utility`, etc
        :type module: str
        :param call: extrinsic module's specific 'call' or method, like system's `remark` call.
        :type call: str
        :param config: the `ScrapeConfig`
        :type config: ScrapeConfig
        :return: the events
        :rtype: list
        """
        items = []

        self.logger.info(f"Fetching events {module}.{call} from {self.endpoint}")

        body = {"module": module, self._api_method_events_call: call}
        if config.params is not None:
            body.update(config.params)

        items = await self._iterate_pages(
            self._api_method_events,
            self._event_metadata_processor,
            last_id_deducer=self._last_id_deducer,
            list_key="events",
            body=body,
            filter=config.filter
        )

        self.db.flush()

        if config.auto_hydrate == True:
            event_indexes = [e.id for e in items]
            items = await self.fetch_events(event_indexes)

        return items

    async def fetch_events(self, event_indexes: list, update_existing: bool = True) -> list:
        """
        Fetches the event with the specified index and writes it to the db.

        :param event_index: The event index to fetch
        :type event_index: str
        :param update_existing: Whether to update the event if it already exists in the db. Defaults to True.
        :type update_existing: bool
        :return: The events
        :rtype: list
        """

        items = []
        
        already_fetched_events = self.db.query_events(chain = self.chain, event_ids = event_indexes).all()
        already_fetched_event_ids = [e.id for e in already_fetched_events]

        # if we do not update existing items, we only need to fetch the ones that are not in the db
        if update_existing == False:
            event_indexes = already_fetched_event_ids

        self.logger.info(f"Fetching {len(event_indexes)} events from {self.endpoint}")

        method = self._api_method_event

        async with httpx.AsyncClient() as client:
            while len(event_indexes) > 0:
                # take up to 1000 extrinsics at a time
                batch = event_indexes[:1000]
                if len(batch) == 0:
                    break
                
                futures = []
                for event_index in batch:
                    body = {"event_index": event_index}
                    task = self._query(method, body=body)
                    
                    self.logger.debug(f"Spawning task for {event_index}")
                    future = asyncio.ensure_future(task)
                    await asyncio.sleep(1/MAX_CALLS_PER_SEC)
                    futures.append(future)

                raw_events = await asyncio.gather(*futures)

                for raw_event in raw_events:
                    event_id = self._event_index_deducer(raw_event)
                    
                    if event_id in already_fetched_event_ids:
                        event = self.db.query_event(self.chain, event_id)
                    else:
                        event = Event()
                    self.update_event_from_raw_event(event, raw_event)

                    self.db.write_item(event)
                    items.append(event)

                self.db.flush()

                for id in batch:
                    event_indexes.remove(id)

        return items
