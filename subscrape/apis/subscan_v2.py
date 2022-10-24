from subscrape.apis.subscan_base import SubscanBase
from subscrape.db.subscrape_db import SubscrapeDB

class SubscanV2(SubscanBase):
    def __init__(self, chain, db: SubscrapeDB, subscan_key):
        super().__init__(chain, db, subscan_key)
        self._extrinsic_index_deducer = lambda e: e["extrinsic_index"]
        self._events_index_deducer = lambda e: f"{e['event_index']}"
        self._event_index_deducer = lambda e: f"{e['block_num']}-{e['event_idx']}"
        self._transfers_index_deducer = lambda e: f"{e['block_num']}-{e['event_idx']}"
        self._last_id_deducer = lambda e: e["id"]
        self._last_transfer_id_deducer = lambda e: [e["block_num"], e["event_idx"]]
        self._api_method_extrinsics = "/api/v2/scan/extrinsics"
        self._api_method_extrinsic = "/api/scan/extrinsic"
        self._api_method_events = "/api/v2/scan/events"
        self._api_method_event = "/api/scan/event"
        self._api_method_transfers = "/api/v2/scan/transfers"
        self._api_method_events_call = "event_id"


    # iterates through all pages until it processed all elements
    # or gets False from the processor
    async def _iterate_pages(
        self,
        method, 
        element_processor, 
        list_key,
        last_id_deducer,
        body={}, 
        filter=None) -> int:
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
        :return: number of items processed
        """

        done = False        # keep crunching until we are done
        rows_per_page = 100 # constant for the rows per page to query
        count = 0           # counter for how many items we queried already
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

            count_new_elements = 0
            for element in elements:
                if filter is not None and filter(element):
                    continue
                was_new_element = element_processor(element)
                if was_new_element:
                    count_new_elements += 1
                else:
                    done = True
                    break

            # update counters and check if we should exit
            count += count_new_elements
            self.logger.debug(count)

            if count >= limit:
                done = True

            self.logger.debug(f"Last ID: {last_id}")

            last_id = last_id_deducer(elements[-1])

        return count