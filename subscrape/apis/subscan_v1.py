from subscrape.apis.subscan_base import SubscanBase
from subscrape.db.subscrape_db import SubscrapeDB

class SubscanV1(SubscanBase):
    def __init__(self, chain, db: SubscrapeDB, subscan_key):
        super().__init__(chain, db, subscan_key)
        self._extrinsic_index_deducer = lambda ex: f"{ex['extrinsic_index']}"
        self._events_index_deducer = lambda ex: f"{ex['block_num']}-{ex['event_idx']}"
        self._event_index_deducer = lambda ex: f"{ex['block_num']}-{ex['event_idx']}"
        self._transfers_index_deducer = lambda e: f"{e['block_num']}-{e['event_idx']}"
        self._last_id_deducer = lambda e: None
        self._last_transfer_id_deducer = lambda e: None
        self._api_method_extrinsics = "/api/scan/extrinsics"
        self._api_method_extrinsic = "/api/scan/extrinsic"
        self._api_method_events = "/api/scan/events"
        self._api_method_event = "/api/scan/event"
        self._api_method_transfers = "/api/scan/transfers"
        self._api_method_events_call = "call"

    # iterates through all pages until it processed all elements
    # or gets False from the processor
    async def _iterate_pages(
        self,
        method, 
        element_processor, 
        list_key, 
        last_id_deducer=None,
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
        :param last_id_deducer: not used, only for forward compatibility
        :type last_id_deducer: function or None
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
                self.logger.debug("We did not find any new elements on the latest page. Stopping.")
                break
            else:
                self.logger.debug(f"Found {count_new_elements} new elements on page {page}.")
                pass

            # update counters and check if we should exit
            count += count_new_elements
            self.logger.debug(count)

            if count >= limit:
                done = True

            page += 1

        return count