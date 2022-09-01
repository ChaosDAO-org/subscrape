from subscrape.apis.subscan_base import SubscanBase
from subscrape.db.subscrape_db import SubscrapeDB

class SubscanV1(SubscanBase):
    def __init__(self, chain, db: SubscrapeDB, subscan_key):
        super().__init__(chain, db, subscan_key)
        self._extrinsic_index_deducer = lambda ex: f"{ex['extrinsic_index']}"
        self._events_index_deducer = lambda ex: f"{ex['block_num']}-{ex['event_idx']}"
        self._api_method_extrinsics = "/api/scan/extrinsics"
        self._api_method_extrinsic = "/api/scan/extrinsic"
        self._api_method_events = "/api/scan/events"
        self._api_method_transfers = "/api/scan/transfers"
        self._api_method_events_call = "call"
