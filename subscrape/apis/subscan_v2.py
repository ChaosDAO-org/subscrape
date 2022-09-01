from subscrape.apis.subscan_base import SubscanBase
from subscrape.db.subscrape_db import SubscrapeDB

class SubscanV2(SubscanBase):
    def __init__(self, chain, db: SubscrapeDB, subscan_key):
        super().__init__(chain, db, subscan_key)
        self._extrinsic_index_deducer = lambda ex: ex["extrinsic_index"]
        self._events_index_deducer = lambda ex: f"{ex['event_index']}"
        self._event_index_deducer = lambda ex: f"{ex['block_num']}-{ex['event_idx']}"
        self._api_method_extrinsics = "/api/v2/scan/extrinsics"
        self._api_method_extrinsic = "/api/scan/extrinsic"
        self._api_method_events = "/api/v2/scan/events"
        self._api_method_event = "/api/scan/event"
        self._api_method_transfers = "/api/v2/scan/transfers"
        self._api_method_events_call = "event_id"