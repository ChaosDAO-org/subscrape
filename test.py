from subscrape.db.subscrape_db import SubscrapeDB

db = SubscrapeDB("data/parachains/kusama_")
db.set_active_extrinsics_call("crowdloan_add_memo")
db.set_extrinsic("crowdloan_add_memo", "10987654-12", {"foo": "bar"})
db.flush_extrinsics()