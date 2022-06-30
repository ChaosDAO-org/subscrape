import csv
import json
from subscrape.db.subscrape_db import SubscrapeDB
from substrateinterface.utils import ss58
import logging
import subscrape

config = {
    "kusama":{
        "extrinsics":{
            "utility": ["batch_all"],
            "crowdloan": ["contribute"]
        }
    }
}

created_header = False
interesting_rows = ["block_timestamp", "block_num", "extrinsic_index", "account_id", "account_index", "nonce", "success", "fee"]
rows =  [["type", "value", "referral"]]
rows[0].extend(interesting_rows)

db = SubscrapeDB("Kusama")


def unwrap_params(params):
    result = {}
    for param in params:
        name = param["name"]
        value = param["value"]
        result[name] = value
    return result


def extract_interesting_extrinsic_properties(extrinsic):
    result = []
    for key in interesting_rows:            
        result.append(extrinsic[key])
    return result


def fetch_direct_contributions():
    extrinsics_storage = db.storage_manager_for_extrinsics_call("crowdloan", "contribute")
    extrinsics = extrinsics_storage.extrinsics_iter()

    for index, extrinsic in extrinsics:
        params = json.loads(extrinsic["params"])
        params = unwrap_params(params)
        if params["index"] == 2110 and extrinsic["success"] == True:
            #account_id = extrinsic["account_id"]
            value = params["value"]
            row = ["direct", value, ""]
            row.extend(extract_interesting_extrinsic_properties(extrinsic))
            rows.append(row)


def fetch_batch_contributions():
    extrinsics_storage = db.storage_manager_for_extrinsics_call("utility", "batch_all")
    extrinsics = extrinsics_storage.extrinsics_iter()

    for index, extrinsic in extrinsics:
        params = json.loads(extrinsic["params"])
        params = unwrap_params(params)
        calls = params["calls"]
        if calls is not None:
            for call in calls:
                if call["call_module"] == "Crowdloan" and call["call_name"] == "contribute":
                    if call["params"][0]["value"] == 2110:
                        assert(len(calls) == 2) # make sure we are not missing anything
                        contribute_call = calls[0]
                        assert(call == contribute_call)
                        memo_call = calls[1]
                        if memo_call["call_module"] == "Crowdloan" and memo_call["call_name"] == "add_memo":
                            memo = memo_call["params"][1]["value"]
                            referral = ss58.ss58_encode(f"0x{memo}", ss58_format=2)
                        else:
                            referral = json.dumps(memo_call)
                        value = contribute_call["params"][1]["value"]
                        #public_key = ss58.ss58_decode(address)
                        #ksm_address = ss58.ss58_encode(public_key, ss58_format=2)
                        row = ["batch", value, referral]
                        row.extend(extract_interesting_extrinsic_properties(extrinsic))
                        rows.append(row)
        else:
            pi = 3


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("scraping")
    subscrape.scrape(config)
    logging.info("transforming")
    fetch_direct_contributions()
    fetch_batch_contributions()
    file_path = "data/transforms/transform.csv"
    with open(file_path, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(rows)


if __name__ == "__main__":
    main()
