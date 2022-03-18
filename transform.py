import csv
import json
from subscrape.db.subscrape_db import SubscrapeDB
from substrateinterface.utils import ss58
# pip install substrate-interface

rows = [["type", "index", "account_id", "value", "referral"]]

db = SubscrapeDB("Kusama")

def unwrap_params(params):
    result = {}
    for param in params:
        name = param["name"]
        value = param["value"]
        result[name] = value
    return result


def fetch_direct_contributions():
    extrinsics = db.extrinsics_iter("crowdloan", "contribute")

    for index, extrinsic in extrinsics:
        extrinsic = json.loads(extrinsic)
        params = json.loads(extrinsic["params"])
        params = unwrap_params(params)
        if params["index"] == 2110:
            account_id = extrinsic["account_id"]
            value = params["value"]
            row = ["direct", index, account_id, value]
            rows.append(row)
            #direct_contributions[index] = extrinsic
    #return direct_contributions

def fetch_batch_contributions():
    extrinsics = db.extrinsics_iter("utility", "batch_all")

    for index, extrinsic in extrinsics:
        extrinsic = json.loads(extrinsic)
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
                            referral = ss58.ss58_encode(f"0x{memo}", ss58_format=42)
                        else:
                            referral = json.dumps(memo_call)
                        account_id = extrinsic["account_id"]
                        value = contribute_call["params"][1]["value"]
                        #public_key = ss58.ss58_decode(address)
                        #ksm_address = ss58.ss58_encode(public_key, ss58_format=2)
                        row = ["memo", index, account_id, value, referral]
                        rows.append(row)
        else:
            pi = 3


fetch_direct_contributions()
fetch_batch_contributions()
file_path = "data/transforms/transform.csv"
with open(file_path, "w", newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(rows)


# Archived code below


# recursively goes through batched calls to check for an actual hit
def process_batch_hit(extrinsic):
    params = extrinsic["params"]
    if type(params) is str:
        params = json.loads(params)
    assert(len(params) == 1)
    calls = params[0]["value"]
    
    # check for empty batch
    if calls is None:
        return

    for call in calls:
        actual_call_module = call["call_module"].lower()
        actual_call_name = call["call_name"].lower()
        if actual_call_module == call_module and actual_call_name == call_name:
            return process_extrinsic_hit(extrinsic)
        elif actual_call_module == "utility" and (actual_call_name == "batch" or actual_call_name == "batch_all"):
            return process_batch_hit(call)               
    return True