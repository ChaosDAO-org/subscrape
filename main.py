import json
import requests
from datetime import datetime
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger()

log.debug("Hello World!")

endpoint = "https://kusama.api.subscan.io"

def query(method, headers = {}, body = {}):
  url = endpoint + method
  headers["Content-Type"] = "application/json"
  body = json.dumps(body)
  before = datetime.now()
  response = requests.post(url, headers = headers, data = body)
  after = datetime.now()
  log.debug("request took: " + str(after - before))
  log.info(response.headers)
  return response.text


api_key = None
request_rate = 0.5 # a call every 2 seconds

for i in range(1):
    response = query("/api/scan/transfers", body= {"row": 1, "page": 1})
    log.info(response)
    obj = json.loads(response)