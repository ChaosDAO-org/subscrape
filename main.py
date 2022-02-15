import json
import requests
from datetime import datetime
import logging
import os

# Setup
## Logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger()
log.debug("Hello World!")

## Load Subscan API Key
api_key = None
if os.path.exists("subscan-key"):
    f = open("subscan-key")
    api_key = f.read()
log.debug(api_key)

# Methods

def query(url, headers = {}, body = {}):
  headers["Content-Type"] = "application/json"
  headers["x-api-key"] = api_key
  body = json.dumps(body)
  before = datetime.now()
  response = requests.post(url, headers = headers, data = body)
  after = datetime.now()
  log.debug("request took: " + str(after - before))
  log.info(response.headers)
  return response.text

# Main

endpoint = "https://kusama.api.subscan.io"
method = "/api/scan/transfers"
url = endpoint + method

for i in range(1):
    response = query(url, body= {"row": 1, "page": 1})
    log.info(response)
    obj = json.loads(response)