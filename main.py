import json
import requests
from datetime import datetime
import logging
import os

import http.client
http.client.HTTPConnection.debuglevel = 1

# Setup
## Logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger()
log.debug("Hello World!")

requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

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
  # TE: there seems to be an issue with the way the requests library handles the server response
  # the request will only conclude successfully after it timed out. one possible reason could be
  # that the server is sending no content-length header. I tried adding the timeout param and it
  # forces a faster timeout and successful conclusion of the request.
  # Possibly related discussion: https://github.com/psf/requests/issues/4023
  response = requests.post(url, headers = headers, data = body, timeout=1)
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