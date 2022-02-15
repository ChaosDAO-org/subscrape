import json
import requests

endpoint = "https://kusama.api.subscan.io"

def query(method, headers = {}, body = {}):
  url = endpoint + method
  headers["Content-Type"] = "application/json"
  body = json.dumps(body)
  response = requests.post(url, headers = headers, data = body)
  #print(response.headers)
  return response.text


response = query("/api/scan/transfers", body= {"row": 1, "page": 1})

print(response)

obj = json.loads(response)