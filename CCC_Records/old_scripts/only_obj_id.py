# API Documentation: 
# Swagger: https://catalog.archives.gov/api/v2/api-docs/#/
# Github: https://github.com/usnationalarchives/Catalog-API/tree/master

import requests
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from apikeys import nara_key

url = "https://catalog.archives.gov/api/v2/records/search/"
headers = {"x-api-key": nara_key}
params = {
    "q": '"Kings Mountain" AND "Co 4479"',
    "rows": 10
}

response = requests.get(url, headers=headers, params=params)

data = response.json()
object_ids = [
    obj.get("objectId")
    for hit in data.get("body", {}).get("hits", {}).get("hits", [])
    for obj in (hit.get("_source", {}).get("record", {}).get("digitalObjects", []) or [])
    if obj.get("objectId")
]

with open("results.json", "w") as file:
    json.dump(object_ids, file, indent=2)
