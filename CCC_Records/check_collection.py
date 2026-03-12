import requests
import json
import sys

# Target NARA ID
nara_id = "12034477"
url = f"https://catalog.archives.gov/api/v2/records"
params = {
    "naId": nara_id
}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json"
}

print(f"Fetching metadata for NARA ID {nara_id}...")
response = requests.get(url, params=params, headers=headers)
print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    try:
        data = response.json()
        with open("collection_meta.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print("Saved to collection_meta.json")
        
        # Try to extract useful info
        if "body" in data and "hits" in data["body"] and "hits" in data["body"]["hits"]:
            hits = data["body"]["hits"]["hits"]
            if len(hits) > 0:
                record = hits[0]["_source"]["record"]
                
                # Look for digitalObjects count
                if "digitalObjects" in record:
                    digital_objects = record["digitalObjects"]
                    print(f"Found {len(digital_objects)} digital objects in the main record.")
                else:
                    print("No digitalObjects array found in the main record.")
                
                # Check for child records or items
                print("Checking for descendants/child records...")
                # We can do a search for records where parent is this ID
                child_params = {
                    "parentNaId": nara_id,
                    "limit": 1
                }
                child_resp = requests.get(url, params=child_params, headers=headers)
                if child_resp.status_code == 200:
                    child_data = child_resp.json()
                    total_children = child_data.get("body", {}).get("hits", {}).get("total", {}).get("value", 0)
                    print(f"Total descendant records found: {total_children}")
                else:
                    print(f"Failed to query children: {child_resp.status_code}")
                
    except Exception as e:
        print(f"Failed to parse or process JSON: {e}")
else:
    print(f"Error: {response.text[:500]}")
