import requests
import json

url = "https://catalog.archives.gov/api/v2/api-docs"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json"
}

print("Fetching Swagger docs...")
response = requests.get(url, headers=headers)
print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    try:
        data = response.json()
        with open("swagger.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print("Saved to swagger.json")
    except Exception as e:
        print(f"Failed to parse JSON: {e}")
        print("Response text (first 1000 chars):")
        print(response.text[:1000])
else:
    print(f"Error: {response.text}")
