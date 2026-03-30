# Download source images for Ladislaus Giza (pages 3-16) from NARA catalog.
# Reads config from Ladislaus_Giza.json (objectId, start_page, end_page).
# Uses the NARA v2 API to get S3 image URLs, then downloads each page as a TIFF.
# Output goes to: CCC_Records/demo/source_images/

import json
import sys
import time
import requests
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_FILE = SCRIPT_DIR / "Ladislaus_Giza.json"
OUTPUT_DIR  = SCRIPT_DIR / "source_images"

sys.path.insert(0, str(SCRIPT_DIR.parent.parent))   # project root for apikeys
from apikeys import nara_key

API_URL = "https://catalog.archives.gov/api/v2/records/search"
HEADERS  = {"x-api-key": nara_key}

MAX_RETRIES     = 4
INITIAL_BACKOFF = 3   # seconds (doubles on each retry)
INTER_REQUEST_DELAY = 0.5   # polite pause between downloads

# ---------------------------------------------------------------------------
# Load config
# ---------------------------------------------------------------------------
with open(CONFIG_FILE, encoding="utf-8") as f:
    config = json.load(f)

na_id      = config["objectId"]
start_page = config["start_page"]
end_page   = config["end_page"]

print(f"Config: naId={na_id}, pages {start_page}–{end_page}")

# ---------------------------------------------------------------------------
# Fetch the full digital-object list from the NARA API
# ---------------------------------------------------------------------------
print(f"\nQuerying NARA API for naId={na_id} …")
resp = requests.get(API_URL, headers=HEADERS, params={"naId": na_id, "rows": 1}, timeout=30)
resp.raise_for_status()
data = resp.json()

hits = data["body"]["hits"]["hits"]
if not hits:
    sys.exit(f"ERROR: No record found for naId={na_id}")

digital_objects = hits[0]["_source"]["record"]["digitalObjects"]
print(f"Found {len(digital_objects)} digital objects for this record.")

# Pages in the NARA response are 1-indexed and ordered sequentially.
# Slice out the pages we want (start_page and end_page are 1-indexed, inclusive).
selected = digital_objects[start_page - 1 : end_page]   # list slice is exclusive at end
print(f"Downloading pages {start_page}–{end_page} ({len(selected)} files) …\n")

# ---------------------------------------------------------------------------
# Download each selected page
# ---------------------------------------------------------------------------
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

for i, obj in enumerate(selected, start=start_page):
    url      = obj["objectUrl"]
    filename = obj["objectFilename"]
    out_path = OUTPUT_DIR / filename

    if out_path.exists():
        print(f"  [page {i:02d}] SKIP  {filename} (already exists)")
        continue

    delay = INITIAL_BACKOFF
    success = False
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"  [page {i:02d}] Downloading {filename} …", end=" ", flush=True)
            dl = requests.get(url, timeout=60, stream=True)
            dl.raise_for_status()

            with open(out_path, "wb") as f:
                for chunk in dl.iter_content(chunk_size=1024 * 256):
                    f.write(chunk)

            size_mb = out_path.stat().st_size / (1024 * 1024)
            print(f"OK ({size_mb:.1f} MB)")
            success = True
            break

        except requests.RequestException as e:
            print(f"FAILED (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                print(f"           Retrying in {delay}s …")
                time.sleep(delay)
                delay *= 2

    if not success:
        print(f"  [page {i:02d}] GAVE UP on {filename} after {MAX_RETRIES} attempts.")

    time.sleep(INTER_REQUEST_DELAY)

print(f"\nDone. Files saved to: {OUTPUT_DIR}")
