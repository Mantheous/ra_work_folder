import requests
import time
import os
import re

BASE_URL = "https://catalog.archives.gov/proxy/extractedText/529913494"
OBJECT_ID_START = 529913512
OBJECT_ID_END = 529913542
OUTPUT_FILE = "extracted_text.txt"

MAX_RETRIES = 4
INITIAL_RETRY_DELAY = 5  # Base delay for exponential backoff

# Browser-like headers to reduce likelihood of being blocked/rate-limited
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://catalog.archives.gov/"
}

def get_already_fetched_ids():
    """Returns a set of object IDs that have already been successfully fetched."""
    if not os.path.exists(OUTPUT_FILE):
        return set()
    
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        content = f.read()
        # Look for successful blocks: objectId headers that aren't followed by [FAILED]
        # This is a bit naive but works for this specific file format
        matches = re.findall(r"objectId: (\d+)", content)
        failed = re.findall(r"\[FAILED after .* for objectId=(\d+)\]", content)
        return set(matches) - set(failed)

def fetch_extracted_text(object_id: int, session: requests.Session) -> str:
    base_fetch_url = f"{BASE_URL}?objectId={object_id}"
    delay = INITIAL_RETRY_DELAY
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Add cache buster on retries
            url = base_fetch_url
            if attempt > 1:
                url += f"&_t={int(time.time() * 1000)}"
                
            response = session.get(url, headers=HEADERS, timeout=30)
            
            # Check if we got HTML instead of JSON (common failure mode for this API)
            content_type = response.headers.get("Content-Type", "")
            if "html" in content_type.lower():
                raise ValueError(f"Received HTML instead of JSON (likely a redirect or temporary error)")
            
            response.raise_for_status()
            data = response.json()
            digital_objects = data.get("digitalObjects", [])
            
            if digital_objects:
                return digital_objects[0].get("extractedText", "[No extractedText field found]")
            return "[No digitalObjects in response]"
            
        except (requests.RequestException, ValueError) as e:
            print(f"    [Attempt {attempt}/{MAX_RETRIES}] Error for objectId={object_id}: {e}")
            if attempt < MAX_RETRIES:
                print(f"    Retrying in {delay}s (Exponential backoff)...")
                time.sleep(delay)
                delay *= 2  # Double the wait time for the next attempt
    
    return f"[FAILED after {MAX_RETRIES} attempts for objectId={object_id}]"

def main():
    object_ids = list(range(OBJECT_ID_START, OBJECT_ID_END + 1))
    fetched_ids = get_already_fetched_ids()
    
    print(f"Total IDs to process: {len(object_ids)}")
    print(f"Already fetched: {len(fetched_ids)}")
    
    # We'll use "a" (append) mode to preserve previous work
    # If the file doesn't exist, it will be created.
    with requests.Session() as session, open(OUTPUT_FILE, "a", encoding="utf-8") as out:
        for i, object_id in enumerate(object_ids, start=1):
            if str(object_id) in fetched_ids:
                print(f"  [{i}/{len(object_ids)}] Skipping objectId={object_id} (Already fetched)")
                continue
            print("Waiting 1 a second...")
            time.sleep(1)

            print(f"  [{i}/{len(object_ids)}] Fetching objectId={object_id}...")
            text = fetch_extracted_text(object_id, session)

            out.write(f"{'=' * 60}\n")
            out.write(f"Record {i} of {len(object_ids)}  |  objectId: {object_id}\n")
            out.write(f"{'=' * 60}\n\n")
            out.write(text.strip())
            out.write("\n\n")
            out.flush() # Ensure it's written immediately in case of crash

    print(f"Done. Output updated in '{OUTPUT_FILE}'.")

if __name__ == "__main__":
    main()
