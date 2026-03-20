# NARA Proxy Endpoint Analysis

## Summary

The proxy endpoint at `/proxy/extractedText/` returns **HTML instead of JSON** for objectIds that **don't have extracted text available on NARA's server**. This is not a bug in your code — it's a server-side fallback behavior.

## Root Cause

When NARA's backend cannot find extracted text data for a given `objectId`, the server returns the **Catalog SPA (Single Page Application) HTML shell** instead of a proper JSON error response. The response still comes back as **HTTP 200** but with `Content-Type: text/html` instead of `application/json`.

> [!IMPORTANT]
> This means your existing code's HTML detection (line 49 of [fetch_extracted_text.py](file:///w:/RA_work_folders/Ashton_Reed/ra_work_folder/CCC_Records/fetch_extracted_text.py)) is working correctly — it catches these cases and retries, but retries will never succeed because the data simply doesn't exist.

## Data Availability Test Results

Tested all objectIds from `529913495` to `529913542` (48 total):

| Result | Count | objectIds |
|--------|-------|-----------|
| ✅ JSON (has text) | **8** | 529913498, 529913499, 529913507, 529913512, 529913521, 529913526, 529913536, 529913541 |
| ❌ HTML (no text) | **40** | All others in the range |

**Only ~17% of objects in this range have extracted text available.**

## Direct API Alternative?

### NARA API v2 (`/api/v2/`)
- **Requires an API key** (request from `Catalog_API@nara.gov`)
- The v2 API provides metadata search/export, but **extracted text is only available through the `/proxy/extractedText/` endpoint**
- Without an API key, the v2 endpoint also returns the SPA HTML shell
- API docs: `https://catalog.archives.gov/api/v2/api-docs` (also requires browser/JS to render)

### Bottom Line
**There is no alternative direct API endpoint for extracted text.** The `/proxy/extractedText/` endpoint is the only way to get this data. The v2 API exposes record metadata but not the extracted text content directly.

## Recommendations for [fetch_extracted_text.py](file:///w:/RA_work_folders/Ashton_Reed/ra_work_folder/CCC_Records/fetch_extracted_text.py)

1. **Detect HTML on first attempt and skip** — don't waste retries on objectIds that will never return data
2. **Log which objectIds have no text** — so you know the actual coverage
3. **Optionally pre-discover valid objectIds** — by checking all objectIds once without retries

```diff
-            if "html" in content_type.lower():
-                raise ValueError(f"Received HTML instead of JSON (likely a redirect or temporary error)")
+            if "html" in content_type.lower():
+                return "[NO_EXTRACTED_TEXT_AVAILABLE]"  # Data doesn't exist on NARA's side
```
