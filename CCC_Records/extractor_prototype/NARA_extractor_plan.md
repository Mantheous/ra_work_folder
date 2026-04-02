# National Archives Record Extractor

## Overview
An automated pipeline to filter, extract, and process Civilian Conservation Corps (CCC) records from NARA. The system follows these steps:
1. **Index**: Query NARA API to identify record groups and digital objects.
2. **Extract**: Fetch OCR text using asynchronous calls for high throughput.
3. **Store**: Use SQLite to track progress (checkpointing) and store raw text.
4. **Segment**: Group text by person using markers and file unit boundaries.
5. **Parse**: Extract genealogical and company data using NLP.
6. **Export**: Provide a searchable JSON file or database interface.

## API Call & Indexing
Use the api key in `apikeys.py` to make an API call to NARA. 
* **Reliability**: If there is a network error, log it and retry the call after a delay. 
* **Flexibility**: Support user-defined search queries and broad searches. 
* **Examples**: A search for "Kings Mountain" and "Co 4479" or a search for all Civilian Conservation Corps records. 
* **Strategy**: Implement offset-based pagination to retrieve *all* hits for the query.
* **Tracking**: Save all NAIDs and ObjectIDs to a local SQLite database to prevent redundant API calls.

## Collect IDs
`CCC_Records\results.json` contains a sample of a search result for "Kings Mountain" and "Co 4479".
* **Sample ID**: The first object ID that shows up in the sample results is `_id: 529913494`. 
* **Retrieval**: Every page (Digital Object) that is a hit in the search results must be retrieved. This often requires multiple API calls to handle all hits within a record group.

## Extract Text
Text extraction is done via the NARA proxy URL. To handle the high volume of pages efficiently, we use asynchronous requests (`aiohttp`).
```python
url = f"https://catalog.archives.gov/proxy/extractedText/{naid}?objectId={object_id}"
```
* **Checkpointing**: Mark each `object_id` in SQLite as "fetched" after success. If the process is interrupted, the script will automatically resume from the last un-fetched ID.

## Process Text & Segmentation
The raw extracted text must be grouped by person. 
* **Separator**: Cases are often divided by a page that says "NEXT CASE BEGINS". 
* **Logic**: A new case starts at the beginning of a File Unit (`naid`) or whenever the specific separator text is detected. 
* **Management**: All pages belonging to a single case are stored together to provide context for NLP.

## Collect Data
Use NLP to extract data from the grouped text blocks. 
* **Targets**: The most important data to collect is genealogical:
    * Full Name
    * CCC Enrollee ID
    * Home Town/State
    * Date of Enlistment / Discharge
    * **Nearest Kin** (Highly useful for genealogical links).
* **Other Data**: Information about the assigned company (e.g., "Co. 4479") and other contextual data is also captured.

## Store & Access Data
The final processed data is stored in the local SQLite database for speed and robustness.
* **Search Script**: Create a Python script that can search the database/JSON for specific fields (name, company, date).
* **JSON Export**: Maintain the ability to export the structured results to a JSON file for compatibility.