
import requests
import json

def query_ccc_company(company_name,):
    # NARA Catalog API v2 search endpoint
    url = "https://catalog.archives.gov/api/v2/records/search/"
    
    # Constructing a boolean search string
    query_string = f'"{company_name}"'
    
    # API Parameters (limiting to 100 results for the initial pull)
    params = {
        'q': query_string,
        'limit': 100
    }
    
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': 'U4dyklgi89adWiuQ4hbko3D018K35o7G4yMIjKv9' # Uncomment and paste your key once approved
    }
    
    print(f"Querying NARA API for: {query_string}...\n")
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  
        
        data = response.json()
        
        # NARA v2 API nests the search results inside body -> hits -> hits
        results = data.get('body', {}).get('hits', {}).get('hits', [])
        
        if not results:
            print("No catalog records found matching your query.")
            return
            
        print(f"Found {len(results)} catalog entries. Extracting metadata...\n")
        
        with open("results.json", "w") as file:
            json.dump(results, file, indent=2)
        # for item in results:
        #     source = item.get('_source', {})
        #     record = source.get('record', {})
            
        #     # Extracting key metadata points
        #     na_id = source.get('id', 'Unknown ID')
        #     title = record.get('title', 'No Title Provided')
            
        #     # Scope and Content notes often contain the richest historical summaries
        #     scope = record.get('scopeAndContentNote', 'No scope or content description available.')
            
        #     print(f"--- Record NAID: {na_id} ---")
        #     print(f"Title: {title}")
        #     print(f"Description: {scope[:250]}...\n")
            
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while connecting to the API: {e}")

if __name__ == "__main__":
    query_ccc_company("Co. 3560")