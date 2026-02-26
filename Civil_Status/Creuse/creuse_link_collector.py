# Scraper for "https://archives.creuse.fr/rechercher/archives-numerisees/registres-paroissiaux-et-de-letat-civil?arko_default_6089614c0e9ed--ficheFocus="
# As of Febuary 2026

# Creuse only shows 24 results on page 7. Thus trust the page count over the number of records
# copied to the CSV.

from playwright.sync_api import sync_playwright, expect # type: ignore
import sys
sys.path.append("W:\\RA_work_folders\\Ashton_Reed")
from Utilities.notifier import notify
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import re

# Global variables for tracking progress

# This is not the origial link. This is a filtered link to finish off the scraper
root_link = 'https://archives.creuse.fr/rechercher/archives-numerisees/registres-paroissiaux-et-de-letat-civil?arko_default_6089614c0e9ed--ficheFocus='
csv_location = "Creuse_Civil_Status/Creuse.csv"
tries = 0
max_tries = 5
results_per_page = 25
STARTING_PAGE = 399 # starts at 1, or you can set it to the page to resume on
STARTING_INDEX = 0
DEBUG = False
collumn_numbers = {
    "commune": 0,
    "cote": 1,
    "act_types": 2,
    "period": 3,
    "image_count": 4,
}

def url_for_page(base_url, page_number, results_per_page=25) -> str:
    # 1. Extract the hex ID (the 'funny string')
    # This regex looks for 'arko_default_' followed by alphanumeric characters
    match = re.search(r'arko_default_([a-z0-9]+)', base_url)
    if not match:
        raise ValueError("Could not find the Arko ID in the base URL.")
    
    arko_id = match.group(1)
    prefix = f"arko_default_{arko_id}"
    
    # 2. Calculate the 'from' offset
    # Page 1 = 0, Page 2 = 25, Page 3 = 50
    record_offset = (page_number - 1) * results_per_page
    
    # 3. Construct the query parameters from scratch
    query_params = {
        f"{prefix}--ficheFocus": "",
        f"{prefix}--from": str(record_offset),
        f"{prefix}--resultSize": str(results_per_page)
    }
    
    # 4. Strip the old query from the URL and attach the new one
    parsed_url = urlparse(base_url)
    new_query = urlencode(query_params)
    
    # Rebuild the URL using the original path but our fresh query
    return urlunparse(parsed_url._replace(query=new_query))

# TODO: Add this function to the base scraper.
def jump_to_page(target_page: int):
    page.goto(url_for_page(root_link, target_page))

def click_terms():
    # accept terms and conditions.
    page.locator("button[data-cy='accept-license']").click()
    page.wait_for_load_state('networkidle')

def get_link(row) -> str:
    # click on the eyeball button
    row.locator("td").nth(collumn_numbers["image_count"]).locator("button").click()
    page.wait_for_load_state('networkidle')

    try:
        # click on the three dots button
        page.locator("button[data-cy='btn-toggle-volet']").click()
    except:
        click_terms() # try clicking the terms and conditions
        page.locator("button[data-cy='btn-toggle-volet']").click()
    
    page.mouse.move(0, 0) # the tooltip sometimes covers the button

    # click on the share button
    page.locator('button[title="Partage et impression"]').click()
    href = page.locator('a.exporter').get_attribute("href")
    return str(href)

def process_row(i: int):
    try:
        wait_for_load()
        table_body = page.locator('tbody')
        row = table_body.locator('tr').nth(i)
        # Copy down some data from the table
        commune = row.locator("td").nth(collumn_numbers["commune"]).inner_text()
        period = row.locator("td").nth(collumn_numbers["period"]).inner_text()
        cote = row.locator("td").nth(collumn_numbers["cote"]).inner_text()
        act_types = row.locator("td").nth(collumn_numbers["act_types"]).inner_text()
        image_count = int(row.locator("td").nth(collumn_numbers["image_count"]).inner_text().split()[0][1:])
        href = get_link(row)

    except:
        # Attempt to recover from timeout error
        # pass
        global tries
        print(f"timeout processing row... try {tries}...")
        tries += 1
        if tries > max_tries:
            notify(message="Creuse scraper failed to process row after max tries", subject="Creuse Scraper Error")
            print("exiting...")
            exit()
        jump_to_page(page_number)
        process_row(i)
        # tries = 0
        return

    row_data = f"{cote}|{commune}|{period}|{act_types.replace('\n', ', ')}|{image_count}|{href}"
    with open(csv_location, "a", encoding="utf-8") as f:
        f.write(row_data + "\n")

    # go back to the results page
    page.wait_for_load_state('networkidle')
    page.locator('button.close').click()

def start_scrapping_on(starting_page: int, starting_row: int):
    jump_to_page(starting_page)
    scrape_page(starting_page, starting_row)
    next_page(starting_page)
        

def next_page(page_number: int):
    page_number+=1
    nav_buttons = page.locator('nav.pagination_haute').locator("ul")
    nav_buttons.get_by_text(str(page_number)).click()
    wait_for_load()

def count_rows() -> int:
    # page.locator('//*[@id="select_nombre_resultats"]').first.select_option(label=f"{results_per_page}") # set results per page every time just in case the site resets it
    wait_for_load()
    table_body = page.locator('tbody')
    row_count = table_body.locator("tr").count()
    return row_count

def scrape_page(page_number: int, starting_row: int = 0):
    print(f"Page {page_number}:")
    row_count = count_rows()
    if DEBUG:
        print(f"Page {page_number} | Row {1}/{row_count}")
        process_row(0)
    else:
        for i in range(starting_row, row_count):
            print(f"Page {page_number} | Row {i+1}/{row_count}")
            process_row(i)

def wait_for_load():
    try:
        # Wait for the loader to appear (briefly)
        page.wait_for_selector(".loading-ring", state="visible", timeout=2000)
        # Wait for the loader to DISAPPEAR
        page.wait_for_selector(".loading-ring", state="hidden")
    except:
        pass

if __name__ == "__main__":
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not DEBUG) # False for debugging
        context = browser.new_context()
        page = context.new_page()

        page.goto(root_link)

        wait_for_load()
        # page.wait_for_load_state('networkidle')
        # page.wait_for_timeout(1000)
        page.locator('//*[@id="select_nombre_resultats"]').first.select_option(label=f"{results_per_page}")

        try:
            number_of_records = int(page.locator('div.nombre_resultat_facettes').first.inner_text().replace('\u202f', '').split()[0])
        except:
            number_of_records = 12428

        # start_scrapping_on(starting_page=STARTING_PAGE, starting_row=STARTING_INDEX)
        page_number = STARTING_PAGE
        jump_to_page(page_number)
        while(page_number <= number_of_records // results_per_page):
            try:
                scrape_page(page_number)
                next_page(page_number)
                page_number += 1
                tries = 0 # reset tries after successfully processing a page
            except Exception as e:
                print(f"Error on page {page_number}: {str(e)}. Retrying...")
                tries += 1
                if tries > max_tries:             
                    notify(message=f"Creuse scraper failed to process page {page_number} after max tries", subject="Creuse Scraper Error")
                    print("exiting...")
                    exit()
                jump_to_page(page_number)
            
    notify(message="Creuse scraper has finished", subject="Creuse Finished")
                
                