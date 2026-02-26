# This class is a scraper for websites that use the Arkaïe platform.
# It is refered to as type 1 in the progress sheet: https://docs.google.com/spreadsheets/d/1OjWKvVCp7wJMZ8MoXS3Tzi79Z3UX70XRhZ-vxAopKiY/edit?gid=957413461#gid=957413461
# Example websites:
# - https://archives.creuse.fr/rechercher/archives-numerisees/registres-paroissiaux-et-de-letat-civil?arko_default_6089614c0e9ed--ficheFocus=
# - https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-de-la-ville-de-troyes-1535-1919?arko_default_62289d8b205f4--ficheFocus=

# Each scraper is a variant of this class, so if a method needs to be customized, extend the class.

import sys
import re
from playwright.sync_api import sync_playwright, expect
sys.path.append("W:\\RA_work_folders\\Ashton_Reed")
from Utilities.notifier import notify
from urllib.parse import urlparse, urlencode, urlunparse

class CollumnNumbers:
    def __init__(self, cote: int, commune: int, act_types: int, period: int, image_count: int):
        self.cote = cote
        self.commune = commune
        self.act_types = act_types
        self.period = period
        self.image_count = image_count

class DebugConfig:
    """
    Headless mode: is best for running the scraper at speed when you know it works well.
    One per page: scrapes the first record on each page so you know that it is successfully navigating to the next page
    Raise exceptions: When you are doing a full run it will likely still time out ocasionally. You want to keep going
    despite those error, but when you are testing, you want to know about those errors so you can fix them.
    """
    def __init__(self, headless: bool, one_per_page: bool = False, raise_exceptions: bool = False):
        self.headless = headless
        self.one_per_page = one_per_page
        self.raise_exceptions = raise_exceptions

# TODO: add stopping point. For testing
class ArkaieScraper:
    def __init__(
            self, 
            root_link: str, 
            name: str, 
            collumn_numbers: CollumnNumbers,
            csv_location: str = None,
            results_per_page: int = 100, # I think that 100 will be faster
            starting_page: int = 1, 
            starting_index: int = 0, 
            max_tries: int = 5, 
            debug_config: DebugConfig = DebugConfig(headless=False, one_per_page=False),
            department: str = None
        ):
        self.root_link = root_link
        self.name = name
        self.collumn_numbers = collumn_numbers

        if csv_location is None:
            self.csv_location = f"Civil_Status/{name}/{name}.csv"
        else:
            self.csv_location = csv_location

        # Aube2 should have the department as "Aube" not "Aube2", so we can specify it manually. For most scrapers, the department is the same as the name.
        if department is None:
            self.department = name
        else:            
            self.department = department
            
        self.results_per_page = results_per_page
        self.starting_page = starting_page
        self.starting_index = starting_index
        self.max_tries = max_tries
        self.tries = 0
        self.debug_config = debug_config
        self.page_number = starting_page
    
    # TODO if there are more than 10000 records, we need to run twice with different urls
    def run_main(self):
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self.debug_config.headless)
            context = browser.new_context()
            self.page = context.new_page()
            self.jump_to_page(self.page_number)
            number_of_records = int(self.page.locator('div.nombre_resultat_facettes').first.inner_text().replace('\u202f', '').split()[0])

            while(self.page_number <= number_of_records // self.results_per_page):
                try:
                    self.scrape_page(self.page_number)
                    self.page_number += 1
                    self.jump_to_page(self.page_number)
                    self.tries = 0 # reset tries after successfully processing a page
                except Exception as e:
                    self.recover_page_fail(e)
            
        notify(message=f"{self.name} scraper has finished", subject=f"{self.name} Finished")
        
    def url_for_page_number(self, page_number) -> str:
        # 1. Extract the hex ID (the 'funny string')
        # This regex looks for 'arko_default_' followed by alphanumeric characters
        match = re.search(r'arko_default_([a-z0-9]+)', self.root_link)
        if not match:
            raise ValueError("Could not find the Arko ID in the base URL.")
        
        arko_id = match.group(1)
        prefix = f"arko_default_{arko_id}"
        
        # 2. Calculate the 'from' offset
        # Page 1 = 0, Page 2 = 25, Page 3 = 50
        record_offset = (page_number - 1) * self.results_per_page
        
        # 3. Construct the query parameters from scratch
        query_params = {
            f"{prefix}--ficheFocus": "",
            f"{prefix}--from": str(record_offset),
            f"{prefix}--resultSize": str(self.results_per_page)
        }
        
        # 4. Strip the old query from the URL and attach the new one
        parsed_url = urlparse(self.root_link)
        new_query = urlencode(query_params)
        
        # Rebuild the URL using the original path but our fresh query
        return urlunparse(parsed_url._replace(query=new_query))
  
    def jump_to_page(self, target_page: int):
        # 1. Extract the hex ID (the 'funny string')
        # This regex looks for 'arko_default_' followed by alphanumeric characters
        match = re.search(r'arko_default_([a-z0-9]+)', self.root_link)
        if not match:
            raise ValueError("Could not find the Arko ID in the base URL.")
        
        arko_id = match.group(1)
        prefix = f"arko_default_{arko_id}"
        
        # 2. Calculate the 'from' offset
        # Page 1 = 0, Page 2 = 25, Page 3 = 50
        record_offset = (target_page - 1) * self.results_per_page
        
        # 3. Construct the query parameters from scratch
        query_params = {
            f"{prefix}--ficheFocus": "",
            f"{prefix}--from": str(record_offset),
            f"{prefix}--resultSize": str(self.results_per_page)
        }
        
        # 4. Strip the old query from the URL and attach the new one
        parsed_url = urlparse(self.root_link)
        new_query = urlencode(query_params)
        
        # Rebuild the URL using the original path but our fresh query
        url = urlunparse(parsed_url._replace(query=new_query))
        self.page.goto(url)

    def scrape_page(self, page_number: int, starting_row: int = 0):
        print(f"Page {page_number}:")
        row_count = self.count_rows()
        if self.debug_config.one_per_page:
            # for debugging only
            print(f"Page {page_number} | Row {1}/{row_count}")
            self.process_row(0)  
        else:
            for i in range(starting_row, row_count):
                print(f"Page {page_number} | Row {i+1}/{row_count}")
                self.process_row(i)

    def process_row(self, i: int):
        try:
            table_body = self.page.locator('tbody')
            row = table_body.locator('tr').nth(i)
            # Copy down some data from the table
            commune = row.locator("td").nth(self.collumn_numbers.commune).inner_text().replace('\n', '_')
            period = row.locator("td").nth(self.collumn_numbers.period).inner_text()
            cote = row.locator("td").nth(self.collumn_numbers.cote).inner_text()
            act_types = row.locator("td").nth(self.collumn_numbers.act_types).inner_text().replace('\n', '_')

            if row.locator("td").nth(self.collumn_numbers.image_count).inner_text() == '':
                print(f"No images in row {i+1}")
                # we need to write unaccesible records to verify we scraped every record we could
                self.write_row(i, cote, commune, period, act_types, "", "")
                return
            image_count = int(row.locator("td").nth(self.collumn_numbers.image_count).inner_text().split()[0][1:])
            href = self.enter_viewer(row)

        except Exception as e:
            self.recover_row_fail(i, e)
            return

        self.write_row(i, cote, commune, period, act_types, image_count, href)

        

    def enter_viewer(self, row) -> str:
        self.navigate_to_download_link(row)
        href = self.page.locator('a.exporter').get_attribute("href")
        # go back to the results page
        self.page.wait_for_load_state('networkidle')
        self.page.locator('button.close').click()
        return str(href)

    def click_terms(self):
        # accept terms and conditions.
        self.page.locator("button[data-cy='accept-license']").click()
        self.page.wait_for_load_state('networkidle')

    def navigate_to_download_link(self, row):
        # click on the eyeball button
        row.locator("td").nth(self.collumn_numbers.image_count).locator("button").click()
        self.page.wait_for_load_state('networkidle')

        try:
            # click on the three dots button
            self.page.locator("button[data-cy='btn-toggle-volet']").click()
        except:
            self.click_terms() # try clicking the terms and conditions
            self.page.locator("button[data-cy='btn-toggle-volet']").click()
        
        self.page.mouse.move(0, 0) # the tooltip sometimes covers the button

        # click on the share button
        self.page.locator('button[title="Partage et impression"]').click()

    def count_rows(self) -> int:
        self.wait_for_load()
        table_body = self.page.locator('tbody')
        row_count = table_body.locator("tr").count()
        return row_count
    
    
    def wait_for_load(self):
        try:
            # Wait for the loader to appear (briefly)
            self.page.wait_for_selector(".loading-ring", state="visible", timeout=2000)
            # Wait for the loader to disappear
            self.page.wait_for_selector(".loading-ring", state="hidden")
        except:
            pass

    def write_row(self, i, cote, commune, period, act_types, image_count, href):
        # CSV format: department | page_number | row_number | cote | commune | period | act_types | image_count | href
        row_data = f"{self.department}|{self.page_number}|{i}|{cote}|{commune}|{period}|{act_types.replace('\n', ', ')}|{image_count}|{href}"
        with open(self.csv_location, "a", encoding="utf-8") as f:
            f.write(row_data + "\n")
        
    def recover_row_fail(self, i, e):
        # Attempt to recover from timeout error
            if self.debug_config.raise_exceptions:
                raise e
            
            print(f"Timeout processing row {i} Try {self.tries} Error: {e}")
            self.tries += 1
            if self.tries > self.max_tries:
                notify(message=f"{self.name} scraper failed to process row after max tries", subject=f"{self.name} Scraper Error")
                print("exiting...")
                exit()
            self.jump_to_page(self.page_number)
            self.process_row(i)
    
    def recover_page_fail(self, e):
        if self.debug_config.raise_exceptions:
            raise e
        print(f"Page error: '{str(e)}' on page {self.page_number}. Retrying...")
        self.tries += 1
        if self.tries > self.max_tries:             
            notify(message=f"{self.name} scraper failed to process page {self.page_number} after max tries", subject=f"{self.name} Scraper Error")
            print("exiting...")
            exit()
        self.jump_to_page(self.page_number)
    