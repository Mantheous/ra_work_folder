# Scraper for "https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-des-communes-de-laube-hors-troyes-1552-1919?arko_default_6228a5627b9b1--ficheFocus="
# As of Febuary 2026

# Cool things about this site:
#   The downoad links for a time period have the image number in the url
#   href="https://www.archives-aube.fr/_recherche-images/download/304243/image/29316/**Image number**/full/max/0/default.jpg"
#   These are zero indexed so for 1566-1739 it's 0-151 
#   There is no crawl delay

# Plan:
# We are going to iteratively go through the results page
#   Let's make a CSV file that has the period, Type of Acts, image count and link
# 
# Then we will go throught the CSV file and just download all of the files. We will probably do that headlessly for better performance

# There are 13247 results, 25 per page, so 530 pages to go through

from playwright.sync_api import sync_playwright, expect
import csv  
import os
import piexif
import time

# Global variables for tracking progress

current_record = 1
total_records = 13247
records_per_page = 25

def get_current_page_number() -> int:
    return current_record // records_per_page

def scrape_links_from_page(page, root_link) -> None:
  
    # terms = True # to track if we need to accept the terms and conditions
    global records_per_page # this is defined from the results per page, It could be changed but there is no reason to.
    global current_record
    global total_records


    for i in range(records_per_page):
        # Wait for the table to load in
        page.wait_for_load_state('networkidle', timeout=60000)
        expect(page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[2]/table/tbody'))
        row = page.locator(f'//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[2]/table/tbody/tr[{i+1}]')

        # Copy down some data from the table
        period = row.locator("td").nth(1).inner_text()
        act_types = row.locator("td").nth(2).inner_text()
        number_of_images = int(row.locator("td").nth(5).inner_text().split()[0][1:])
        for i in range(number_of_images):
            link = f"https://www.archives-aube.fr/_recherche-images/download/304243/image/{29315 + current_record}/{i}/full/max/0/default.jpg"
            row_data = f"({period})_({act_types.replace("\n", "-")})-{i}|{link}"
            with open(f"Aube_links.csv", "a", encoding="utf-8") as f:
                f.write(row_data + "\n")

        current_record += 1
        if current_record > total_records:
            break

if __name__ == "__main__":
    
    root_link = 'https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-des-communes-de-laube-hors-troyes-1552-1919?arko_default_6228a5627b9b1--ficheFocus='

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        
        page.goto(root_link)   

        while current_record <= total_records:
            print(f"Scraping page {get_current_page_number() + 1}...")
            scrape_links_from_page(page, root_link)

            # navigate to the next page
            page.wait_for_load_state('networkidle', timeout=60000)
            nav_buttons = page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[1]/div[2]/nav/ul')
            nav_buttons.get_by_text(str(get_current_page_number() + 1)).click()
            time.sleep(3) # Sometimes it doesn't change the records
            page.wait_for_load_state('networkidle', timeout=60000)
