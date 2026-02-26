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
import time

# Global variables for tracking progress

current_record = 1
total_records = 13247
records_per_page = 25
csv_location = "Aube Civil Status/Aube.csv"

def get_current_page_number() -> int:
    return current_record // records_per_page + 1 # with out the +1 it's zero indexed

def scrape_links_from_page(page, root_link) -> None:
  
    terms = True # to track if we need to accept the terms and conditions
    global records_per_page # this is defined from the results per page, It could be changed but there is no reason to.
    global current_record
    global total_records


    for i in range(records_per_page):
        # Wait for the table to load in
        page.wait_for_load_state('networkidle')
        expect(page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[2]/table/tbody'))
        row = page.locator(f'//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[2]/table/tbody/tr[{i+1}]')

        # Copy down some data from the table
        period = row.locator("td").nth(1).inner_text()
        act_types = row.locator("td").nth(2).inner_text()
        # click on the eyeball button
        row.locator("td").nth(5).locator("button").click()
        page.wait_for_load_state('networkidle')

        # accept terms and conditions. For some reason it's on the second page too
        if current_record in [1, 26]:
            page.locator("button[data-cy='accept-license']").click()
            page.wait_for_load_state('networkidle')
            # terms = False

        record_id = page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[4]/div[2]/div[2]/div[2]/h1').inner_text()
        # click on the three dots button
        page.locator("button[data-cy='btn-toggle-volet']").click()
        page.mouse.move(0, 0) # the tooltip sometimes covers the button

        # click on the share button
        page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[4]/div[2]/section/nav/ul/li[5]/button').click(timeout=60000)
        href = page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[4]/div[2]/section/div/div[2]/ul/li[1]/a').get_attribute("href")

        image_count = int(page.locator(".nb_total").inner_text()[2:]) # remove the "/ " at the start

        row_data = f"{record_id}|{act_types.replace("\n", ", ")}|{image_count}|{href}"
        with open(csv_location, "a", encoding="utf-8") as f:
            f.write(row_data + "\n")

        # go back to the results page
        # page.goto(root_link)
        page.wait_for_load_state('networkidle', timeout=60000)
        page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[4]/div[2]/div[2]/button[3]').click()

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
            print(f"Scraping page {get_current_page_number()}...")
            scrape_links_from_page(page, root_link)

            # navigate to the next page
            page.wait_for_load_state('networkidle', timeout=60000)
            nav_buttons = page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[1]/div[2]/nav/ul')
            nav_buttons.get_by_text(str(get_current_page_number() + 1)).click()
