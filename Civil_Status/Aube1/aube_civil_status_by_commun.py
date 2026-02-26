# Scraper for "https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-des-communes-de-laube-hors-troyes-1552-1919?arko_default_6228a5627b9b1--ficheFocus="
# As of Febuary 2026

# Cool things about this site:
#   The downoad links for a time period have the image number in the url
#   href="https://www.archives-aube.fr/_recherche-images/download/304243/image/29316/**Image number**/full/max/0/default.jpg"
#   These are zero indexed so for 1566-1739 it's 0-151 
#   There is no crawl delay

# There are 13247 results

# This scraper is getting shelved for now because the site lacks pretty essential information. The method I have used to get
# the commune information results in duplicates and I is really a problem with the website. I did scrape all of the links
# however to use we need to deal with the duplicates. If there are two copies the solution is probably just to choose a commune name
# and just merge them. There is a case where there is a commune that is a combination of two neighboring communes.

from playwright.sync_api import sync_playwright, expect # type: ignore
import time
import sys
sys.path.append("W:\\RA_work_folders\\Ashton_Reed")
from Utilities.notifier import notify

# Global variables for tracking progress

csv_location = "Aube Civil Status/Aube.csv"
communes_path = "Aube Civil Status/Aube_Communes.txt"
tries = 0
max_tries = 3

def filter_by_commune(commune: str) -> bool:
    try:
        # open up the filter button
        page.locator('//*[@id="volet-de-filtres-arko_default_6228a5627b9b1"]/div[2]/div/div').click()
        page.locator('//*[@id="commune_paroisse"]').fill(commune)
        page.locator('//*[@id="commune_paroisse"]').press("Enter")
        page.wait_for_load_state('networkidle')
        results_number_loc = page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[1]/div[1]/span')

        # this takes a long time to load
        try: 
            expect(results_number_loc).not_to_contain_text('247') # we refilter from nothing everytime so this comfirms that it's loaded.
            page.wait_for_timeout(4000) # just in case
        except:
            page.wait_for_timeout(5000)
        page.locator("#select_nombre_resultats").first.select_option(label="100") # increase beyond 25 for so we don't have to navigate through as many pages
        
    except:
        global tries
        print(f"timeout filtering... trying again {tries}")
        tries += 1
        if tries > max_tries:
            notify(message="Aube scraper has failed", subject="Aube Error")
            print("exiting...")
            exit()
        filter_by_commune(commune)
        tries = 0 # reset tries after a successful attempt

def click_terms():
    # accept terms and conditions. For some reason it's on the second page too
    page.locator("button[data-cy='accept-license']").click()
    page.wait_for_load_state('networkidle')

def process_row(commun: str, i: int):
    try:
        # Wait for the table to load in
        page.wait_for_load_state('networkidle')
        expect(page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[2]/table/tbody'))
        row = page.locator(f'//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[2]/table/tbody/tr[{i+1}]')

        # Copy down some data from the table
        act_types = row.locator("td").nth(2).inner_text()
        image_count = int(row.locator("td").nth(5).inner_text().split()[0][1:])
        # click on the eyeball button
        row.locator("td").nth(5).locator("button").click()
        page.wait_for_load_state('networkidle')

        if i == 0 and commun == communes[0].strip():
            click_terms()

        try:
            record_id = page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[4]/div[2]/div[2]/div[2]/h1').inner_text()
        except:
            click_terms() # try clicking the terms and conditions
            record_id = page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[4]/div[2]/div[2]/div[2]/h1').inner_text()
            # click on the three dots button
        page.locator("button[data-cy='btn-toggle-volet']").click()
        page.mouse.move(0, 0) # the tooltip sometimes covers the button

        # click on the share button
        page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[4]/div[2]/section/nav/ul/li[5]/button').click()
        href = page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[4]/div[2]/section/div/div[2]/ul/li[1]/a').get_attribute("href")
    except:
        global tries
        print(f"timeout processing row... try {tries}...")
        tries += 1
        if tries > max_tries:
            notify(message="Aube scraper has failed", subject="Aube Error")
            print("exiting...")
            exit()
        page.goto(root_link)
        filter_by_commune(commun)
        process_row(commun, i)
        tries = 0 # reset tries after a successful attempt
        return

    row_data = f"{record_id.split(", ")[1]}|{commun}|{record_id.split(", ")[0]}|{act_types.replace("\n", ", ")}|{image_count}|{href}"
    with open(csv_location, "a", encoding="utf-8") as f:
        f.write(row_data + "\n")

    # go back to the results page
    page.wait_for_load_state('networkidle')
    page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[4]/div[2]/div[2]/button[3]').click()



if __name__ == "__main__":
    
    root_link = 'https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-des-communes-de-laube-hors-troyes-1552-1919?arko_default_6228a5627b9b1--ficheFocus='

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True) # False for debugging
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(60000) # the site can be slow so I increased the timeout

        communes = []
        with open(communes_path, "r", encoding="utf-8") as f:
            communes= f.readlines()

        for commun in communes:
            page.goto(root_link)
            filter_by_commune(commun.strip())

            # Table
            table_body = page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[2]/table/tbody')
            expect(table_body)
            page.wait_for_timeout(500)
            row_count = table_body.locator("tr").count()
            for i in range(row_count):
                print(f"Row {i+1} of {row_count} for commune {commun.strip()}")
                process_row(commun.strip(), i)
    notify(message="Aube scraper has finished", subject="Aube Finished")