# Scraper for "https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-de-la-ville-de-troyes-1535-1919?arko_default_62289d8b205f4--ficheFocus="
# As of Febuary 2026

# This web scraper is very very similair to abue 1. The site is structured a tad differently so some of the Xpaths are
# different. This site also has communes with over 100 records so we have to flip through pages. The good thing is 
# each commune is actually labeled.

from playwright.sync_api import sync_playwright, expect # type: ignore
import sys
sys.path.append("W:\\RA_work_folders\\Ashton_Reed")
from Utilities.notifier import notify
import time

# Global variables for tracking progress

csv_location = "Aube2 Civil Status/Aube2.csv"
communes_path = "Aube2 Civil Status/Aube2_Communes.txt"
tries = 0
max_tries = 2
results_per_page = 25

def filter_by_commune(commune: str):
    try:
        # open up the filter button
        page.locator('//*[@id="volet-de-filtres-arko_default_62289d8b205f4"]/div[2]/div').click()
        page.locator('//*[@id="commune_paroisse"]').fill(commune)
        page.locator('//*[@id="commune_paroisse"]').press("Enter")
        page.wait_for_load_state('networkidle')
        results_number_loc = page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[1]/div[1]/span')
        page.wait_for_timeout(2000)
        # we then
        try: 
            expect(results_number_loc).not_to_contain_text('247') # we refilter from nothing everytime so this comfirms that it's loaded.
        except:
            page.wait_for_timeout(5000)
            # if (1 != page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[2]/table/tbody').locator("tr").count()):
            #     raise Exception()
        page.locator("#select_nombre_resultats").first.select_option(label=f"{results_per_page}") # increase beyond 25 for so we don't have to navigate through as many pages
        
    except:
        global tries
        print(f"timeout filtering... trying again {tries}")
        tries += 1
        if tries > max_tries:
            notify()
            print("exiting...")
            exit()
        filter_by_commune(commune)

def click_terms():
    # accept terms and conditions. For some reason it's on the second page too
    # if page.locator("button[data-cy='accept-license']").count() == 0:
    #     return
    page.locator("button[data-cy='accept-license']").click()
    page.wait_for_load_state('networkidle')

def process_row(commun: str, i: int, page_number):
    try:
        # Wait for the table to load in
        page.wait_for_load_state('networkidle')
        table_body = page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[2]/table/tbody')
        expect(table_body)
        row = table_body.locator('tr').nth(i)
        # Copy down some data from the table
        act_types = row.locator("td").nth(2).inner_text()
        image_count = int(row.locator("td").nth(4).inner_text().split()[0][1:])
        # click on the eyeball button
        row.locator("td").nth(4).locator("button").click()
        page.wait_for_load_state('networkidle')

        if i == 0 and commun == communes[0].strip() and page_number == 1: # the first time we click on the button we have to accept the terms and conditions
            click_terms()

        try:
            record_id = page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[4]/div[2]/div[2]/div[2]/h1').inner_text()
        except:
            click_terms() # try clicking the terms and conditions
            record_id = page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[4]/div[2]/div[2]/div[2]/h1').inner_text()
            # click on the three dots button
        page.locator("button[data-cy='btn-toggle-volet']").click()
        page.mouse.move(0, 0) # the tooltip sometimes covers the button

        # click on the share button
        page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[4]/div[2]/section/nav/ul/li[6]/button').click()
        href = page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[4]/div[2]/section/div/div[2]/ul/li[1]/a').get_attribute("href")
    except:
        global tries
        print(f"timeout processing row... try {tries}...")
        tries += 1
        if tries > max_tries:
            notify()
            print("exiting...")
            exit()
        page.goto(root_link)
        go_to_page(page_number)
        filter_by_commune(commun)
        process_row(commun, i, page_number)
        return

    row_data = f"{record_id.split(", ")[1]}|{commun}|{record_id.split(", ")[2]}|{act_types.replace("\n", ", ")}|{image_count}|{href}"
    with open(csv_location, "a", encoding="utf-8") as f:
        f.write(row_data + "\n")

    # go back to the results page
    page.wait_for_load_state('networkidle')
    page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[4]/div[2]/div[2]/button[3]').click()

def count_rows() -> int:
    row_count = table_body.locator("tr").count()
    while(row_count != results_per_page):
        page.wait_for_timeout(500)
        row_count = table_body.locator("tr").count()
    return row_count
    
def go_to_page(page_number):
    nav_buttons = page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[1]/div[2]/nav/ul')
    nav_buttons.get_by_text(str(page_number)).click()
    # nav_buttons.locator("li:not([class])").nth(page_number - 1).click()
    page.wait_for_timeout(1000)

if __name__ == "__main__":
    
    root_link = 'https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-de-la-ville-de-troyes-1535-1919?arko_default_62289d8b205f4--ficheFocus='

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False) # False for debugging
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
            table_body = page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[2]/table/tbody')
            expect(table_body)

            page_number = 1
            row_count = count_rows()
            next_button = page.locator('button.bouton_pagination')
            while(next_button.count() != 0): # I need a new condition because there are some communes with exactly 25 results
                if(page_number != 1):
                    go_to_page(page_number)

                for i in range(0, row_count, 25): # I increased the step for faster debuging
                    print(f"Commune: {commun.strip()} Page: {page_number} Row: {i+1}/{row_count}")
                    process_row(commun.strip(), i, page_number)
                
                row_count = count_rows()
                page_number += 1
            
    notify(message="Aube 2 scraper has finished", subject="Aube 2 Finished")
                
                