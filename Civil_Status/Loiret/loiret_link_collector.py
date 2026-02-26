# Scraper for "https://www.archives-loiret.fr/faire-vos-recherches/archives-numerisees/etat-civil?arko_default_61e6b3f775f99--ficheFocus="
# As of Febuary 2026

from playwright.sync_api import sync_playwright, expect # type: ignore
import sys
sys.path.append("W:\\RA_work_folders\\Ashton_Reed")
from Utilities.notifier import notify

# Global variables for tracking progress

name = "Loiret"
root_link = 'https://www.archives-loiret.fr/faire-vos-recherches/archives-numerisees/etat-civil?arko_default_61e6b3f775f99--ficheFocus='
csv_location = f"{name} Civil Status/{name}.csv"
tries = 0
max_tries = 5
results_per_page = 25
STARTING_PAGE = 1 # starts at 1, or you can set it to the page to resume on
STARTING_INDEX = 0
collumn_numbers = {
    "cote": 0,
    "commune": 1,
    "act_types": 2,
    "period": 4,
    "image_count": 6,
}
DEBUG = False

def jump_to_page(target_page: int):
    page.goto(root_link)
    # Change Page
    for i in range(2, target_page):
        nav_buttons = page.locator('nav.pagination_haute').first.locator("ul")
        nav_buttons.get_by_text(str(i+1)).click()

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
        commune = row.locator("td").nth(collumn_numbers["commune"]).inner_text().replace('\n', '_')
        period = row.locator("td").nth(collumn_numbers["period"]).inner_text()
        cote = row.locator("td").nth(collumn_numbers["cote"]).inner_text()
        act_types = row.locator("td").nth(collumn_numbers["act_types"]).inner_text().replace('\n', '_')

        if row.locator("td").nth(collumn_numbers["image_count"]).inner_text() == '':
            print(f"No images in row {i+1}")
            return
        image_count = int(row.locator("td").nth(collumn_numbers["image_count"]).inner_text().split()[0][1:])
        href = get_link(row)

    except:
        # Attempt to recover from timeout error
        # pass
        global tries
        print(f"timeout processing row... try {tries}...")
        tries += 1
        if tries > max_tries:
            notify(message=f"{name} scraper failed to process row after max tries", subject=f"{name} Scraper Error")
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
    wait_for_load()
    table_body = page.locator('tbody')
    row_count = table_body.locator("tr").count()
    return row_count

def scrape_page(page_number: int, starting_row: int = 0):
    print(f"Page {page_number}:")
    row_count = count_rows()
    if DEBUG:
        # for debugging only
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
        browser = pw.chromium.launch(headless=not DEBUG) # It failed when I ran it headlessly
        context = browser.new_context()
        page = context.new_page()

        page.goto(root_link)

        wait_for_load()
        page.locator('//*[@id="select_nombre_resultats"]').first.select_option(label=f"{results_per_page}")

        try:
            number_of_records = int(page.locator('div.nombre_resultat_facettes').first.inner_text().replace('\u202f', '').split()[0])
        except:
            number_of_records = 12428

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
                    notify(message=f"{name} scraper failed to process page {page_number} after max tries", subject=f"{name} Scraper Error")
                    print("exiting...")
                    exit()
                jump_to_page(page_number)
            
    notify(message=f"{name} scraper has finished", subject=f"{name} Finished")
                
                