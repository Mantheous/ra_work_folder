# Scraper for "https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-de-la-ville-de-troyes-1535-1919?arko_default_62289d8b205f4--ficheFocus="
# As of Febuary 2026

# This web scraper is very very similair to abue 1. The site is structured a tad differently so some of the Xpaths are
# different. This site also has communes with over 100 records so we have to flip through pages. The good thing is 
# each commune is actually labeled.

from playwright.async_api import async_playwright, expect # type: ignore
import sys
sys.path.append("W:\\RA_work_folders\\Ashton_Reed")
from Utilities.notifier import notify
import asyncio

# Global variables for tracking progress

csv_location = "Aube2 Civil Status/Aube2.csv"
communes_path = "Aube2 Civil Status/Aube2_Communes.txt"
tries = 0
max_tries = 2

async def filter_by_commune(commune: str, page):
    try:
        # open up the filter button
        await page.locator('//*[@id="volet-de-filtres-arko_default_62289d8b205f4"]/div[2]/div').click()
        await page.locator('//*[@id="commune_paroisse"]').fill(commune)
        await page.locator('//*[@id="commune_paroisse"]').press("Enter")
        await page.wait_for_load_state('networkidle')
        results_number_loc = page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[1]/div[1]/span')
        await page.wait_for_timeout(2000)
        # we then
        try: 
            await expect(results_number_loc).not_to_contain_text('247') # we refilter from nothing everytime so this comfirms that it's loaded.
        except:
            await page.wait_for_timeout(5000)
            # if (1 != page.locator('//*[@id="cms_colonne_centre"]/div[3]/div/div[2]/div/div[2]/table/tbody').locator("tr").count()):
            #     raise Exception()
        await page.locator("#select_nombre_resultats").first.select_option(label="100") # increase beyond 25 for so we don't have to navigate through as many pages
        
    except:
        global tries
        print(f"timeout filtering... trying again {tries}")
        tries += 1
        if tries > max_tries:
            notify()
            print("exiting...")
            exit()
        await filter_by_commune(commune, page)

async def click_terms(page):
    # accept terms and conditions. For some reason it's on the second page too
    await page.locator("button[data-cy='accept-license']").click()
    await page.wait_for_load_state('networkidle')

async def process_row(commun: str, i: int, page, root_link, communes):
    try:
        # Wait for the table to load in
        await page.wait_for_load_state('networkidle')
        table_body = await page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[2]/table/tbody')
        await expect(table_body)
        row = table_body.locator('tr').nth(i)
        # Copy down some data from the table
        act_types = row.locator("td").nth(2).inner_text()
        image_count = int(row.locator("td").nth(4).inner_text().split()[0][1:])
        # click on the eyeball button
        row.locator("td").nth(4).locator("button").click()
        await page.wait_for_load_state('networkidle')

        if i == 0 and commun == communes[0].strip():
            await click_terms(page)

        try:
            record_id = page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[4]/div[2]/div[2]/div[2]/h1').inner_text()
        except:
            await click_terms(page) # try clicking the terms and conditions
            record_id = page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[4]/div[2]/div[2]/div[2]/h1').inner_text()
            # click on the three dots button
        page.locator("button[data-cy='btn-toggle-volet']").click()
        page.mouse.move(0, 0) # the tooltip sometimes covers the button

        # click on the share button
        await page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[4]/div[2]/section/nav/ul/li[6]/button').click()
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
        await filter_by_commune(commun, page)
        await process_row(commun, i, page, root_link, communes)
        return

    row_data = f"{record_id.split(', ')[1]}|{commun}|{record_id.split(', ')[2]}|{act_types.replace('\n', ', ')}|{image_count}|{href}"
    with open(csv_location, "a", encoding="utf-8") as f:
        f.write(row_data + "\n")

    # go back to the results page
    await page.wait_for_load_state('networkidle')
    await page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[4]/div[2]/div[2]/button[3]').click()

async def main():
    root_link = 'https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-de-la-ville-de-troyes-1535-1919?arko_default_62289d8b205f4--ficheFocus='
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False) # False for debugging
        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_timeout(60000) # the site can be slow so I increased the timeout

        communes = []
        with open(communes_path, "r", encoding="utf-8") as f:
            communes= f.readlines()

        for commun in communes:
            await page.goto(root_link)
            await filter_by_commune(commun.strip(), page)

            # Table
            table_body = page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[2]/table/tbody')
            expect(table_body)

            row_count = await table_body.locator("tr").count()
            while(row_count == 25):
                await page.wait_for_timeout(500)
                row_count = await table_body.locator("tr").count()

            page_number = 1
            # print(f"Page {page_number} for commune: {commun.strip()}")
            # for i in range(row_count):
            #     print(f"Row {i+1} of {row_count} for commune {commun.strip()}")
            #     process_row(commun.strip(), i)

            while(row_count == 100):
                page_number+=1
                nav_buttons = page.locator('//*[@id="cms_colonne_centre"]/div[2]/div/div/div/div[1]/div[2]/nav')
                await nav_buttons.get_by_text(str(page_number + 1)).click()

                print(f"Page {page_number} for commune: {commun.strip()}")
                for i in range(row_count):
                    print(f"Row {i+1} of {row_count} for commune {commun.strip()}")
                    await process_row(commun.strip(), i, page, root_link, communes)

if __name__ == "__main__":
    asyncio.run(main())
    notify(message="Aube 2 scraper has finished", subject="Aube 2 Finished")
                
                