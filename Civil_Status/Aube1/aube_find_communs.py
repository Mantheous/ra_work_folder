# this generates the list of all the communes that have records

from playwright.sync_api import sync_playwright, expect

FILENAME = "Aube Civil Status/Aube_Communes.txt"

if __name__ == "__main__":
    
    root_link = 'https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-des-communes-de-laube-hors-troyes-1552-1919?arko_default_6228a5627b9b1--ficheFocus='

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        page.goto(root_link)
        
        # for each letter that has communes...
        page.locator('//*[@id="volet-de-filtres-arko_default_6228a5627b9b1"]/div[2]/div/div').click()
        page.locator('//*[@id="aria-filtre-arko_default_6228a660884d3"]/div[1]/button').click()
        letters = page.locator("//body/div[4]/nav/ul")
        for i in range(22):
            letters.locator("li").nth(i).click()
            text = page.locator('//body/div[4]/div[1]').inner_text()
            with open(FILENAME, "a", encoding="utf-8") as f:
                f.write(text + "\n")
        lines = []
        with open(FILENAME, "r", encoding="utf-8") as f:
            lines = f.readlines()
        with open(FILENAME, "w", encoding="utf-8") as f:
            f.writelines(lines[0::2])
