# this generates the list of all the communes that have records

from playwright.sync_api import sync_playwright, expect
import re
import pandas as pd

def retrieve_communes(link: str, out_file = None) -> pd.DataFrame:
    df = pd.DataFrame(columns= ["communs", "record_count"])
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        page.goto(link)
        
        # for each letter that has communes...
        # for some sites the button is hidden
        commune_search = page.get_by_label(re.compile(r"Commune", re.IGNORECASE))
        if commune_search.count() == 1:
            commune_search.click()
        page.get_by_label('Consulter la liste').click()
        letters = page.locator("//body/div[4]/nav/ul")
        for i in range(letters.locator("li").count()):
            letters.locator("li").nth(i).click()
            text = page.locator('//body/div[4]/div[1]').inner_text()
            communes = text.split("\n")[::2]
            counts = text.split("\n")[1::2]
            counts = [int(x) for x in counts]
            new_df = pd.DataFrame({"communs": communes, "record_count": counts})
            df = pd.concat([df, new_df], ignore_index=True)
            if out_file:
                df.to_csv(out_file, index=False, encoding="utf-8")
    return df

if __name__ == "__main__":
    
    root_link = 'https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-de-la-ville-de-troyes-1535-1919?arko_default_62289d8b205f4--ficheFocus='

    # file_name = "Aube_Civil_Status/Aube_Communes_and_Count.csv"
    results = retrieve_communes(root_link)
    # print(f"Total records: {results[['record_count']].sum()}")
    print("Total records:", results["record_count"].sum())
