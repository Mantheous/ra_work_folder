# this generates the list of all the communes that have records

import re
import pandas as pd
from Utilities.browser_utils import load_browser
from Arkaie_Scraper.commun_tools import navigate_to_commune_list, get_letters

def navigate_to_commune_list(page):
    # for some sites the button is hidden
    try:
        page.get_by_label('Consulter la liste').first.click()
    except:
        commune_search = page.get_by_label(re.compile(r"Commune", re.IGNORECASE))
        if commune_search.count() == 1:
            commune_search.click()        
        page.get_by_label('Consulter la liste').first.click()

def retrieve_communes(link: str, out_file = None) -> pd.DataFrame:
    """Navigates the site and builds a CSV dictionary of all communes."""
    df = pd.DataFrame(columns= ["communs", "record_count"])
    with load_browser(headless=False) as page:
        page.goto(link)
        
        navigate_to_commune_list(page)
        letters = get_letters(page)
        for i in range(letters.locator("li").count()):
            letters.locator("li").nth(i).click()
            text = page.locator('.filtre_liste_popup_liste').inner_text()
            communes = text.split("\n")[::2]
            counts = text.split("\n")[1::2]
            counts = [int(x) for x in counts]
            new_df = pd.DataFrame({"communs": communes, "record_count": counts})
            df = pd.concat([df, new_df], ignore_index=True)
            if out_file:
                df.to_csv(out_file, index=False, encoding="utf-8")
    return df

if __name__ == "__main__":
    
    root_link = 'https://archives28.fr/archives-et-inventaires-en-ligne/histoire-des-individus-des-populations-et-genealogie/les-registres-paroissiaux-et-detat-civil?arko_default_6241bc24d7427--ficheFocus='

    # file_name = "Aube_Civil_Status/Aube_Communes_and_Count.csv"
    results = retrieve_communes(root_link)
    # print(f"Total records: {results[['record_count']].sum()}")
    print("Total records:", results["record_count"].sum())
