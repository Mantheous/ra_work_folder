# Scraper for "https://www.archives18.fr/archives-numerisees/registres-paroissiaux-et-etat-civil?arko_default_61011a8e5db65--ficheFocus="
# As of March 2026

import re
import sys
import pandas as pd

sys.path.append("W:\\RA_work_folders\\Ashton_Reed\\ra_work_folder")
from Utilities.Validation.retrieve_communes import retrieve_communes
from Arkaie_Scraper.arkaie_scraper import ArkaieScraper, CollumnNumbers, DebugConfig
from playwright.sync_api import sync_playwright

class CherScraper(ArkaieScraper):
    '''
    The primary variation in this scraper is that it implements a filtered search. It needs to split up the
    records because the page can't support more that 10,000 records in a single search.
    '''
    def __init__(self, debug_config: DebugConfig, starting_page: int = 0):
        super().__init__(
            root_link="https://www.archives18.fr/archives-numerisees/registres-paroissiaux-et-etat-civil?arko_default_61011a8e5db65--ficheFocus=",
            name="Cher",
            department="Cher",
            collumn_numbers=CollumnNumbers(
                commune=0,
                period=1,
                cote=2,
                act_types=3,
                image_count=5
            ),
            debug_config=debug_config,
            starting_page=starting_page
        )

    def run_filtered(self):
        try:
            communes = pd.read_csv("W:/RA_work_folders/Ashton_Reed/ra_work_folder/Civil_Status/Cher/cher_communes.csv")
        except:
            communes = retrieve_communes(self.root_link, "W:/RA_work_folders/Ashton_Reed/ra_work_folder/Civil_Status/Cher/cher_communes.csv")

        for commune in communes['communs'].tolist():
            self.filter_link = self.get_filter_url(commune)
            self.run_main()

    def get_filter_url(self, commune: str) -> str:
        url = ""
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self.debug_config.headless)
            context = browser.new_context()
            page = context.new_page()
            page.goto(self.root_link)

            commune_search = page.get_by_label(re.compile(r"Commune", re.IGNORECASE))
            if commune_search.count() == 1:
                commune_search.click()
            page.get_by_label('Consulter la liste').click()
            letters = page.locator("//body/div[4]/nav/ul")
            try:
                letters.get_by_text(commune[0]).click(timeout=1000)
            except:
                pass
            page.locator('.filtre_liste_popup_liste').get_by_title(commune, exact=True).click()
            page.wait_for_timeout(2000)
            url = page.url
            browser.close()
        return url

    # There are many cases where records are not labeled to show up in the filters
    # This will filter for all of the left overs.
    def get_not_commune_filter(self, communes: pd.DataFrame) -> str:
        url = ""
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self.debug_config.headless)
            context = browser.new_context()
            page = context.new_page()
            page.goto(self.root_link)

            page.locator('#arko_default_61011b4c3eacb--operator').select_option('NOT')

            for commune in communes['communs'].tolist():
                commune_search = page.get_by_label(re.compile(r"Commune", re.IGNORECASE))
                if commune_search.count() == 1:
                    commune_search.click()

                page.get_by_label('Consulter la liste').click()
                letters = page.locator("//body/div[4]/nav/ul")
                try:
                    letters.get_by_text(commune[0],).click(timeout=1000)
                except:
                    pass
                page.locator('.filtre_liste_popup_liste').get_by_title(commune, exact=True).click()
            url = page.url
            browser.close()
        return url

if __name__ == "__main__":
    scraper = CherScraper(
        debug_config=DebugConfig(
            headless=True, 
            one_per_page=False, 
            raise_exceptions=False
            ),
            starting_page=1,
        )

    scraper.run_filtered()
    # communes = pd.read_csv("W:/RA_work_folders/Ashton_Reed/ra_work_folder/Civil_Status/Cher/cher_communes.csv")
    # print(scraper.get_not_commune_filter(communes))

