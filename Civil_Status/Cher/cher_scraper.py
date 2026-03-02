# Scraper for "https://www.archives18.fr/archives-numerisees/registres-paroissiaux-et-etat-civil?arko_default_61011a8e5db65--ficheFocus="
# As of March 2026

import re
import sys
import time

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
        # self.filter_link = "https://www.archives18.fr/archives-numerisees/registres-paroissiaux-et-etat-civil?arko_default_61011a8e5db65--ficheFocus=&arko_default_61011a8e5db65--filtreGroupes%5Bmode%5D=simple&arko_default_61011a8e5db65--filtreGroupes%5Bop%5D=AND&arko_default_61011a8e5db65--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61011b4c3eacb%5D%5Bop%5D=AND&arko_default_61011a8e5db65--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61011b4c3eacb%5D%5Bq%5D%5B%5D=&arko_default_61011a8e5db65--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61011b4c3eacb%5D%5Bq%5D%5B%5D=Ach%C3%A8res%5B%5Barko_fiche_615ab249b84b4%5D%5D&arko_default_61011a8e5db65--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61011b4c3eacb%5D%5Bextras%5D%5Bmode%5D=popup&arko_default_61011a8e5db65--from=0&arko_default_61011a8e5db65--resultSize=25&arko_default_61011a8e5db65--contenuIds%5B%5D=2655740&arko_default_61011a8e5db65--modeRestit=arko_default_61011eb03aad2"
        # self.run_main()
        communes = retrieve_communes(self.root_link)

        for commune in communes['communs'].tolist():
            self.filter_link = self.get_filter_url(commune)
            self.run_main()

    def get_filter_url(self, commune: str) -> str:
        url = ""
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=False)
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
            page.locator('.filtre_liste_popup_liste').get_by_title(commune).click()
            page.wait_for_timeout(2000)
            url = page.url
            print(url)
            browser.close()
        return url

if __name__ == "__main__":
    scraper = CherScraper(
        debug_config=DebugConfig(
            headless=False, 
            one_per_page=True, 
            raise_exceptions=True
            ),
            starting_page=0,
        )

    print(scraper.run_filtered())

