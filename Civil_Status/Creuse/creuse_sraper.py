# Scraper for "https://archives.creuse.fr/rechercher/archives-numerisees/registres-paroissiaux-et-de-letat-civil?arko_default_6089614c0e9ed--ficheFocus=&arko_default_6089614c0e9ed--filtreGroupes%5Bmode%5D=simple&arko_default_6089614c0e9ed--filtreGroupes%5Bop%5D=AND&arko_default_6089614c0e9ed--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_6089619825455%5D%5Bop%5D=NOT&arko_default_6089614c0e9ed--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_6089619825455%5D%5Bextras%5D%5Bmode%5D=select&arko_default_6089614c0e9ed--from=0&arko_default_6089614c0e9ed--resultSize=25&arko_default_6089614c0e9ed--contenuIds%5B%5D=476687&arko_default_6089614c0e9ed--modeRestit=arko_default_60896998ef13f"
# As of Febuary 2026

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from Utilities.paths import PROJECT_ROOT
from Arkaie_Scraper.arkaie_scraper import ArkaieScraper, CollumnNumbers, DebugConfig

class CreuseScraper(ArkaieScraper):
    '''
    The primary variation in this scraper is that the "cote" is not available on the main page, 
    so we have to enter the viewer to get it. See ArkaieScraper for general functionality.
    '''
    def __init__(
            self, 
            debug_config: DebugConfig, 
            starting_page: int = 0, 
            csv_location: str = None, # pyright: ignore[reportArgumentType]
            stoping_page: int = None, # pyright: ignore[reportArgumentType]
        ): 
        super().__init__(
            root_link="https://archives.creuse.fr/rechercher/archives-numerisees/registres-paroissiaux-et-de-letat-civil?arko_default_6089614c0e9ed--ficheFocus=#",
            name="Creuse",
            collumn_numbers=CollumnNumbers(
                cote=1, # pyright: ignore[reportArgumentType]
                commune=0,
                act_types=2,
                period=3,
                image_count=4
            ),
            debug_config=debug_config,
            starting_page=starting_page,
            csv_location=csv_location
        )

    def run_split(self, filter1: str, filter2: str):
        self.filter_link = filter1
        self.run_main()
        print("Filter 2" + "-" * 10)
        self.filter_link = filter2
        self.run_main()





if __name__ == "__main__":
    scraper = CreuseScraper(
        debug_config=DebugConfig(
            headless=True, 
            one_per_page=False, 
            raise_exceptions=False,
            notify_crash=False # it will send a notification for both halves. I could just make the modification, but I don't find the notifier that useful now
            ),
            starting_page=1,
            # csv_location="ra_work_folder/Civil_Status/Aube2/Aube2test.csv"
        )

    scraper.run_split(
        filter1= "https://archives.creuse.fr/rechercher/archives-numerisees/registres-paroissiaux-et-de-letat-civil?arko_default_6089614c0e9ed--ficheFocus=&arko_default_6089614c0e9ed--filtreGroupes%5Bmode%5D=simple&arko_default_6089614c0e9ed--filtreGroupes%5Bop%5D=AND&arko_default_6089614c0e9ed--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_6089619825455%5D%5Bop%5D=NOT&arko_default_6089614c0e9ed--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_6089619825455%5D%5Bq%5D%5B%5D=Mariages%5B%5Barko_fiche_60815ed983140%5D%5D&arko_default_6089614c0e9ed--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_6089619825455%5D%5Bextras%5D%5Bmode%5D=select&arko_default_6089614c0e9ed--from=0&arko_default_6089614c0e9ed--resultSize=25&arko_default_6089614c0e9ed--contenuIds%5B%5D=476687&arko_default_6089614c0e9ed--modeRestit=arko_default_60896998ef13f",
        filter2= "https://archives.creuse.fr/rechercher/archives-numerisees/registres-paroissiaux-et-de-letat-civil?arko_default_6089614c0e9ed--ficheFocus=&arko_default_6089614c0e9ed--filtreGroupes%5Bmode%5D=simple&arko_default_6089614c0e9ed--filtreGroupes%5Bop%5D=AND&arko_default_6089614c0e9ed--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_6089619825455%5D%5Bop%5D=AND&arko_default_6089614c0e9ed--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_6089619825455%5D%5Bq%5D%5B%5D=Mariages%5B%5Barko_fiche_60815ed983140%5D%5D&arko_default_6089614c0e9ed--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_6089619825455%5D%5Bextras%5D%5Bmode%5D=select&arko_default_6089614c0e9ed--from=0&arko_default_6089614c0e9ed--resultSize=25&arko_default_6089614c0e9ed--contenuIds%5B%5D=476687&arko_default_6089614c0e9ed--modeRestit=arko_default_60896998ef13f"
    )

