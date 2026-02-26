# Scraper for "https://archives.saint-etienne.fr/recherches-et-consultation-1/recherche-documentaire/genealogie?arko_default_619517ef1a5d5--ficheFocus=&arko_default_619517ef1a5d5--filtreGroupes%5Bmode%5D=simple&arko_default_619517ef1a5d5--filtreGroupes%5Bop%5D=AND&arko_default_619517ef1a5d5--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61976d807f4e2%5D%5Bop%5D=NOT&arko_default_619517ef1a5d5--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61976d807f4e2%5D%5Bextras%5D%5Bmode%5D=select&arko_default_619517ef1a5d5--from=0&arko_default_619517ef1a5d5--resultSize=25&arko_default_619517ef1a5d5--contenuIds%5B%5D=156426&arko_default_619517ef1a5d5--modeRestit=arko_default_6195181087d5d"
# As of Febuary 2026

import sys
sys.path.append("W:\\RA_work_folders\\Ashton_Reed")
from Arkaie_Scraper.arkaie_scraper import ArkaieScraper, CollumnNumbers

scraper = ArkaieScraper(
    root_link="https://archives.saint-etienne.fr/recherches-et-consultation-1/recherche-documentaire/genealogie?arko_default_619517ef1a5d5--ficheFocus=&arko_default_619517ef1a5d5--filtreGroupes%5Bmode%5D=simple&arko_default_619517ef1a5d5--filtreGroupes%5Bop%5D=AND&arko_default_619517ef1a5d5--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61976d807f4e2%5D%5Bop%5D=NOT&arko_default_619517ef1a5d5--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61976d807f4e2%5D%5Bextras%5D%5Bmode%5D=select&arko_default_619517ef1a5d5--from=0&arko_default_619517ef1a5d5--resultSize=25&arko_default_619517ef1a5d5--contenuIds%5B%5D=156426&arko_default_619517ef1a5d5--modeRestit=arko_default_6195181087d5d",
    name="Saint Etienne",
    collumn_numbers=CollumnNumbers(
        cote=0,
        commune=1,
        act_types=2,
        period=4,
        image_count=6
    )
)

scraper.run_main()