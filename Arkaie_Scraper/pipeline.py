from enum import Enum
from Civil_Status.Aube2.Aube2_scraper import Aube2Scraper
from ra_work_folder.Arkaie_Scraper.arkaie_scraper import DebugConfig
from ra_work_folder.Utilities.validate_csv import validate
import pandas as pd

class Scrapers(Enum):
    """Enumeration of available department scrapers."""
    Aube = None
    Aube2 = Aube2Scraper(
        debug_config=DebugConfig(headless=True,),
        starting_page=0,
    )
    Cher = None


def main(starting_phase: int, scraper: Scrapers):
    """Main pipeline runner for a specified department scraper."""
    if scraper.value is None:
        print("Scraper not implemented yet.")
        return
    if starting_phase <= 1:
        scraper.run_main()
    
    validate(scraper.value.csv_location, scraper.value.number_of_records)
    # Format for downloading
    # Download images



if __name__ == "__main__":
    main(1, Scrapers.Aube2)