import sys
from unittest import result
import pytest
import os
import pandas as pd
sys.path.append("W:\\RA_work_folders\\Ashton_Reed\\ra_work_folder")
from Arkaie_Scraper.arkaie_scraper import ArkaieScraper, DebugConfig
from Civil_Status.Aube2.Aube2_scraper import Aube2Scraper
from Civil_Status.Cher.cher_scraper import CherScraper

def test_url_for_page_number_filtered():
    filter_url = "https://www.archives18.fr/archives-numerisees/registres-paroissiaux-et-etat-civil?arko_default_61011a8e5db65--ficheFocus=&arko_default_61011a8e5db65--filtreGroupes%5Bmode%5D=simple&arko_default_61011a8e5db65--filtreGroupes%5Bop%5D=AND&arko_default_61011a8e5db65--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61011b4c3eacb%5D%5Bop%5D=AND&arko_default_61011a8e5db65--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61011b4c3eacb%5D%5Bq%5D%5B%5D=&arko_default_61011a8e5db65--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61011b4c3eacb%5D%5Bq%5D%5B%5D=Ach%C3%A8res%5B%5Barko_fiche_615ab249b84b4%5D%5D&arko_default_61011a8e5db65--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61011b4c3eacb%5D%5Bextras%5D%5Bmode%5D=popup&arko_default_61011a8e5db65--from=0&arko_default_61011a8e5db65--resultSize=25&arko_default_61011a8e5db65--contenuIds%5B%5D=2655740&arko_default_61011a8e5db65--modeRestit=arko_default_61011eb03aad2"
    page_number = 1
    scraper = ArkaieScraper()
    scraper.filter_link = filter_url
    scraper.page_number = 0
    scraper.results_per_page = 100
    result_url = scraper.url_for_page_number_filtered(page_number)
    assert result_url == "https://www.archives18.fr/archives-numerisees/registres-paroissiaux-et-etat-civil?arko_default_61011a8e5db65--ficheFocus=&arko_default_61011a8e5db65--filtreGroupes%5Bmode%5D=simple&arko_default_61011a8e5db65--filtreGroupes%5Bop%5D=AND&arko_default_61011a8e5db65--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61011b4c3eacb%5D%5Bop%5D=AND&arko_default_61011a8e5db65--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61011b4c3eacb%5D%5Bq%5D%5B%5D=&arko_default_61011a8e5db65--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61011b4c3eacb%5D%5Bq%5D%5B%5D=Ach%C3%A8res%5B%5Barko_fiche_615ab249b84b4%5D%5D&arko_default_61011a8e5db65--filtreGroupes%5Bgroupes%5D%5B0%5D%5Barko_default_61011b4c3eacb%5D%5Bextras%5D%5Bmode%5D=popup&arko_default_61011a8e5db65--from=0&arko_default_61011a8e5db65--resultSize=100&arko_default_61011a8e5db65--contenuIds%5B%5D=2655740&arko_default_61011a8e5db65--modeRestit=arko_default_61011eb03aad2"



def test_aube2_scraper_integrity():
    # Setup test-specific paths
    test_csv = "tests/outputs/Aube2_test_results.csv"

    # empty the test file
    if os.path.exists(test_csv):
        os.remove(test_csv)
    
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(test_csv), exist_ok=True)
    
    # Initialize scraper with "Safe" test settings
    scraper = Aube2Scraper(
        debug_config=DebugConfig(
            headless=True,       # CI/CD friendly
            one_per_page=True,   # Speed up the test
            raise_exceptions=True, # We WANT it to crash if something is broken
            stoping_page=2           # Only check first few pages to verify logic
        ),
        starting_page=1,
        csv_location=test_csv,
    )

    # 1. Execution Phase
    try:
        scraper.run_main()
    except Exception as e:
        pytest.fail(f"Scraper crashed during execution: {e}")

    # 2. Validation Phase (The "Verify" part)
    assert os.path.exists(test_csv), "CSV file was not created."
    
    df = pd.read_csv(test_csv)
    assert not df.empty, "Scraper finished but returned no data."
    
    # Check for critical columns that your base scraper should provide
    expected_columns = ["Commune", "Year", "URL"] # Update these to your actual columns
    for col in expected_columns:
        assert col in df.columns, f"Missing expected column: {col}"

def test_cher_scraper_integrity():
    # Setup test-specific paths
    test_csv = "tests/outputs/Cher_test_results.csv"

    # empty the test file
    if os.path.exists(test_csv):
        os.remove(test_csv)
    
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(test_csv), exist_ok=True)
    
    # Initialize scraper with "Safe" test settings
    scraper = CherScraper(
        debug_config=DebugConfig(
            headless=True,       # CI/CD friendly
            one_per_page=True,   # Speed up the test
            raise_exceptions=True, # We WANT it to crash if something is broken
            stoping_page=2           # Only check first few pages to verify logic
        ),
        starting_page=1,
        csv_location=test_csv,
    )

    # 1. Execution Phase
    try:
        scraper.run_filtered()
    except Exception as e:
        pytest.fail(f"Scraper crashed during execution: {e}")

    # 2. Validation Phase (The "Verify" part)
    assert os.path.exists(test_csv), "CSV file was not created."
    
    df = pd.read_csv(test_csv)
    assert not df.empty, "Scraper finished but returned no data."
    
    # Check for critical columns that your base scraper should provide
    expected_columns = ["Commune", "Year", "URL"] # Update these to your actual columns
    for col in expected_columns:
        assert col in df.columns, f"Missing expected column: {col}"

if __name__ == "__main__":
    test_url_for_page_number_filtered()