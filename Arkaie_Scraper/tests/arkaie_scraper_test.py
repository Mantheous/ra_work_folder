import pytest
import os
import pandas as pd
from Arkaie_Scraper.arkaie_scraper import DebugConfig
from Civil_Status.Aube2.Aube2_scraper import Aube2Scraper

def test_aube2_scraper_integrity():
    # Setup test-specific paths
    test_csv = "tests/outputs/Aube2_test_results.csv"
    
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(test_csv), exist_ok=True)
    
    # Initialize scraper with "Safe" test settings
    scraper = Aube2Scraper(
        debug_config=DebugConfig(
            headless=False,       # CI/CD friendly
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