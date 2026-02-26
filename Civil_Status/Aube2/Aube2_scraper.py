# Scraper for "https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-de-la-ville-de-troyes-1535-1919?arko_default_62289d8b205f4--ficheFocus="
# As of Febuary 2026

import sys
sys.path.append("W:\\RA_work_folders\\Ashton_Reed")
from Arkaie_Scraper.arkaie_scraper import ArkaieScraper, CollumnNumbers, DebugConfig

class Aube2Scraper(ArkaieScraper):
    '''
    The primary variation in this scraper is that the "cote" is not available on the main page, 
    so we have to enter the viewer to get it. See ArkaieScraper for general functionality.
    '''
    def __init__(self, debug_config: DebugConfig):
        super().__init__(
            root_link="https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-de-la-ville-de-troyes-1535-1919?arko_default_62289d8b205f4--ficheFocus=",
            name="Aube2",
            department="Aube",
            collumn_numbers=CollumnNumbers(
                cote=None,
                commune=0,
                act_types=2,
                period=1,
                image_count=4
            ),
            debug_config=debug_config
        )

    def enter_viewer(self, row):
        self.navigate_to_download_link(row)
        cote = self.page.locator('h1[data-cy="image-legende"]').inner_text().split(", ")[1]
        href = self.page.locator('a.exporter').get_attribute("href")
        self.page.locator('button.close').click()
        return str(href), cote
    
    def process_row(self, i: int):
        try:
            self.wait_for_load()
            table_body = self.page.locator('tbody')
            row = table_body.locator('tr').nth(i)
            # Copy down some data from the table
            commune = row.locator("td").nth(self.collumn_numbers.commune).inner_text().replace('\n', '_')
            period = row.locator("td").nth(self.collumn_numbers.period).inner_text()
            act_types = row.locator("td").nth(self.collumn_numbers.act_types).inner_text().replace('\n', '_')

            if row.locator("td").nth(self.collumn_numbers.image_count).inner_text() == '':
                print(f"No images in row {i+1}")
                # we need to write unaccesible records to verify we scraped every record we could
                self.write_row(i, "", commune, period, act_types, "", "")
                return
            image_count = int(row.locator("td").nth(self.collumn_numbers.image_count).inner_text().split()[0][1:])
            href, cote = self.enter_viewer(row)

        except Exception as e:
            self.recover_row_fail(i, e)
            return

        self.write_row(i, cote, commune, period, act_types, image_count, href)




scraper = Aube2Scraper(
    debug_config=DebugConfig(
        headless=False, 
        one_per_page=False, 
        raise_exceptions=False
        )
    )

scraper.run_main()

