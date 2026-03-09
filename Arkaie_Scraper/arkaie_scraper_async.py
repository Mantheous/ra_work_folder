# Async, parallel version of the Arkaïe scraper.
# Spawns one Playwright browser page per results-page and scrapes them
# concurrently, bounded by CONCURRENCY_LIMIT (like downloader.py).
#
# Compatible with the same websites as arkaie_scraper.py:
#   - https://archives.creuse.fr/...
#   - https://www.archives-aube.fr/...

import sys
import re
import asyncio
import threading
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as AsyncTimeoutError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from Utilities.paths import PROJECT_ROOT
from Utilities.notifier import notify
from urllib.parse import urlparse, urlencode, urlunparse

# ── tuneable constant (mirrors CONCURRENCY_LIMIT in downloader.py) ──────────
CONCURRENCY_LIMIT = 50

DEFAULT_URL = "https://archives.creuse.fr"


class CollumnNumbers:
    def __init__(
        self,
        cote: int = 0,
        commune: int = 1,
        act_types: int = 2,
        period: int = 3,
        image_count: int = 4,
    ):
        self.cote = cote
        self.commune = commune
        self.act_types = act_types
        self.period = period
        self.image_count = image_count


class DebugConfig:
    """
    headless       – run without a visible browser window (faster)
    one_per_page   – scrape only the first row per page (for quick tests)
    raise_exceptions – re-raise instead of silently retrying (for dev)
    stoping_page   – stop after this page number (for tests)
    notify_crash   – send a notification on crash
    """

    def __init__(
        self,
        headless: bool,
        one_per_page: bool = False,
        raise_exceptions: bool = False,
        stoping_page: int = None,  # pyright: ignore[reportArgumentType]
        notify_crash: bool = True,
    ):
        self.headless = headless
        self.one_per_page = one_per_page
        self.raise_exceptions = raise_exceptions
        self.stoping_page = stoping_page
        self.notify_crash = notify_crash


class ArkaieScraperAsync:
    """
    Drop-in parallel replacement for ArkaieScraper.

    Each results-page is processed in its own Playwright browser page.
    Up to CONCURRENCY_LIMIT pages run at the same time (asyncio.Semaphore).

    CSV writes are serialised with a threading.Lock so concurrent coroutines
    never interleave partial lines.
    """

    def __init__(
        self,
        root_link: str = DEFAULT_URL,
        name: str = "Default",
        collumn_numbers: CollumnNumbers = CollumnNumbers(),
        filter_link: str = None,  # pyright: ignore[reportArgumentType]
        csv_location: str = None,  # pyright: ignore[reportArgumentType]
        results_per_page: int = 100,
        starting_page: int = 1,
        starting_index: int = 0,
        max_tries: int = 5,
        concurrency_limit: int = CONCURRENCY_LIMIT,
        debug_config: DebugConfig = DebugConfig(
            headless=False,
            one_per_page=False,
            stoping_page=None,  # pyright: ignore[reportArgumentType]
        ),
        department: str = None,  # pyright: ignore[reportArgumentType]
    ):
        self.root_link = root_link
        self.name = name
        self.collumn_numbers = collumn_numbers

        self.filter_link = filter_link if filter_link is not None else root_link
        self.csv_location = (
            csv_location
            if csv_location is not None
            else str(PROJECT_ROOT / "Civil_Status" / name / f"{name}.csv")
        )
        self.department = department if department is not None else name

        self.results_per_page = results_per_page
        self.starting_page = starting_page
        assert starting_page > 0
        self.starting_index = starting_index
        self.max_tries = max_tries
        self.concurrency_limit = concurrency_limit
        self.debug_config = debug_config

        # Shared state
        self._csv_lock = threading.Lock()
        self._number_of_records: int | None = None

    # ── public entry point ───────────────────────────────────────────────────

    def run_main(self):
        """Synchronous wrapper so callers don't need to know about asyncio."""
        asyncio.run(self._run_async())

    # ── core async logic ─────────────────────────────────────────────────────

    async def _run_async(self):
        semaphore = asyncio.Semaphore(self.concurrency_limit)

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=self.debug_config.headless)

            # ── Step 1: discover total record count from page 1 ──────────────
            probe_ctx = await browser.new_context()
            probe_page = await probe_ctx.new_page()
            await probe_page.goto(self._url_for_page(1))
            await self._wait_for_load(probe_page)

            try:
                raw = await probe_page.locator(
                    "div.nombre_resultat_facettes"
                ).first.inner_text()
                self._number_of_records = int(
                    raw.replace("\u202f", "").split()[0]
                )
            except AsyncTimeoutError:
                self._number_of_records = 1

            await probe_ctx.close()

            total_pages = (self._number_of_records // self.results_per_page) + 1
            print(
                f"[{self.name}] {self._number_of_records} records → {total_pages} pages "
                f"(concurrency={self.concurrency_limit})"
            )

            # ── Step 2: build page range respecting debug limits ─────────────
            last_page = total_pages
            if (
                self.debug_config.stoping_page is not None
                and self.debug_config.stoping_page < last_page
            ):
                last_page = self.debug_config.stoping_page

            page_numbers = range(self.starting_page, last_page + 1)

            # ── Step 3: scatter all pages as concurrent tasks ─────────────────
            tasks = [
                self._scrape_page_task(browser, semaphore, pn)
                for pn in page_numbers
            ]
            await asyncio.gather(*tasks)

            await browser.close()

        notify(
            message=f"{self.name} async scraper has finished",
            subject=f"{self.name} Finished",
        )

    async def _scrape_page_task(self, browser, semaphore: asyncio.Semaphore, page_number: int):
        """Acquire the semaphore, open a fresh browser context, scrape one results-page."""
        async with semaphore:
            ctx = await browser.new_context()
            page = await ctx.new_page()
            tries = 0

            while True:
                try:
                    await page.goto(self._url_for_page(page_number))
                    await self._wait_for_load(page)
                    await self._scrape_page(page, page_number)
                    break  # success
                except Exception as e:
                    tries += 1
                    if self.debug_config.raise_exceptions or tries > self.max_tries:
                        if self.debug_config.notify_crash:
                            notify(
                                message=f"{self.name} failed on page {page_number} after {tries} tries: {e}",
                                subject=f"{self.name} Scraper Error",
                            )
                        print(f"[{self.name}] Giving up on page {page_number}: {e}")
                        break
                    print(
                        f"[{self.name}] Page {page_number} error (try {tries}): {e} – retrying…"
                    )

            await ctx.close()

    # ── per-page scraping ────────────────────────────────────────────────────

    async def _scrape_page(self, page, page_number: int):
        print(f"[{self.name}] Page {page_number}:")
        row_count = await self._count_rows(page)

        if self.debug_config.one_per_page:
            print(f"  Page {page_number} | Row 1/{row_count}")
            await self._process_row(page, page_number, 0)
        else:
            start = self.starting_index if page_number == self.starting_page else 0
            for i in range(start, row_count):
                print(f"  Page {page_number} | Row {i + 1}/{row_count}")
                await self._process_row(page, page_number, i)

    async def _process_row(self, page, page_number: int, i: int):
        tries = 0
        while True:
            try:
                table_body = page.locator("tbody")
                row = table_body.locator("tr").nth(i)

                commune = (
                    await row.locator("td").nth(self.collumn_numbers.commune).inner_text()
                ).replace("\n", "_")
                period = await row.locator("td").nth(self.collumn_numbers.period).inner_text()
                cote = await row.locator("td").nth(self.collumn_numbers.cote).inner_text()
                act_types = (
                    await row.locator("td").nth(self.collumn_numbers.act_types).inner_text()
                ).replace("\n", "_")

                count_text = await row.locator("td").nth(self.collumn_numbers.image_count).inner_text()
                if count_text == "":
                    print(f"  Page {page_number} | Row {i + 1}: no images")
                    self._write_row(page_number, i, cote, commune, period, act_types, "", "")
                    return

                image_count = int(count_text.split()[0][1:])
                href = await self._enter_viewer(page, row)
                self._write_row(page_number, i, cote, commune, period, act_types, image_count, href)
                return  # success

            except Exception as e:
                tries += 1
                if self.debug_config.raise_exceptions or tries > self.max_tries:
                    print(f"  Giving up on page {page_number} row {i + 1}: {e}")
                    return
                print(
                    f"  Page {page_number} | Row {i + 1} error (try {tries}): {e} – retrying…"
                )
                # reload the page before retrying the row
                await page.goto(self._url_for_page(page_number))
                await self._wait_for_load(page)

    # ── viewer navigation ────────────────────────────────────────────────────

    async def _enter_viewer(self, page, row) -> str:
        await self._navigate_to_download_link(page, row)
        href = await page.locator("a.exporter").get_attribute("href")
        await page.wait_for_load_state("networkidle")
        await page.locator("button.close").click()
        return str(href)

    async def _navigate_to_download_link(self, page, row):
        await row.locator("td").nth(self.collumn_numbers.image_count).locator("button").click()
        await page.wait_for_load_state("networkidle")

        try:
            await page.locator("button[data-cy='btn-toggle-volet']").click()
        except Exception:
            await self._click_terms(page)
            await page.locator("button[data-cy='btn-toggle-volet']").click()

        await page.mouse.move(0, 0)
        await page.locator('button[title="Partage et impression"]').click()

    async def _click_terms(self, page):
        await page.locator("button[data-cy='accept-license']").click()
        await page.wait_for_load_state("networkidle")

    # ── helpers ──────────────────────────────────────────────────────────────

    async def _count_rows(self, page) -> int:
        await self._wait_for_load(page)
        return await page.locator("tbody").locator("tr").count()

    async def _wait_for_load(self, page):
        try:
            await page.wait_for_selector(".loading-ring", state="visible", timeout=2000)
            await page.wait_for_selector(".loading-ring", state="hidden")
        except Exception:
            pass

    def _url_for_page(self, page_number: int) -> str:
        """Delegate to the correct URL builder based on whether a filter is in use."""
        if self.root_link == self.filter_link:
            return self._url_for_page_number(page_number)
        return self._url_for_page_number_filtered(page_number)

    def _url_for_page_number(self, page_number: int) -> str:
        match = re.search(r"arko_default_([a-z0-9]+)", self.root_link)
        if not match:
            raise ValueError("Could not find the Arko ID in the base URL.")
        arko_id = match.group(1)
        prefix = f"arko_default_{arko_id}"
        record_offset = (page_number - 1) * self.results_per_page
        query_params = {
            f"{prefix}--ficheFocus": "",
            f"{prefix}--from": str(record_offset),
            f"{prefix}--resultSize": str(self.results_per_page),
        }
        parsed_url = urlparse(self.root_link)
        return urlunparse(parsed_url._replace(query=urlencode(query_params)))

    def _url_for_page_number_filtered(self, page_number: int) -> str:
        arko_match = re.search(r"arko_default_[a-z0-9]+", self.filter_link)
        if not arko_match:
            raise ValueError("Could not find the Arko ID in the base URL.")
        record_offset = (page_number - 1) * self.results_per_page
        new_url = re.sub(r"(--from=)[^&]*", "--from=" + str(record_offset), self.filter_link)
        new_url = re.sub(r"(--resultSize=)[^&]*", "--resultSize=" + str(self.results_per_page), new_url)
        return new_url

    def _write_row(self, page_number, i, cote, commune, period, act_types, image_count, href):
        row_data = (
            f"{self.department}|{page_number}|{i + 1}|{cote}"
            f"|{commune.strip().replace(' ', '_')}"
            f"|{period.replace(' ', '_')}"
            f"|{act_types.replace(chr(10), ', ').replace(' ', '_')}"
            f"|{image_count}|{href}"
        )
        with self._csv_lock:
            with open(self.csv_location, "a", encoding="utf-8") as f:
                f.write(row_data + "\n")


# ── example usage ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = ArkaieScraperAsync(
        root_link="https://archives.creuse.fr/rechercher/archives-numerisees/registres-paroissiaux-et-de-letat-civil?arko_default_6089614c0e9ed--ficheFocus=",
        name="Creuse",
        concurrency_limit=CONCURRENCY_LIMIT,
        debug_config=DebugConfig(headless=True),
    )
    scraper.run_main()
