from contextlib import contextmanager
from playwright.sync_api import sync_playwright
 
@contextmanager
def load_browser(headless: bool = True):
    """Get a Playwright page with automatic cleanup"""
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        yield page
        browser.close()