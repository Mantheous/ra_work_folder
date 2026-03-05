import re

# These functions assume the context of the table page
def navigate_to_commune_list(page):
    # for some sites the button is hidden
    try:
        page.get_by_label('Consulter la liste').first.click()
    except:
        commune_search = page.get_by_label(re.compile(r"Commune", re.IGNORECASE))
        if commune_search.count() == 1:
            commune_search.click()        
        page.get_by_label('Consulter la liste').first.click()

def get_letters(page):
    return page.locator(".filtre_liste_popup_nav").locator("ul")
