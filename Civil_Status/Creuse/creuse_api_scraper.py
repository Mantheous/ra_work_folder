"""
API-based scraper for the Creuse departmental archives.
https://archives.creuse.fr/rechercher/archives-numerisees/registres-paroissiaux-et-de-letat-civil

Advantages over the Playwright scraper (creuse_sraper.py):
  - No browser required: uses the JSON API directly via requests.
  - Captures ALL records, including those hidden from anonymous users
    by the visibilite-internaute-confirme CSS class.
  - Much faster and more reliable.

Output CSV format (pipe-delimited, no header):
  Department | Batch | Index | Cote | Commune | Period | ActType | ImageCount | ImageURL

As of March 2026.
"""

import sys
import re
import time
import requests
from pathlib import Path
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from Utilities.paths import PROJECT_ROOT

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://archives.creuse.fr"
API_URL = f"{BASE_URL}/_recherche-api/search/2"
IMAGE_DOWNLOAD_TEMPLATE = (
    "{base}/_recherche-images/download/{record_id}/image/{file_id}/0/full/max/0/default.jpg"
)

# Arko IDs extracted from the URLs (stable as of Feb 2026)
ARKO_MOTEUR = "arko_default_6089614c0e9ed"
ARKO_FIELD = "arko_default_608961eed93bb"
ARKO_GROUPE = "arko_default_6089619825455"
CONTENT_ID = "476687"
MODE_RESTIT = "arko_default_60896998ef13f"

# The scraper runs two passes — one excluding Mariages, one including only
# Mariages — to match the original run_split() approach in creuse_sraper.py.
FILTER_NOT_MARIAGES = {
    f"{ARKO_MOTEUR}--filtreGroupes[mode]": "simple",
    f"{ARKO_MOTEUR}--filtreGroupes[op]": "AND",
    f"{ARKO_MOTEUR}--filtreGroupes[groupes][0][{ARKO_GROUPE}][op]": "NOT",
    f"{ARKO_MOTEUR}--filtreGroupes[groupes][0][{ARKO_GROUPE}][q][]": "Mariages[[arko_fiche_60815ed983140]]",
    f"{ARKO_MOTEUR}--filtreGroupes[groupes][0][{ARKO_GROUPE}][extras][mode]": "select",
}

FILTER_ONLY_MARIAGES = {
    f"{ARKO_MOTEUR}--filtreGroupes[mode]": "simple",
    f"{ARKO_MOTEUR}--filtreGroupes[op]": "AND",
    f"{ARKO_MOTEUR}--filtreGroupes[groupes][0][{ARKO_GROUPE}][op]": "AND",
    f"{ARKO_MOTEUR}--filtreGroupes[groupes][0][{ARKO_GROUPE}][q][]": "Mariages[[arko_fiche_60815ed983140]]",
    f"{ARKO_MOTEUR}--filtreGroupes[groupes][0][{ARKO_GROUPE}][extras][mode]": "select",
}

PAGE_SIZE = 25          # API max recommended page size; 100 also works
DELAY_BETWEEN_PAGES = 1 # seconds, be polite
DEPARTMENT_NAME = "Creuse"
RECORDS_PER_BATCH = 100  # CSV batch size (rows per batch number); matches original


# ---------------------------------------------------------------------------
# HTML parsing helpers (all data lives in the `html` field of the API JSON)
# ---------------------------------------------------------------------------

def _text(tag, selector: str, attr: str | None = None) -> str:
    el = tag.select_one(selector)
    if el is None:
        return ""
    if attr:
        return el.get(attr, "").strip()
    return el.get_text(strip=True)


def parse_row(tr_tag) -> dict | None:
    """
    Parse a single <tr class="resultat_container ..."> into a dict with keys:
      commune, cote, act_types, period, image_count, image_url
    Returns None if the row has no image data.
    """
    # ---------- Commune ----------
    commune_span = tr_tag.select_one("span[data-champ='commune_intitule']")
    commune = commune_span.get_text(strip=True) if commune_span else ""

    # ---------- Cote ----------
    cote_span = tr_tag.select_one("span[data-champ='cote']")
    cote = cote_span.get_text(strip=True) if cote_span else ""

    # ---------- Act types (may be multiple <li> items) ----------
    act_spans = tr_tag.select("span[data-champ='type_acte']")
    act_types = "_".join(s.get_text(strip=True) for s in act_spans)

    # ---------- Period ----------
    date_span = tr_tag.select_one("span[data-champ='date']")
    period = date_span.get_text(strip=True).replace(" ", "_") if date_span else ""

    # ---------- Image count & URL ----------
    # The eyeball button holds a data-visionneuse JSON with idArkoFile
    btn = tr_tag.select_one("button.bouton_rond.visualiser")
    if btn is None:
        # No images available for this record
        return {
            "commune": commune,
            "cote": cote,
            "act_types": act_types,
            "period": period,
            "image_count": "",
            "image_url": "",
        }

    import json
    visionneuse_json = btn.get("data-visionneuse", "{}")
    try:
        visionneuse = json.loads(visionneuse_json)
    except json.JSONDecodeError:
        visionneuse = {}

    id_arko_file = visionneuse.get("idArkoFile", "")

    # Image count from the nombre_images span: "(740 images)" → 740
    count_span = tr_tag.select_one("span.nombre_images")
    image_count = ""
    if count_span:
        m = re.search(r"\((\d+)\s+images?\)", count_span.get_text())
        if m:
            image_count = m.group(1)

    return {
        "commune": commune,
        "cote": cote,
        "act_types": act_types,
        "period": period,
        "image_count": image_count,
        "id_arko_file": id_arko_file,
    }


def build_image_url(record_id: int, id_arko_file: int | str) -> str:
    if not id_arko_file:
        return ""
    return IMAGE_DOWNLOAD_TEMPLATE.format(
        base=BASE_URL,
        record_id=record_id,
        file_id=id_arko_file,
    )


# ---------------------------------------------------------------------------
# Core fetching logic
# ---------------------------------------------------------------------------

def fetch_page(filter_params: dict, offset: int, session: requests.Session) -> dict:
    """Call the search API and return the parsed JSON."""
    params = {
        f"{ARKO_MOTEUR}--ficheFocus": "",
        f"{ARKO_MOTEUR}--from": str(offset),
        f"{ARKO_MOTEUR}--resultSize": str(PAGE_SIZE),
        f"{ARKO_MOTEUR}--contenuIds[]": CONTENT_ID,
        f"{ARKO_MOTEUR}--modeRestit": MODE_RESTIT,
        **filter_params,
    }
    resp = session.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def scrape_filter(
    filter_params: dict,
    csv_path: Path,
    batch_start: int = 1,
    row_counter_start: int = 0,
    session: requests.Session | None = None,
) -> tuple[int, int]:
    """
    Paginate through all results for a given filter and write rows to csv_path.

    Returns (next_batch_start, next_row_counter).
    """
    if session is None:
        session = requests.Session()

    offset = 0
    batch_number = batch_start
    # within-batch counter (1-indexed, resets at RECORDS_PER_BATCH)
    within_batch = (row_counter_start % RECORDS_PER_BATCH) or 0
    global_index = row_counter_start  # absolute record index since start

    total = None

    while True:
        print(f"  Fetching offset {offset} ...", end=" ", flush=True)
        data = fetch_page(filter_params, offset, session)

        if total is None:
            total = data.get("total", 0)
            print(f"(total={total})")
        else:
            print()

        html_blob = data.get("html", "")
        results_meta = data.get("results", [])  # parallel list with record IDs
        count = data.get("count", 0)

        if not html_blob or count == 0:
            break

        soup = BeautifulSoup(html_blob, "html.parser")
        rows = soup.select("tr.resultat_container")

        for i, tr in enumerate(rows):
            record_meta = results_meta[i] if i < len(results_meta) else {}
            record_id = record_meta.get("id", "")

            row_data = parse_row(tr)
            if row_data is None:
                continue

            global_index += 1
            within_batch += 1

            # Advance batch number when we exceed RECORDS_PER_BATCH
            if within_batch > RECORDS_PER_BATCH:
                batch_number += 1
                within_batch = 1

            commune = row_data["commune"].strip().replace(" ", "_")
            period = row_data["period"]
            cote = row_data["cote"]
            act_types = row_data["act_types"].replace(" ", "_")
            image_count = row_data.get("image_count", "")
            id_arko_file = row_data.get("id_arko_file", "")
            image_url = build_image_url(record_id, id_arko_file)

            csv_line = (
                f"{DEPARTMENT_NAME}|{batch_number}|{within_batch}|"
                f"{cote}|{commune}|{period}|{act_types}|{image_count}|{image_url}"
            )

            with open(csv_path, "a", encoding="utf-8") as f:
                f.write(csv_line + "\n")

        offset += count
        if offset >= total:
            break

        time.sleep(DELAY_BETWEEN_PAGES)

    return batch_number, global_index


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    csv_path = Path(PROJECT_ROOT) / "Civil_Status" / "Creuse" / "Creuse_api.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # Clear/create the output file
    csv_path.write_text("", encoding="utf-8")

    session = requests.Session()
    session.headers.update({
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"{BASE_URL}/rechercher/archives-numerisees/registres-paroissiaux-et-de-letat-civil",
    })

    print("=== Pass 1: Everything EXCEPT Mariages ===")
    last_batch, last_row = scrape_filter(
        filter_params=FILTER_NOT_MARIAGES,
        csv_path=csv_path,
        batch_start=1,
        row_counter_start=0,
        session=session,
    )
    print(f"Pass 1 complete. Last batch={last_batch}, total rows={last_row}\n")

    print("=== Pass 2: Mariages ONLY ===")
    last_batch, last_row = scrape_filter(
        filter_params=FILTER_ONLY_MARIAGES,
        csv_path=csv_path,
        batch_start=last_batch,
        row_counter_start=last_row,
        session=session,
    )
    print(f"Pass 2 complete. Last batch={last_batch}, total rows={last_row}")
    print(f"\nDone! Output written to: {csv_path}")


if __name__ == "__main__":
    main()
