"""
find_leftovers.py
-----------------
Finds images that were missed by the downloader by comparing what exists on
disk against the expected file structure derived from the progress CSV.

Usage:
    python find_leftovers.py <path_to_csv> [output_csv]

CSV format (pipe-delimited, no header):
    department|page|index|cote|commune|period|record_type|img_count|url

The url column contains the base URL with page 0; all other pages are derived
by substituting the page index into the URL path segment before "/full".

Output (missed_pages.csv) matches the format used by missed_downloader.py:
    department|page|index|cote|commune|period|record_type|<page_idx>|<page_url>
where each row represents a single missing image page.
"""

import asyncio
import os
import re
import sys
import pandas as pd
from pathlib import Path

# ─── Configuration ──────────────────────────────────────────────────────────
_THIS_DIR = Path(__file__).resolve().parent.parent
from Utilities.paths import PROJECT_ROOT

FRI_FOLDER_PATH = str(PROJECT_ROOT / "Civil_Status" / "Results")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def page_url(base_url: str, page_idx: int) -> str:
    """
    Derive the download URL for a specific page index from the base (page=0) URL.
    The URL contains a segment like /0/full or /272/full – replace that number.
    """
    return re.sub(r'/(\d+)/full', f'/{page_idx}/full', base_url)


def build_base_filename(dept: str, r_type: str, commune: str) -> str:
    return f"{dept}_{r_type}_{commune}_"


def detect_batches_on_disk(folder_path: str, base_filename: str) -> list[int]:
    """Return sorted list of batch numbers that have at least page 0 on disk."""
    if not os.path.isdir(folder_path):
        return []
    batches = []
    batch = 0
    while os.path.isfile(os.path.join(folder_path, f"{base_filename}{batch}_0.jpg")):
        batches.append(batch)
        batch += 1
    return batches


def check_batch(folder_path: str, base_filename: str, batch: int,
                expected_count: int) -> list[int]:
    """Return list of missing page indices for a given batch."""
    missing = []
    for page in range(expected_count):
        fp = os.path.join(folder_path, f"{base_filename}{batch}_{page}.jpg")
        if not os.path.isfile(fp):
            missing.append(page)
    return missing


# ─── Core ────────────────────────────────────────────────────────────────────

def _check_row(row_index: int, row) -> tuple[int, list[list]]:
    """
    Synchronous worker that checks one CSV row's folder for missing pages.
    Returns (row_index, list_of_missing_rows) so results can be re-sorted.
    """
    dept      = str(row.department)
    r_type    = str(row.record_type)
    commune   = str(row.commune)
    period    = str(row.period)
    img_count = int(row.img_count)
    base_url  = str(row.url)

    folder_path   = os.path.join(FRI_FOLDER_PATH, dept, r_type, commune, period)
    base_filename = build_base_filename(dept, r_type, commune)

    # ── Batch detection ─────────────────────────────────────────────
    batches_on_disk = detect_batches_on_disk(folder_path, base_filename)

    missing_rows = []

    if not batches_on_disk:
        print(
            f"[MISSING BATCH] {folder_path}\n"
            f"  → batch 0 completely absent (expected {img_count} images)"
        )
        for page in range(img_count):
            missing_rows.append([
                row.department, row.page, row.index, row.cote,
                row.commune, row.period, row.record_type,
                page,
                page_url(base_url, page),
            ])
        return row_index, missing_rows

    if len(batches_on_disk) > 1:
        print(
            f"[EXTRA BATCHES] {folder_path}\n"
            f"  → Found batches {batches_on_disk}, expected only batch 0"
        )

    batch = batches_on_disk[0]
    missing_pages = check_batch(folder_path, base_filename, batch, img_count)

    if missing_pages:
        print(
            f"[INCOMPLETE] {folder_path} | batch {batch} "
            f"→ {len(missing_pages)}/{img_count} missing"
        )
        for page in missing_pages:
            missing_rows.append([
                row.department, row.page, row.index, row.cote,
                row.commune, row.period, row.record_type,
                page,
                page_url(base_url, page),
            ])

    return row_index, missing_rows


async def find_leftovers(df: pd.DataFrame, output_csv: str):
    """
    For each row in *df* (which represents one register/volume), concurrently:
      1. Compute the expected folder path.
      2. Determine how many batches should exist.
      3. Check each expected page file; collect missing ones.
      4. Write all missing pages to *output_csv* in missed_pages format.

    All folder checks run in parallel via asyncio.to_thread (thread-pool),
    so all folders are examined at the same time rather than sequentially.
    """
    # Launch every row check concurrently in threads
    tasks = [
        asyncio.to_thread(_check_row, i, row)
        for i, row in enumerate(df.itertuples())
    ]
    results = await asyncio.gather(*tasks)

    # Re-sort by original row index to keep output order deterministic
    results.sort(key=lambda x: x[0])

    missing_rows = []
    total_missing = 0
    total_checked = int(df['img_count'].sum())

    for _idx, rows in results:
        missing_rows.extend(rows)
        total_missing += len(rows)

    # ── Write output ────────────────────────────────────────────────────
    if missing_rows:
        out_df = pd.DataFrame(missing_rows)
        out_df.to_csv(output_csv, index=False, header=False, sep='|')
        print(f"\n✓ Wrote {len(missing_rows)} missing-page rows → {output_csv}")
    else:
        print("\n✓ No missing pages found. Nothing written.")

    print(f"\nSummary: {total_missing} missing / {total_checked} total pages checked")


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":

    csv_file   = "ra_work_folder\Civil_Status\Aube2\Aube2_cleaned.csv"
    output_csv = sys.argv[2] if len(sys.argv) > 2 else "ra_work_folder\Civil_Status\Aube2\missed_pages.csv"

    if not os.path.exists(csv_file):
        print(f"Error: CSV file not found: {csv_file}")
        sys.exit(1)

    column_headers = [
        'department', 'page', 'index', 'cote',
        'commune', 'period', 'record_type', 'img_count', 'url'
    ]
    df = pd.read_csv(csv_file, header=None, delimiter='|', names=column_headers)

    # Sanitize spaces → underscores (same as downloader.py)
    sanitize_cols = ['department', 'record_type', 'commune', 'period']
    for col in sanitize_cols:
        df[col] = df[col].astype(str).str.replace(" ", "_", regex=False)

    asyncio.run(find_leftovers(df, output_csv))