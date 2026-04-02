"""
Stage 4: Search & Export
CLI tool to query the cases database and optionally export to JSON.

Usage:
    python search.py                          # List all cases
    python search.py --name "Smith"
    python search.py --company 4479
    python search.py --state "South Carolina"
    python search.py --export results.json
"""

import argparse
import json
import logging

from db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)


def search_cases(name: str = None, company: str = None, state: str = None) -> list[dict]:
    """Query the cases table using parsed_data JSON fields."""
    conn = get_connection()
    rows = conn.execute("SELECT case_id, na_id, case_text, parsed_data FROM cases").fetchall()
    conn.close()

    results = []
    for row in rows:
        parsed = json.loads(row["parsed_data"]) if row["parsed_data"] else {}

        # Apply filters
        if name and name.lower() not in parsed.get("name", "").lower():
            continue
        if company and company not in parsed.get("company", ""):
            continue
        if state and state.lower() not in parsed.get("home_state", "").lower():
            continue

        results.append({
            "case_id": row["case_id"],
            "na_id": row["na_id"],
            "parsed": parsed,
            "text_preview": (row["case_text"] or "")[:200],
        })

    return results


def print_results(results: list[dict]):
    """Pretty-print results to the terminal."""
    if not results:
        print("No matching cases found.")
        return

    print(f"\n{'='*60}")
    print(f" Found {len(results)} case(s)")
    print(f"{'='*60}\n")

    for r in results:
        parsed = r["parsed"]
        print(f"  Case #{r['case_id']}  |  NAID: {r['na_id']}")
        print(f"  {'-'*50}")
        if parsed.get("name"):
            print(f"    Name:           {parsed['name']}")
        if parsed.get("enrollee_id"):
            print(f"    Enrollee ID:    {parsed['enrollee_id']}")
        if parsed.get("company"):
            print(f"    Company:        Co. {parsed['company']}")
        if parsed.get("home_town"):
            print(f"    Home Town:      {parsed['home_town']}")
        if parsed.get("home_state"):
            print(f"    Home State:     {parsed['home_state']}")
        if parsed.get("enlistment_date"):
            print(f"    Enlisted:       {parsed['enlistment_date']}")
        if parsed.get("discharge_date"):
            print(f"    Discharged:     {parsed['discharge_date']}")
        if parsed.get("nearest_kin"):
            print(f"    Nearest Kin:    {parsed['nearest_kin']}")
        if not parsed:
            print(f"    (no fields extracted)")
            print(f"    Preview: {r['text_preview']}")
        print()


def export_to_json(results: list[dict], filepath: str):
    """Export results to a JSON file."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    log.info("Exported %d cases to %s", len(results), filepath)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search extracted CCC records.")
    parser.add_argument("--name",    type=str, help="Filter by enrollee name (partial match)")
    parser.add_argument("--company", type=str, help="Filter by company number")
    parser.add_argument("--state",   type=str, help="Filter by home state (partial match)")
    parser.add_argument("--export",  type=str, metavar="FILE", help="Export results to JSON file")
    args = parser.parse_args()

    results = search_cases(name=args.name, company=args.company, state=args.state)
    print_results(results)

    if args.export:
        export_to_json(results, args.export)
