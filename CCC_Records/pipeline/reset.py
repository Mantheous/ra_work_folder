# Resets the database
# Use with care! It is not hard to re-run the pipeline, but it will take a while.
# It may also go out of date and require modification to successfully run. 
# Only run this if you are willing to re-run the pipeline and fix it if it's broken.

import sqlite3
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Modes
# 1. Reset everything               - Clears the whole database file
# 2. Reset to after search          - Clears all but hits table
# 3. Reset to after text extraction - Clears extracted_text column in extraction_chunks table, and cases table
# 4. Reset to after segmenter       - Clears cases table
# 5. Reset to after nlp             - Clears json column in cases table

MODE = 5

DB_PATH = Path(__file__).resolve().parent / "ccc_records.db"

def reset_database(mode: int):
    print(f"Executing reset for Mode {mode}...")
    if mode == 1:
        if DB_PATH.exists():
            DB_PATH.unlink()
        print("Mode 1: Reset everything - Cleared the whole database.")
        return

    if not DB_PATH.exists():
        print("Database does not exist.")
        return

    conn = sqlite3.connect(DB_PATH)

    if mode == 2:
        conn.execute("DELETE FROM cases")
        conn.execute("DELETE FROM extraction_chunks")
        conn.execute("UPDATE hits SET status = 'pending'")
        print("Mode 2: Reset to after search - Cleared all but hits table.")

    elif mode == 3:
        conn.execute("DELETE FROM cases")
        conn.execute("UPDATE extraction_chunks SET extracted_text = NULL, status = 'pending'")
        conn.execute("UPDATE hits SET status = 'pending'")
        print("Mode 3: Reset to after text extraction - Cleared extracted_text column in extraction_chunks table, and cases table.")

    elif mode == 4:
        conn.execute("DELETE FROM cases")
        print("Mode 4: Reset to after segmenter - Cleared cases table.")

    elif mode == 5:
        conn.execute("UPDATE cases SET json = NULL")
        print("Mode 5: Reset to after nlp - Cleared json column in cases table.")

    else:
        print(f"Invalid mode: {mode}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    reset_database(MODE)
