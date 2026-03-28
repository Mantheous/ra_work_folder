"""
Shared SQLite database module for the NARA CCC extractor pipeline.
Creates and manages the ccc_records.db database with tables for
records, pages, and cases.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "ccc_records.db"


def get_connection():
    """Return a connection to the SQLite database, creating tables if needed."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # Better concurrency
    _create_tables(conn)
    return conn

# Right now we are just getting the pages table. I know what the cases table needs, but we aren't implementing it yet
def _create_tables(conn):
    """Create the database schema if it does not already exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS hits (
            na_id           TEXT PRIMARY KEY,    
            record_group    TEXT NOT NULL,
            record_title    TEXT NOT NULL,
            object_id_start INT NOT NULL, -- Starting page of the record EI the object id of the first page
            object_id_end   INT NOT NULL, -- Ending page of the record EI the object id of the last page
            status          TEXT NOT NULL DEFAULT 'pending'
        );
        CREATE INDEX IF NOT EXISTS idx_hits_naid   ON hits(na_id);

        CREATE TABLE IF NOT EXISTS extraction_chunks (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            na_id           TEXT NOT NULL REFERENCES hits(na_id),
            chunk_page      INT  NOT NULL,      -- the ?page= param (1-indexed)
            total_objects   INT,                -- "total" from the API response
            extracted_text  TEXT,               -- JSON array of page objects from digitalObjects in this chunk
            status          TEXT NOT NULL DEFAULT 'pending',  -- pending | done | failed
            UNIQUE(na_id, chunk_page)
        );
        CREATE INDEX IF NOT EXISTS idx_chunks_naid ON extraction_chunks(na_id);

        CREATE TABLE IF NOT EXISTS cases (
            case_start_id   TEXT PRIMARY KEY, -- Object Id of the first page
            case_end_id     TEXT, -- Object Id of the last page
            na_id           TEXT NOT NULL REFERENCES hits(na_id),
            extracted_text  TEXT              -- JSON array of pages in the case
            json            TEXT -- JSON of data processed with NLP
        );
        CREATE INDEX IF NOT EXISTS idx_cases_naid ON cases(na_id);
    """)
    conn.commit()
