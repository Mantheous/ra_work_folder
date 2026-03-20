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


def _create_tables(conn):
    """Create the database schema if it does not already exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pages (
            object_id   TEXT PRIMARY KEY,
            na_id       TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'pending',
            raw_text    TEXT
        );

        CREATE TABLE IF NOT EXISTS cases (
            case_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            na_id           TEXT NOT NULL,

            -- Meta Data --
            query           TEXT NOT NULL,
            url             TEXT NOT NULL, -- "https://catalog.archives.gov/id/{NAID}?objectPage={PAGE}"
            starting_page   INT NOT NULL, -- NAID - Object ID
            ending_page     INT NOT NULL, -- starting page - Object ID
            record_group    TEXT NOT NULL,
            title           TEXT NOT NULL,

            -- Extracted Data --
            case_text       TEXT,
            json_data       TEXT,  -- JSON blob
            FOREIGN KEY (na_id) REFERENCES pages(na_id)
        );

        CREATE INDEX IF NOT EXISTS idx_pages_status ON pages(status);
        CREATE INDEX IF NOT EXISTS idx_pages_naid   ON pages(na_id);
        CREATE INDEX IF NOT EXISTS idx_cases_naid   ON cases(na_id);
    """)
    conn.commit()
