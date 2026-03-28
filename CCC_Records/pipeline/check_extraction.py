import sqlite3
from pathlib import Path

DB_PATH = Path(r"w:\RA_work_folders\Ashton_Reed\ra_work_folder\CCC_Records\pipeline\ccc_records.db")

conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row

# --- Extraction stats ---
stats = conn.execute("""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN status = 'extracted' THEN 1 ELSE 0 END) as extracted,
        SUM(CASE WHEN status = 'pending'   THEN 1 ELSE 0 END) as pending
    FROM hits
""").fetchone()

print("=" * 60)
print("EXTRACTION STATS")
print("=" * 60)
print(f"  Total hits:        {stats['total']}")
print(f"  Extracted (done):  {stats['extracted']}")
print(f"  Pending:           {stats['pending']}")

# --- Chunk stats ---
chunk_stats = conn.execute("""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN status = 'done'    THEN 1 ELSE 0 END) as done,
        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
        SUM(CASE WHEN status = 'failed'  THEN 1 ELSE 0 END) as failed
    FROM extraction_chunks
""").fetchone()

print()
print("CHUNK STATS")
print("=" * 60)
print(f"  Total chunks:   {chunk_stats['total']}")
print(f"  Done:           {chunk_stats['done']}")
print(f"  Pending:        {chunk_stats['pending']}")
print(f"  Failed:         {chunk_stats['failed']}")

# --- Cases stats ---
case_stats = conn.execute("""
    SELECT COUNT(*) as total
    FROM cases
""").fetchone()

print()
print("CASE STATS")
print("=" * 60)
print(f"  Total cases:    {case_stats['total']}")

# --- Sample extraction ---
sample = conn.execute("""
    SELECT na_id, chunk_page, length(extracted_text) as text_len, extracted_text
    FROM extraction_chunks
    WHERE status = 'done'
    LIMIT 1
""").fetchone()

print()
print("=" * 60)
print("SAMPLE EXTRACTION")
print("=" * 60)
if sample:
    print(f"  na_id:        {sample['na_id']}")
    print(f"  chunk_page:   {sample['chunk_page']}")
    print(f"  raw_text len: {sample['text_len']} chars")
    print()
    print("  --- First 100 chars of extracted_text ---")
    print(sample['extracted_text'][:100])
    print()
    print(f"  Catalog URL:  https://catalog.archives.gov/id/{sample['na_id']}")
else:
    print("  No extracted hits with text found yet.")

# --- Sample case ---
sample_case = conn.execute("""
    SELECT na_id, case_start_id, case_end_id, length(extracted_text) as text_len, extracted_text
    FROM cases
    LIMIT 1
""").fetchone()

print()
print("=" * 60)
print("SAMPLE CASE")
print("=" * 60)
if sample_case:
    print(f"  na_id:         {sample_case['na_id']}")
    print(f"  case_start_id: {sample_case['case_start_id']}")
    print(f"  case_end_id:   {sample_case['case_end_id']}")
    print(f"  raw_text len:  {sample_case['text_len']} chars")
    print()
    print("  --- First 1000 chars of extracted_text ---")
    if sample_case['extracted_text']:
        print(sample_case['extracted_text'][:1000])
    else:
        print("None")
    print()
else:
    print("  No cases with text found yet.")

conn.close()
