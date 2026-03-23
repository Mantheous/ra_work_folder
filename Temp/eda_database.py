import sqlite3
conn = sqlite3.connect('CCC_Records/pipeline/ccc_records.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [row[0] for row in cursor.fetchall()]
print(f'Tables found: {tables}')
for table in tables:
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f'{table}: {count} entries')
print()
print('Sample rows from extraction_chunks:')
for row in conn.execute("SELECT na_id, chunk_page, extracted_text FROM extraction_chunks LIMIT 25"):
    print(f"  {row['na_id']} | {row['chunk_page']} | {row['extracted_text']}")
conn.close()
