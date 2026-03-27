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
print("=" * 60)
print('Sample rows from hits:')
print("=" * 60)
for row in conn.execute("""
    SELECT na_id, status, object_id_start, object_id_end, raw_text 
    FROM hits 
    WHERE na_id = '489808685'
"""):
    print(f"  {row['na_id']} | {row['status']} | {row['object_id_start']} | {row['object_id_end']} | {row['raw_text'][:100]}")
conn.close()
