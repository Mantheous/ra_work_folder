import sqlite3
conn = sqlite3.connect('CCC_Records/pipeline/ccc_records.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

for row in conn.execute("""
    SELECT extracted_text
    FROM cases
    WHERE na_id = '489808685'
    LIMIT 1
"""):
    print(f"  {row['extracted_text']}")

# delete cases
# conn.execute("DELETE FROM cases")
# conn.commit()
conn.close()
