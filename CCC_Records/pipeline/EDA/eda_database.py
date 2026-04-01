import sqlite3
import json
conn = sqlite3.connect('CCC_Records/pipeline/ccc_records (1).db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

all_data = []
for row in conn.execute("""
    SELECT extracted_text, json
    FROM cases
    WHERE na_id = '546201098'
    LIMIT 5
"""):
    # print(f"  {row['extracted_text'][:1000]}")
    print("=" * 60)
    json_val = row['json'] or "NULL"
    print(f"  {json_val[:1000]}")
    # all_data.append(json.loads(row['extracted_text']))

# download json
# with open('page_json_529913494.json', 'w', encoding='utf-8') as f:
#     json.dump(all_data, f, indent=4)




# delete cases
# conn.execute("DELETE FROM cases")
# conn.commit()
conn.close()
