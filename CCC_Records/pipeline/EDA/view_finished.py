### View data

import os
import sqlite3
import json
import csv
import random

conn = sqlite3.connect('CCC_Records/pipeline/ccc_records.db')
conn.row_factory = sqlite3.Row

def construct_link(na_id, case_id):
    return f"https://catalog.archives.gov/id/{na_id}?objectPage={int(case_id)-int(na_id)}"

all_data = []

# Fetch all rowids to safely handle random sampling without full table sorting
print("Fetching case IDs...")
cursor = conn.cursor()
cursor.execute("SELECT rowid FROM cases")
all_rowids = [row[0] for row in cursor.fetchall()]

sample_size = min(1000, len(all_rowids))
print(f"Sampling {sample_size} random cases from {len(all_rowids)} total cases...")
sampled_rowids = random.sample(all_rowids, sample_size)

# Fetch only the sampled rows
placeholders = ','.join('?' * len(sampled_rowids))
query = f"""
    SELECT case_start_id, na_id, json
    FROM cases
    WHERE rowid IN ({placeholders})
"""

for row in conn.execute(query, sampled_rowids):
    try:
        json_data = json.loads(row['json'])
    except (json.JSONDecodeError, TypeError):
        json_data = {}

    gen = json_data.get('general_info') or {}
    
    def get_val(obj, key):
        """Helper to get value or return 'null' string."""
        val = obj.get(key)
        return str(val) if val is not None else "null"

    # Handle Name (can be dict or string)
    name_data = gen.get('name')
    if isinstance(name_data, dict):
        name = f"{name_data.get('first', '')} {name_data.get('last', '')}".strip() or "null"
    else:
        name = str(name_data) if name_data is not None else "null"

    date_of_birth = get_val(gen, 'dob')
    address = get_val(gen, 'address')
    
    # Handle Nearest Relative (can be dict or string)
    nr_data = gen.get('nearest_relative')
    if isinstance(nr_data, dict):
        nearest_relative = nr_data.get('name', "null")
    else:
        nearest_relative = str(nr_data) if nr_data is not None else "null"

    company = "Not Implemented"
    link = construct_link(row['na_id'], row['case_start_id'])
    all_data.append([name, date_of_birth, address, nearest_relative, company, link])

# Write results to CSV
with open('view_results.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["Name", "Date of Birth", "Address", "Nearest Relative", "Company", "Link"])
    writer.writerows(all_data)

conn.close()
print(f"Extraction complete. {len(all_data)} records written to view_results.csv")