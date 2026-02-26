# This is a generic downloader. It reads a CSV file with lines in the format:
# filename|link

import os
in_csv = "Aube_test_pairs.csv"
folder = "Aube_test_downloads\\"

for line in open(in_csv, "r", encoding="utf-8"):
    filename, link = line.strip().split("|")
    os.system(f"curl -o {folder}{filename} {link}") # windows

    # I need to use piefix, for copyright information

    # also need to match the folder structure of the census data