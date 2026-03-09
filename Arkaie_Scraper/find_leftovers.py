# this file finds the files that were missed by the downloader.
# usually because of connection errors

import pandas as pd
import os
from pathlib import Path
_THIS_DIR = Path(__file__).resolve().parent.parent
from Utilities.paths import PROJECT_ROOT

FRI_FOLDER_PATH = str(PROJECT_ROOT / "Civil_Status" / "Results")

def find_leftovers(mdf, missing_csv):
    for row in mdf.itertuples():
        #construct path
        folder_path = os.path.join(FRI_FOLDER_PATH, dept, r_type, commune, period)



if __name__ == "__main__":
    column_headers = ['department','page','index','cote', 'commune', 'period', 'record_type', 'img_count', 'url']
    df = pd.read_csv(csv_file, header=None, delimiter='|', names=column_headers)
    df = df.map(lambda x: str(x).replace(" ", "_"))
    find_leftovers(df, "missing_csv.csv")