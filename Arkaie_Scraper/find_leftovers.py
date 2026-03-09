# this file finds the files that were missed by the downloader.
# usually because of connection errors

import pandas as pd

FRI_FOLDER_PATH = "W:\\RA_work_folders\\Ashton_Reed\\ra_work_folder\\Civil_Status\\Results\\"

def find_leftovers(mdf, missing_csv):
    for row in mdf.itertuples():
        #construct path
        folder_path = os.path.join(FRI_FOLDER_PATH, dept, r_type, commune, period)



if __name__ == "__main__":
    column_headers = ['department','page','index','cote', 'commune', 'period', 'record_type', 'img_count', 'url']
    df = pd.read_csv(csv_file, header=None, delimiter='|', names=column_headers)
    df = df.map(lambda x: str(x).replace(" ", "_"))
    find_leftovers(df, "missing_csv.csv")