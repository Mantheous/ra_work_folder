import pandas as pd

csv_path = "ra_work_folder\Civil_Status\Aube2\Aube2_cleaned.csv"
column_headers = ['Department','Page','Index','Cote', 'Commune', 'Date_Range', 'Record_Type', 'Count', 'URL']
df = pd.read_csv(csv_path, header=None, delimiter='|', names=column_headers)
print(df['Count'].sum())