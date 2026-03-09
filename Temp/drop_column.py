import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from Utilities.paths import PROJECT_ROOT

csv_path = str(PROJECT_ROOT / "Civil_Status" / "Aube2" / "Aube2_cleaned_missed.csv")
column_headers = ['Department','Page','Index','Cote', 'Commune', 'Date_Range', 'Record_Type', 'Count', 'Page_Number', 'URL']
df = pd.read_csv(csv_path, header=None, delimiter='|', names=column_headers)
df = df.drop(columns=['Count'])
df.to_csv(csv_path, index=False, header=False, sep='|')