import sys, os
# add project root to path (only needed if not already in PYTHONPATH)
root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root not in sys.path:
    sys.path.insert(0, root)
import pandas as pd
from Utilities.Validation.retrieve_communes import retrieve_communes

url = 'https://www.archives-aube.fr/recherches/documents-numerises/genealogie/tout-letat-civil/etat-civil-de-la-ville-de-troyes-1535-1919?arko_default_62289d8b205f4--ficheFocus='
df = retrieve_communes(url, None)

print(df.head())
print("Total records:", df["record_count"].sum())