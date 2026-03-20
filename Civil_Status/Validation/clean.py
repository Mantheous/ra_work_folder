import pandas as pd

input_csv = "Validation/validation_test.csv"
output_csv = "Validation/validation_test_cleaned.csv"

def clean():
    """
    Clean the CSV:
    - Remove all entries where URL is not unique (keeps no copy, not even the first)
    - Drop entries where both image Count and URL are NaN
    - Replace NaN Record_Type with 'Record-type-unindexed'
    """
    column_headers = ['Department', 'Page', 'Index', 'Cote', 'Commune', 'Date_Range', 'Record_Type', 'Count', 'URL']
    df = pd.read_csv(input_csv, header=None, delimiter='|', names=column_headers)

    # Drop rows where Count AND URL are both NaN
    df = df.dropna(subset=['Count', 'URL'], how='all')

    # Remove all rows with a non-unique URL (keep=False drops every duplicate, including the first)
    df = df[~df['URL'].duplicated(keep=False)]

    # Fill remaining NaN Record_Type values
    df['Record_Type'] = df['Record_Type'].fillna('Record-type-unindexed')

    # Cast Count to int (safe now that NaN rows have been dropped)
    df['Count'] = df['Count'].astype(int)

    df.to_csv(output_csv, index=False, header=False, sep='|')

if __name__ == '__main__':
    clean()
