import pandas as pd

input_csv = "Utilities/validation_test.csv"
output_csv = "Utilities/validation_test_deduped.csv"

def main():
    column_headers = ['Cote', 'Commune', 'Date_Range', 'Record_Type', 'Count', 'URL']
    df = pd.read_csv(input_csv, header=None, delimiter='|', names=column_headers)
    df = df.drop_duplicates()
    df.to_csv(output_csv, index=False, header=False, sep='|')

if __name__ == '__main__':
    main()
