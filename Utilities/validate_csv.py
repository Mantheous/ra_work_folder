import pandas as pd

# This file is for ensuring that we have scraped quality data. Often these inconsistencies are
# due to bad data in the website. For example, in Dordogne I found some records that don't have
# a cote. These records were single pages that contained no useful information. Ideally these
# results should be edge cases that are impossible to find until we have processed the data
# that can be resolved manually with ease.

def verify_unique_by_index(file_path, col_index):
    try:
        # Load file with the pipe delimiter
        column_headers = ['Cote', 'Commune', 'Date_Range', 'Record_Type', 'Count', 'URL']
        df = pd.read_csv(file_path, header=None, delimiter='|', names=column_headers)
        
        # Validate index range
        if col_index >= len(df.columns):
            print(f"Error: Index {col_index} is out of bounds. File only has {len(df.columns)} columns.")
            return
        
        # Check for empty cells
        nan_rows = df[df.isna().any(axis=1)]
        
        if nan_rows.empty:
            print("✅ No missing values found.")
        else:
            print(f"❌ Found {len(nan_rows)} rows with missing values:\n")
            print(nan_rows)

        # Make sure the date is a date
        # I guess that "An X" is a valid date
        # regex_pattern = r'[^0-9\s-]' # This pattern matches any character that is not a digit, whitespace, or hyphen
        # invalid_date_rows = df[df['Date_Range'].str.contains(regex_pattern, na=False)]
        # if len(invalid_date_rows) == 0:
        #     print("✅ All date ranges are valid.")
        # else:
        #     print(f"❌ Found {len(invalid_date_rows)} rows with invalid date ranges:\n")
        #     print(invalid_date_rows)

        # Verify each link is unique
        target_col = df.iloc[:, col_index]
        
        if target_col.is_unique:
            print(f"✅ Success: Column at index {col_index} is unique.")
        else:
            # Get counts of all values
            counts = target_col.value_counts()
            
            # Filter to only include values that appear more than once
            duplicates = counts[counts > 1]
            
            print(f"❌ Failed: Found {len(duplicates)} distinct values with duplicates at index {col_index}.\n")
            print(f"{'Value':<30} | {'Occurrences':<10}")
            print("-" * 45)
            
            for value, count in duplicates.items():
                print(f"Link: {str(value):<30} | Count: {count:<10} | Commune: {df[df.iloc[:, col_index] == value].iloc[0, 1]}")

            print(f"❌ Failed: Found {len(duplicates)} distinct values with duplicates at index {col_index}.\n")
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    

if __name__ == "__main__":
    verify_unique_by_index('Utilities/validation_test.csv', 5)
