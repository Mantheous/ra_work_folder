import pandas as pd

# This file is for ensuring that we have scraped quality data. Often these inconsistencies are
# due to bad data in the website. For example, in Dordogne I found some records that don't have
# a cote. These records were single pages that contained no useful information. Ideally these
# results should be edge cases that are impossible to find until we have processed the data
# that can be resolved manually with ease.

# High quality record:
# - Has no missing values
# - Has a unique URL (we don't want duplicates)
# - Can be traced back to the source

def validate(file_path, number_of_records=None):
    try:
        # Load file with the pipe delimiter
        column_headers = ['Department','Page','Index','Cote', 'Commune', 'Date_Range', 'Record_Type', 'Count', 'URL']
        df = pd.read_csv(file_path, header=None, delimiter='|', names=column_headers)
        
        
        # Check for empty cells
        nan_rows = df[df.isna().any(axis=1)]
        
        if nan_rows.empty:
            print("✅ No missing values found.")
        else:
            print(f"❌ Found {len(nan_rows)} rows with missing values:\n")
            print(nan_rows)

        # Verify each link is unique
        if df['URL'].is_unique:
            print(f"✅ Success: URL column is unique.")
        else:
            # Get counts of all values
            counts = df['URL'].value_counts()
            
            # Filter to only include values that appear more than once
            duplicates = counts[counts > 1]
            
            print(f"❌ Failed: Found {len(duplicates)} distinct URLs with duplicates.\n")
            print(f"{'Value':<30} | {'Occurrences':<10}")
            print("-" * 45)
            
            for value, count in duplicates.items():
                print(f"Link: {str(value):<30} | Count: {count:<10} | Commune: {df[df['URL'] == value].iloc[0, 1]}")

            print(f"❌ Failed: Found {len(duplicates)} distinct URLs with duplicates.\n")
        
        # Check that we have the expected number of records
        if number_of_records is not None:
            actual_count = len(df)
            if actual_count == number_of_records:
                print(f"✅ Success: Found expected number of records ({number_of_records}).")
            else:
                print(f"❌ Failed: Expected {number_of_records} records, but found {actual_count}.")

        # TODO - Trace back first record, second on the second page, and last record
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    

if __name__ == "__main__":
    validate('ra_work_folder/Utilities/validation_test.csv', 12428)
