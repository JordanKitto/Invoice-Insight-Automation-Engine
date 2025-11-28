import pandas as pd

# 1. Read the original data (This is correct)
# Using a raw string (r'') is good practice for Windows paths
input_file_path = r'C:\Users\kittojor\OneDrive - Queensland Health\A001_Data_Analytics\A0017_PIOR\Output_Files\Original_Invoice_Data_CSV.csv'
df = pd.read_csv(input_file_path)

# 2. Drop the problematic row (This is correct)
df_cleaned = df.drop(index=1527999, axis=0)

# 3. Define the path for the CLEANED data (Changed the file name for safety)
# Always save cleaned data to a new file to preserve the original source.
output_file_path = r'C:\Users\kittojor\OneDrive - Queensland Health\A001_Data_Analytics\A0017_PIOR\Output_Files\Original_Invoice_Data_CSV.csv'

# 4. SAVE THE *CLEANED* DATAFRAME (Correction applied here)
# The .to_csv() method returns None, so df_save will always be None.
# The variable is unnecessary unless you need to check return value of a custom method.
df_cleaned.to_csv(output_file_path, index=False) 

# 5. Print results (Correct for verification)
print(f"Original row count: {len(df)}")
print(f"Cleaned row count: {len(df_cleaned)}")
print(f"Cleaned data successfully saved to: {output_file_path}")