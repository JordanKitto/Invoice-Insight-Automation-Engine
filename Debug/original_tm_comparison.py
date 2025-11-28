import pandas as pd
import os
import sys 

# Ensure project root is on PYTHONPATH
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
print(f"[debug] Project root on sys.path: {ROOT}")

tm_path = r"Output_Files\transaction_master.csv"
orig_path = r"Output_Files\Original_Invoice_Data_CSV.csv"
change_path = r"Output_Files\Change_Invoice_Data_CSV.csv"


df_original = pd.read_csv(orig_path, dtype=str, low_memory=False)
# df_tm = pd.read_csv(tm_path, dtype=str, low_memory=False)
# df_change = pd.read_csv(change_path, dtype=str, low_memory=False)

# print(df_change.columns)


print(df_original.iloc[1534279])