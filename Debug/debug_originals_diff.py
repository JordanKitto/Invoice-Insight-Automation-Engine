import pandas as pd
import os
import sys 

# Ensure project root is on PYTHONPATH
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
print(f"[debug] Project root on sys.path: {ROOT}")

from Utils.originals_capture_csv import normalize_doc_id

tm_path = r"Output_Files\transaction_master.csv"
orig_path = r"Output_Files\Original_Invoice_Data_CSV.csv"

print("Loading CSVs...")
tm = pd.read_csv(tm_path, dtype=str, low_memory=False)
orig = pd.read_csv(orig_path, dtype=str, low_memory=False)

tm["ENTRY_DATE"] = pd.to_datetime(tm["ENTRY_DATE"], errors="coerce")
orig["ENTRY_DATE"] = pd.to_datetime(orig["ENTRY_DATE"], errors="coerce")

# Build normalised DOC_KEY for both, using the same logic as the job
tm["DOC_KEY"] = tm["DOC_ID"].apply(normalize_doc_id)
orig["DOC_KEY"] = orig["DOC_ID"].apply(normalize_doc_id)

orig_keys = set(orig["DOC_KEY"])

# 1. Full TM vs Originals, on normalised DOC_KEY
new_all_norm = tm[~tm["DOC_KEY"].isin(orig_keys)].copy()
print(f"New DOC_KEYs across FULL transaction_master (normalised): {len(new_all_norm):,}")

# 2. Recent window, using the same cutoff as before
cutoff = pd.Timestamp("2025-10-22")
recent = tm[tm["ENTRY_DATE"] >= cutoff].copy()
recent["DOC_KEY"] = recent["DOC_ID"].apply(normalize_doc_id)

new_recent_norm = recent[~recent["DOC_KEY"].isin(orig_keys)].copy()
print(f"Rows in recent window (ENTRY_DATE >= {cutoff.date()}): {len(recent):,}")
print(f"New DOC_KEYs within recent window (normalised): {len(new_recent_norm):,}")

# Optional: inspect a few of the "new_recent" by raw DOC_ID vs Originals
print("\nSample of 10 normalised 'new' DOC_IDs in recent window (if any):")
print(
    new_recent_norm[["DOC_ID", "DOC_KEY", "ENTRY_DATE"]]
    .head(10)
    .to_string(index=False)
)
