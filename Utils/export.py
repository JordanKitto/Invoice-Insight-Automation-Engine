import os
from datetime import datetime
import pandas as pd

# This module handles CSV export functionality for data processing tasks.
# Key features:
# - Automatically creates an output directory if it does not exist
# - Supports exporting data with custom column headers
# - Allows optional filename specification or automatic timestamp-based naming
# - Supports appending to existing CSV files
# - Can optionally print export progress

EXPORT_DIR = "Output_Files"  # Default folder to store exported CSV files

# Ensure the export directory exists; create it if missing
def ensure_export_dir():
    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)

# Export data to a CSV file using column headers and row data
def export_to_csv(columns, rows, filename=None, filename_prefix="export", append=False, log_progress=False):

    ensure_export_dir()  # Make sure output directory is ready

    df = pd.DataFrame(rows, columns=columns)  # Create DataFrame from input data

    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.csv"  # Generate a unique filename with timestamp

    filepath = os.path.join(EXPORT_DIR, filename)

    if append and os.path.exists(filepath):
        df.to_csv(filepath, mode="a", header=False, index=False)  # Append without header
    else:
        df.to_csv(filepath, index=False)  # Create new file or overwrite

    if log_progress:
        print(f"📦 Exported CSV to: {filepath}")  # Optional console output

    return filepath  # Return path for reference or logging
