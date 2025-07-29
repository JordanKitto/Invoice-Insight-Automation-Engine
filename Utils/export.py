import os
from datetime import datetime
import pandas as pd

EXPORT_DIR = "Output_Files"

def ensure_export_dir():
    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)

def export_to_csv(columns, rows, filename=None, filename_prefix="export", append=False, log_progress=False):
    """
    Export a list of rows with column headers to a CSV file.

    Args:
        columns (list[str]): Column headers.
        rows (list[tuple]): Data rows.
        filename (str, optional): Exact filename to use. If not provided, uses filename_prefix + timestamp.
        filename_prefix (str): Prefix for auto-generated filename.
        append (bool): Whether to append to existing file.
    """
    ensure_export_dir()

    df = pd.DataFrame(rows, columns=columns)

    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.csv"

    filepath = os.path.join(EXPORT_DIR, filename)

    if append and os.path.exists(filepath):
        df.to_csv(filepath, mode="a", header=False, index=False)
    else:
        df.to_csv(filepath, index=False)

    if log_progress:
        print(f"📦 Exported CSV to: {filepath}")
        
    return filepath
