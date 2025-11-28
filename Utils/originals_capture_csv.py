import os
from datetime import datetime, timedelta

import pandas as pd

from Utils.pretty_print import step_header, sub

# Columns we want to keep in the Originals file
ORIGINALS_COLUMNS = [
    "DOC_ID",
    "INVOICE_TYPE",
    "ENTRY_DATE",
    "COMPANY_CODE",
    "DOC_DATE",
    "INVOICE_NUMBER",
    "AMOUNT",
    "VENDOR_NUM",
    "VENDOR_NAME_1",
    "VENDOR_NAME_2",
    "PO_NUM",
    "ABN",
    "DSS_DOWNLOAD_DATE",
    "STATUS_TEXT",
]


def normalize_doc_id(value) -> str:
    """Normalise DOC_ID for comparison."""
    if value is None:
        return ""
    return str(value).lstrip("0")


def load_csv(path: str) -> pd.DataFrame:
    """
    Load a CSV into a DataFrame. Returns an empty DataFrame if the file does not exist.
    """
    if not os.path.exists(path):
        sub(f"[load_csv] {path} not found. Returning empty DataFrame.")
        return pd.DataFrame()

    df = pd.read_csv(path, dtype=str, low_memory=False)
    sub(f"[load_csv] Loaded {len(df)} rows from {path}")
    return df


def to_originals_schema(source_df: pd.DataFrame) -> pd.DataFrame:
    """
    Trim Transaction Master to the Originals schema.
    """
    missing = [c for c in ORIGINALS_COLUMNS if c not in source_df.columns]
    if missing:
        raise ValueError(
            f"[to_originals_schema] Source is missing required columns for originals schema: {missing}"
        )

    trimmed = source_df[ORIGINALS_COLUMNS].copy()
    sub(
        f"[to_originals_schema] Trimmed to originals schema "
        f"({len(trimmed)} rows, {len(trimmed.columns)} columns)"
    )
    return trimmed


def filter_recent_by_entry_date(df: pd.DataFrame, days: int = 30) -> pd.DataFrame:
    """
    Filter to rows with ENTRY_DATE within the last `days` days.

    ENTRY_DATE is expected to be in a standard, pandas-parseable format
    (for example 'YYYY-MM-DD'). We rely on pandas' default parser and
    use a Timestamp cutoff, matching the debug behaviour.
    """
    if "ENTRY_DATE" not in df.columns:
        raise ValueError("[filter_recent_by_entry_date] ENTRY_DATE column is required.")

    if df.empty:
        sub("[filter_recent_by_entry_date] Input DataFrame is empty. Nothing to filter.")
        return df.copy()

    df = df.copy()

    # Parse dates without forcing dayfirst=True so that ISO formats are handled correctly
    parsed_dates = pd.to_datetime(
        df["ENTRY_DATE"],
        errors="coerce",
    )

    before = len(df)

    # Use a Timestamp cutoff (today at midnight minus `days`)
    cutoff_ts = pd.Timestamp.today().normalize() - pd.Timedelta(days=days)

    mask = parsed_dates >= cutoff_ts
    recent = df[mask].copy()

    after = len(recent)
    nat_count = parsed_dates.isna().sum()

    sub(
        f"[filter_recent_by_entry_date] Kept {after} of {before} rows "
        f"(cutoff = {cutoff_ts.date()}, NaT dates = {nat_count})"
    )

    return recent


def get_existing_ids(orig_df: pd.DataFrame) -> set:
    """
    Return a set of normalised DOC_ID values already present in Originals.
    """
    if orig_df.empty:
        sub("[get_existing_ids] Originals DataFrame is empty. No existing DOC_IDs.")
        return set()

    if "DOC_ID" not in orig_df.columns:
        sub("[get_existing_ids] DOC_ID column not found. Returning empty set.")
        return set()

    norm_ids = {normalize_doc_id(v) for v in orig_df["DOC_ID"]}
    sub(f"[get_existing_ids] Found {len(norm_ids)} existing DOC_IDs.")
    return norm_ids


def find_new_rows(source_df: pd.DataFrame, existing_ids: set) -> pd.DataFrame:
    """
    From the date-filtered Transaction Master slice, return only rows whose DOC_ID is new.
    """
    if source_df.empty:
        sub("[find_new_rows] Source DataFrame is empty. No new rows.")
        return source_df.copy()

    if "DOC_ID" not in source_df.columns:
        raise ValueError("[find_new_rows] DOC_ID column not found in source DataFrame.")

    df = source_df.copy()
    df["_DOC_KEY"] = df["DOC_ID"].apply(normalize_doc_id)

    before = len(df)
    df = df.drop_duplicates(subset=["_DOC_KEY"], keep="first")
    after = len(df)

    if after != before:
        sub(
            f"[find_new_rows] Dropped {before - after} duplicate DOC_IDs "
            "by normalised key."
        )

    mask_new = ~df["_DOC_KEY"].isin(existing_ids)
    new_rows = df[mask_new].drop(columns=["_DOC_KEY"]).copy()

    sub(
        f"[find_new_rows] Found {len(new_rows)} new rows "
        f"from {after} unique DOC_IDs."
    )
    return new_rows


def append_new_rows(originals_path: str, new_rows_df: pd.DataFrame) -> int:
    """
    Append new rows to the originals CSV.
    """
    if new_rows_df.empty:
        sub("[append_new_rows] No new rows to append.")
        return 0

    # Ensure directory exists
    os.makedirs(os.path.dirname(originals_path), exist_ok=True)
    file_exists = os.path.exists(originals_path)

    new_rows_df.to_csv(
        originals_path,
        mode="a",
        header=not file_exists,
        index=False,
    )

    sub(f"[append_new_rows] Wrote {len(new_rows_df)} rows to {originals_path}.")
    return len(new_rows_df)


def run_originals_capture(
    transaction_master_csv: str,
    originals_csv: str,
    days_back: int = 30,
) -> int:
    """
    Orchestrate the Originals capture for PIOR.

    Business semantics:
    - Invoice identity is determined by normalised DOC_ID (DOC_KEY).
    - We only consider Transaction Master rows whose ENTRY_DATE is within
      the last `days_back` days.
    - Within that window, any DOC_KEY that does not exist in Originals
      is treated as a new invoice and appended.
    """
    step_header("STEP: Originals Capture")

    # Step 1: Load Transaction Master
    src_full = load_csv(transaction_master_csv)
    if src_full.empty:
        sub("[run_originals_capture] Source CSV empty or missing. Nothing to do.")
        print("=" * 55)
        return 0

    # Step 2: Trim to Originals schema
    src_view = to_originals_schema(src_full)

    # Step 3: Filter by ENTRY_DATE window
    src_recent = filter_recent_by_entry_date(src_view, days=days_back)
    if src_recent.empty:
        sub("[run_originals_capture] No recent rows. Nothing to do.")
        print("=" * 55)
        return 0

    # Ensure ENTRY_DATE is datetime for potential future debugging
    src_recent = src_recent.copy()
    src_recent["ENTRY_DATE"] = pd.to_datetime(
        src_recent["ENTRY_DATE"],
        errors="coerce",
        dayfirst=True,
    )

    sub(
        f"[run_originals_capture] Recent slice has {len(src_recent):,} rows "
        f"for days_back={days_back}"
    )

    # Step 4: Load Originals
    orig_df = load_csv(originals_csv)

    # If Originals is empty, everything in src_recent is new
    if orig_df.empty or "DOC_ID" not in orig_df.columns:
        sub(
            "[run_originals_capture] Originals empty or missing DOC_ID. "
            "Treating all recent rows as new."
        )

        written = append_new_rows(originals_csv, src_recent)
        sub(f"[run_originals_capture] Capture complete. Rows written: {written}")
        print("=" * 55)
        return written

    # Step 5: Build normalised DOC_KEY for both
    orig_df = orig_df.copy()
    orig_df["DOC_KEY"] = orig_df["DOC_ID"].apply(normalize_doc_id)
    src_recent["DOC_KEY"] = src_recent["DOC_ID"].apply(normalize_doc_id)

    orig_keys = set(orig_df["DOC_KEY"])

    sub(f"[run_originals_capture] Unique DOC_KEYs in Originals: {len(orig_keys):,}")

    # Step 6: Compute new rows using DOC_KEY set difference
    before = len(src_recent)
    src_recent = src_recent.drop_duplicates(subset=["DOC_KEY"], keep="first")
    after = len(src_recent)

    if after != before:
        sub(
            f"[run_originals_capture] Dropped {before - after} duplicate rows "
            "in recent slice by DOC_KEY."
        )

    mask_new = ~src_recent["DOC_KEY"].isin(orig_keys)
    new_rows = src_recent[mask_new].drop(columns=["DOC_KEY"]).copy()

    total_new = len(new_rows)
    sub(f"[run_originals_capture] New DOC_KEYs to append: {total_new:,}")

    if total_new > 0:
        sample = new_rows["DOC_ID"].head(10)
        sub("[run_originals_capture] Sample of first 10 DOC_IDs:")
        for v in sample.tolist():
            sub(f"   {v}")

    # Step 7: Append
    written = append_new_rows(originals_csv, new_rows)
    sub(f"[run_originals_capture] Capture complete. Rows written: {written}")
    print("=" * 55)

    return written
