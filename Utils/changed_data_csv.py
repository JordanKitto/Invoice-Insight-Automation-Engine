import os
from typing import List

import numpy as np
import pandas as pd

from Utils.pretty_print import step_header, sub

ALLOWED_TERMINAL_STATUSES = {
    "POSTED",
    "OBSOLETE",
    "CANCELLED",
    "DELETED",
    "CONFIRMED DUPLICATE",
}


def load_originals_dataframe(originals_csv: str) -> pd.DataFrame:
    """
    Load the Originals CSV into a DataFrame.

    If the file does not exist, return an empty DataFrame with zero rows.
    This keeps the Changed Data job safe to run even on the first day
    before any originals have been captured.
    """
    if not os.path.exists(originals_csv):
        sub(
            f"[ChangedData] Originals CSV not found at '{originals_csv}'. "
            "Returning empty DataFrame."
        )
        return pd.DataFrame()

    df = pd.read_csv(originals_csv, dtype=str, low_memory=False)
    sub(
        f"[ChangedData] Loaded Originals CSV from '{originals_csv}' "
        f"with {len(df):,} rows."
    )
    return df


def load_transaction_master_dataframe(tm_csv: str) -> pd.DataFrame:
    """
    Load the Transaction Master CSV into a DataFrame.

    This function is stricter than the Originals loader:
    - The file must exist.
    - The DOC_ID column must be present.
    - DOC_ID must be unique (acts as a primary key).
    """
    if not os.path.exists(tm_csv):
        raise FileNotFoundError(
            f"[ChangedData] Transaction Master CSV not found at '{tm_csv}'. "
            "Cannot compute Changed Data without it."
        )

    df = pd.read_csv(tm_csv, dtype=str, low_memory=False)
    sub(
        f"[ChangedData] Loaded Transaction Master CSV from '{tm_csv}' "
        f"with {len(df):,} rows."
    )

    if "DOC_ID" not in df.columns:
        raise ValueError(
            "[ChangedData] Transaction Master CSV is missing required "
            "column 'DOC_ID'."
        )

    if df["DOC_ID"].duplicated().any():
        raise ValueError(
            "[ChangedData] Transaction Master CSV has duplicate DOC_ID "
            "values. DOC_ID must be unique for Changed Data logic."
        )

    return df


def detect_changed_rows(
    originals_df: pd.DataFrame,
    tm_df: pd.DataFrame,
    compare_columns: List[str],
) -> pd.DataFrame:
    """
    Find DOC_IDs where the current Transaction Master values differ
    from their original snapshot for any of the given columns.
    """
    if originals_df.empty:
        sub("[ChangedData] Originals DataFrame is empty. No changes to detect.")
        return pd.DataFrame()

    if tm_df.empty:
        sub("[ChangedData] Transaction Master DataFrame is empty. No changes to detect.")
        return pd.DataFrame()

    required_cols = {"DOC_ID", *compare_columns}
    missing_in_orig = required_cols.difference(originals_df.columns)
    missing_in_tm = required_cols.difference(tm_df.columns)

    if missing_in_orig:
        raise ValueError(
            f"[ChangedData] Originals DataFrame is missing columns: {sorted(missing_in_orig)}"
        )
    if missing_in_tm:
        raise ValueError(
            f"[ChangedData] Transaction Master DataFrame is missing columns: {sorted(missing_in_tm)}"
        )

    orig_subset = originals_df[["DOC_ID", *compare_columns]].copy()
    tm_subset = tm_df[["DOC_ID", *compare_columns]].copy()

    merged = orig_subset.merge(
        tm_subset,
        on="DOC_ID",
        how="inner",
        suffixes=("_ORIG", "_CURR"),
    )

    if merged.empty:
        sub("[ChangedData] No overlapping DOC_IDs between Originals and TM.")
        return merged

    diff_masks = []
    sentinel = "__NULL__"

    for col in compare_columns:
        col_orig = f"{col}_ORIG"
        col_curr = f"{col}_CURR"

        left = merged[col_orig].fillna(sentinel)
        right = merged[col_curr].fillna(sentinel)

        mask = left != right
        diff_masks.append(mask)

    if not diff_masks:
        return merged.iloc[0:0].copy()

    any_diff = np.logical_or.reduce(diff_masks)
    changed = merged[any_diff].copy()

    sub(
        f"[ChangedData] Detected {changed['DOC_ID'].nunique():,} DOC_IDs "
        f"with changes across {len(compare_columns)} column(s)."
    )

    return changed


def load_existing_changed_data(changed_csv: str) -> pd.DataFrame:
    """
    Load the existing Changed Data CSV from previous runs.

    If the file does not exist, return an empty DataFrame.
    """
    if not os.path.exists(changed_csv):
        sub(
            f"[ChangedData] Changed Data CSV not found at '{changed_csv}'. "
            "Starting with empty history."
        )
        return pd.DataFrame()

    df = pd.read_csv(changed_csv, dtype=str, low_memory=False)
    sub(
        f"[ChangedData] Loaded existing Changed Data CSV from '{changed_csv}' "
        f"with {len(df):,} rows."
    )
    return df


def filter_new_changes(
    changed_today_df: pd.DataFrame,
    existing_changed_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Given the set of DOC_IDs that changed today, return only those
    that have not already been appended to the Changed Data CSV.
    """
    if changed_today_df.empty:
        sub("[ChangedData] No changes detected today.")
        return changed_today_df.copy()

    if existing_changed_df.empty:
        sub("[ChangedData] No existing Changed Data file. All changes are new.")
        return changed_today_df.copy()

    if "DOC_ID" not in changed_today_df.columns:
        raise ValueError("[ChangedData] changed_today_df missing DOC_ID column.")

    if "DOCID" not in existing_changed_df.columns:
        raise ValueError("[ChangedData] existing_changed_df missing DOCID column.")

    existing_docids = set(existing_changed_df["DOCID"].astype(str))

    mask_new = ~changed_today_df["DOC_ID"].astype(str).isin(existing_docids)
    new_rows = changed_today_df[mask_new].copy()

    sub(
        f"[ChangedData] {new_rows['DOC_ID'].nunique():,} new changed DOC_IDs "
        "not previously recorded."
    )

    return new_rows


def filter_posted_changes(
    changed_df: pd.DataFrame,
    status_column_base: str = "STATUS_TEXT",
    posted_value: str = "Posted",
) -> pd.DataFrame:
    """
    Filter changed DOC_IDs to only those whose current status is in the set
    of terminal statuses (Posted, Obsolete, Cancelled, Deleted, Confirmed duplicate).

    Note: parameter `posted_value` is retained for backward compatibility
    but the actual filter uses ALLOWED_TERMINAL_STATUSES.
    """
    if changed_df.empty:
        sub("[ChangedData] No changed rows to filter by terminal status.")
        return changed_df.copy()

    current_status_col = f"{status_column_base}_CURR"

    if current_status_col not in changed_df.columns:
        raise ValueError(
            f"[ChangedData] Expected column '{current_status_col}' not found "
            "in changed_df when filtering for terminal status."
        )

    status_series = changed_df[current_status_col].astype(str).str.upper()
    mask_terminal = status_series.isin(ALLOWED_TERMINAL_STATUSES)

    filtered = changed_df[mask_terminal].copy()

    sub(
        "[ChangedData] Filtered to "
        f"{filtered['DOC_ID'].nunique():,} DOC_ID(s) with "
        f"{current_status_col} in {sorted(ALLOWED_TERMINAL_STATUSES)}."
    )

    return filtered


def build_changed_output_rows(
    new_changes_df: pd.DataFrame,
    tm_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build the final Changed Data output rows in the legacy SAP-style schema
    from the set of new changes and the current Transaction Master snapshot.

    Behaviour:
    - One row per DOC_ID.
    - Classic IOR columns (LAYOUT_ID, DOCID, INDEX_DATE, BUDAT, O_*, etc.).
    - High-level issue flags per DOC_ID:
        ISSUE_OBJECTTEXT
        ISSUE_COMPANY_CODE
        ISSUE_DATE_RANGE
        ISSUE_SUPPLIER
        ISSUE_INVOICE_NUMBER
        ISSUE_INVOICE_DATE
        ISSUE_ABN
        ISSUE_AMOUNT
      plus ISSUE_COUNT = sum of these flags.
    """
    if new_changes_df.empty:
        sub("[ChangedData] No new changed DOC_IDs to build output rows for.")
        return new_changes_df.iloc[0:0].copy()

    required_new_change_cols = [
        "DOC_ID",
        "DOC_DATE_ORIG",
        "DOC_DATE_CURR",
        "INVOICE_TYPE_ORIG",
        "INVOICE_TYPE_CURR",
        "COMPANY_CODE_ORIG",
        "COMPANY_CODE_CURR",
        "VENDOR_NUM_ORIG",
        "VENDOR_NUM_CURR",
        "VENDOR_NAME_1_ORIG",
        "VENDOR_NAME_1_CURR",
        "VENDOR_NAME_2_ORIG",
        "VENDOR_NAME_2_CURR",
        "ABN_ORIG",
        "ABN_CURR",
        "PO_NUM_ORIG",
        "PO_NUM_CURR",
        "INVOICE_NUMBER_ORIG",
        "INVOICE_NUMBER_CURR",
        "AMOUNT_ORIG",
        "AMOUNT_CURR",
        "STATUS_TEXT_ORIG",
        "STATUS_TEXT_CURR",
    ]

    missing_new_cols = [c for c in required_new_change_cols if c not in new_changes_df.columns]
    if missing_new_cols:
        raise ValueError(
            f"[ChangedData] new_changes_df is missing required columns: {missing_new_cols}"
        )

    if "DOC_ID" not in tm_df.columns:
        raise ValueError(
            "[ChangedData] Transaction Master DataFrame is missing 'DOC_ID' column."
        )

    tm_required_cols = [
        "DOC_ID",
        "LAYOUT_ID",
        "ENTRY_DATE",
        "POSTING_DATE",
        "PO_LAST_UPDATED",
        "ENTRY_DATE_AND_TIME",
    ]
    missing_tm_cols = [c for c in tm_required_cols if c not in tm_df.columns]
    if missing_tm_cols:
        raise ValueError(
            f"[ChangedData] Transaction Master DataFrame is missing required "
            f"columns: {missing_tm_cols}"
        )

    tm_subset = tm_df[tm_required_cols].copy()

    merged = new_changes_df.merge(
        tm_subset,
        on="DOC_ID",
        how="left",
        validate="one_to_one",
    )

    # Gracefully handle missing LAYOUT_ID
    missing_mask = merged["LAYOUT_ID"].isna()
    if missing_mask.any():
        missing_docids = merged.loc[missing_mask, "DOC_ID"].unique()
        sub(
            f"[ChangedData] Warning: LAYOUT_ID missing for "
            f"{len(missing_docids):,} DOC_ID(s): {missing_docids[:10]}. "
            "Proceeding with empty LAYOUT_ID for these rows."
        )
        merged.loc[missing_mask, "LAYOUT_ID"] = ""

    # ---------------------------------------------------
    # Build IOR-compatible core columns
    # ---------------------------------------------------
    output = pd.DataFrame(
        {
            "LAYOUT_ID": merged["LAYOUT_ID"],
            "DOCID": merged["DOC_ID"],
            "INDEX_DATE": merged["ENTRY_DATE"],
            "BUDAT": merged["POSTING_DATE"],
            "O_BLDAT": merged["DOC_DATE_ORIG"],
            "BLDAT": merged["DOC_DATE_CURR"],
            "O_DOCTYPE": merged["INVOICE_TYPE_ORIG"],
            "DOCTYPE": merged["INVOICE_TYPE_CURR"],
            "O_BUKRS": merged["COMPANY_CODE_ORIG"],
            "BUKRS": merged["COMPANY_CODE_CURR"],
            "O_LIFNR": merged["VENDOR_NUM_ORIG"],
            "LIFNR": merged["VENDOR_NUM_CURR"],
            "O_Vend_Name": merged["VENDOR_NAME_1_ORIG"],
            "VEND_NAME": merged["VENDOR_NAME_1_CURR"],
            "O_Vend_Name2": merged["VENDOR_NAME_2_ORIG"],
            "VEND_NAME2": merged["VENDOR_NAME_2_CURR"],
            "M_ABN": merged["ABN_CURR"],
            "O_VENDOR_VAT_NO": merged["ABN_ORIG"],
            "VENDOR_VAT_NO": merged["ABN_CURR"],
            "O_EBELN": merged["PO_NUM_ORIG"],
            "EBELN": merged["PO_NUM_CURR"],
            "O_XBLNR": merged["INVOICE_NUMBER_ORIG"],
            "XBLNR": merged["INVOICE_NUMBER_CURR"],
            "O_RMWWR": merged["AMOUNT_ORIG"],
            "RMWWR": merged["AMOUNT_CURR"],
            "O_OBJTXT": merged["STATUS_TEXT_ORIG"],
            "OBJTXT": merged["STATUS_TEXT_CURR"],
            "Issue": merged["STATUS_TEXT_CURR"],
            "IssueRectifiedDate": merged["PO_LAST_UPDATED"],
            "PO_LASTCHANGEDATETIME": merged["PO_LAST_UPDATED"],
            "Invoice_Entry_Time_Stamp": merged["ENTRY_DATE_AND_TIME"],
        }
    )

    # ---------------------------------------------------
    # High-level issue flags (1 per category) + ISSUE_COUNT
    # ---------------------------------------------------
    sentinel = "__NULL__"

    # 1. Status / object text issue: original vs current status text
    output["ISSUE_OBJECTTEXT"] = (
        output["O_OBJTXT"].fillna(sentinel) != output["OBJTXT"].fillna(sentinel)
    ).astype(int)

    # 2. Company code issue: changed or missing/invalid
    # Treat blank or '9999' as invalid company code
    bukrs_orig = output["O_BUKRS"].fillna("")
    bukrs_curr = output["BUKRS"].fillna("")
    invalid_cc = bukrs_curr.eq("") | bukrs_curr.eq("9999")
    output["ISSUE_COMPANY_CODE"] = (
        (bukrs_orig != bukrs_curr) | invalid_cc
    ).astype(int)

    # 3. Supplier issue: any difference in vendor number or names
    issue_supplier = (
        (output["O_LIFNR"].fillna(sentinel) != output["LIFNR"].fillna(sentinel))
        | (output["O_Vend_Name"].fillna(sentinel) != output["VEND_NAME"].fillna(sentinel))
        | (output["O_Vend_Name2"].fillna(sentinel) != output["VEND_NAME2"].fillna(sentinel))
    )
    output["ISSUE_SUPPLIER"] = issue_supplier.astype(int)

    # 4. Invoice number issue
    output["ISSUE_INVOICE_NUMBER"] = (
        output["O_XBLNR"].fillna(sentinel) != output["XBLNR"].fillna(sentinel)
    ).astype(int)

    # 5. Invoice date issue (document date)
    output["ISSUE_INVOICE_DATE"] = (
        output["O_BLDAT"].fillna(sentinel) != output["BLDAT"].fillna(sentinel)
    ).astype(int)

    # 6. ABN issue
    output["ISSUE_ABN"] = (
        output["O_VENDOR_VAT_NO"].fillna(sentinel) != output["VENDOR_VAT_NO"].fillna(sentinel)
    ).astype(int)

    # 7. Amount issue
    output["ISSUE_AMOUNT"] = (
        output["O_RMWWR"].fillna(sentinel) != output["RMWWR"].fillna(sentinel)
    ).astype(int)

    # 8. Date range issue (simple rule: DOC_DATE outside project window)
    # You can tune these thresholds to your actual business rule.
    project_start = pd.Timestamp("2023-07-01")
    doc_dates = pd.to_datetime(output["BLDAT"], errors="coerce")
    output["ISSUE_DATE_RANGE"] = doc_dates.lt(project_start).fillna(False).astype(int)

    # Final ISSUE_COUNT over these high-level categories
    issue_cols = [
        "ISSUE_OBJECTTEXT",
        "ISSUE_COMPANY_CODE",
        "ISSUE_DATE_RANGE",
        "ISSUE_SUPPLIER",
        "ISSUE_INVOICE_NUMBER",
        "ISSUE_INVOICE_DATE",
        "ISSUE_ABN",
        "ISSUE_AMOUNT",
    ]
    output["ISSUE_COUNT"] = output[issue_cols].sum(axis=1)

    sub(
        f"[ChangedData] Built {len(output):,} Changed Data output row(s) "
        "for new DOC_IDs with issue category flags."
    )

    return output


def run_changed_data_capture(
    transaction_master_csv: str,
    originals_csv: str,
    changed_csv: str,
) -> int:
    """
    Orchestrate the full Changed Data capture process.
    """
    step_header("STEP: Changed Data Capture")
    sub("[ChangedData] Starting Changed Data capture...")

    originals_df = load_originals_dataframe(originals_csv)
    if originals_df.empty:
        sub(
            "[ChangedData] Originals DataFrame is empty. "
            "No changes can be detected. Exiting Changed Data capture."
        )
        print("=" * 55)
        return 0

    tm_df = load_transaction_master_dataframe(transaction_master_csv)

    compare_columns = [
        "DOC_DATE",
        "INVOICE_TYPE",
        "COMPANY_CODE",
        "VENDOR_NUM",
        "VENDOR_NAME_1",
        "VENDOR_NAME_2",
        "ABN",
        "PO_NUM",
        "INVOICE_NUMBER",
        "AMOUNT",
        "STATUS_TEXT",
    ]

    changed_df = detect_changed_rows(
        originals_df=originals_df,
        tm_df=tm_df,
        compare_columns=compare_columns,
    )

    if changed_df.empty:
        sub("[ChangedData] No DOC_IDs with differences found. Nothing to do.")
        print("=" * 55)
        return 0

    changed_posted_df = filter_posted_changes(changed_df)

    if changed_posted_df.empty:
        sub(
            "[ChangedData] No changed DOC_IDs are currently in 'Posted' status. "
            "Nothing to append."
        )
        print("=" * 55)
        return 0

    existing_changed_df = load_existing_changed_data(changed_csv)

    new_changes_df = filter_new_changes(changed_posted_df, existing_changed_df)

    if new_changes_df.empty:
        sub(
            "[ChangedData] All changed-and-posted DOC_IDs are already present "
            "in the Changed Data CSV. Nothing new to append."
        )
        print("=" * 55)
        return 0

    output_rows = build_changed_output_rows(new_changes_df, tm_df)

    if output_rows.empty:
        sub(
            "[ChangedData] After building output rows, no data remained. "
            "Nothing written."
        )
        print("=" * 55)
        return 0

    changed_dir = os.path.dirname(changed_csv)
    if changed_dir:
        os.makedirs(changed_dir, exist_ok=True)

    file_exists = os.path.exists(changed_csv)

    output_rows.to_csv(
        changed_csv,
        mode="a" if file_exists else "w",
        header=not file_exists,
        index=False,
    )

    rows_written = len(output_rows)
    sub(
        f"[ChangedData] Appended {rows_written:,} new row(s) to "
        f"Changed Data CSV at '{changed_csv}'."
    )
    print("=" * 55)

    return rows_written
