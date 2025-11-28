# File: tests/test_changed_data_csv.py

import os
import pandas as pd

from Utils.changed_data_csv import (
    load_originals_dataframe,
    load_transaction_master_dataframe,
    detect_changed_rows,
    load_existing_changed_data,
    filter_new_changes,
    filter_posted_changes,
    build_changed_output_rows,
    run_changed_data_capture,
)


def test_load_originals_dataframe_returns_empty_when_file_missing(tmp_path):
    """
    If the Originals CSV file path does not exist,
    the function should return an empty DataFrame.
    """
    missing_file = tmp_path / "non_existent_originals.csv"

    df = load_originals_dataframe(str(missing_file))

    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert df.shape == (0, 0)


def test_load_originals_dataframe_loads_existing_file(tmp_path):
    """
    If the Originals CSV file exists, the function should load it
    into a DataFrame with matching content.
    """
    csv_path = tmp_path / "Original_Invoice_Data_CSV.csv"
    csv_content = (
        "DOC_ID,INVOICE_TYPE,ENTRY_DATE,AMOUNT\n"
        "1001,INV,2025-01-01,123.45\n"
        "1002,CRN,2025-01-02,67.89\n"
    )
    csv_path.write_text(csv_content, encoding="utf-8")

    df = load_originals_dataframe(str(csv_path))

    assert not df.empty
    assert df.shape == (2, 4)
    assert list(df.columns) == ["DOC_ID", "INVOICE_TYPE", "ENTRY_DATE", "AMOUNT"]
    assert df.loc[0, "DOC_ID"] == 1001
    assert df.loc[0, "INVOICE_TYPE"] == "INV"
    assert df.loc[1, "AMOUNT"] == 67.89


def test_load_transaction_master_dataframe_raises_if_file_missing(tmp_path):
    """
    Transaction Master loader should raise FileNotFoundError
    if the CSV file does not exist.
    """
    missing_file = tmp_path / "non_existent_tm.csv"

    try:
        load_transaction_master_dataframe(str(missing_file))
        assert False, "Expected FileNotFoundError to be raised"
    except FileNotFoundError as exc:
        msg = str(exc)
        assert "Transaction Master CSV not found" in msg


def test_load_transaction_master_dataframe_raises_if_doc_id_missing(tmp_path):
    """
    Transaction Master CSV must contain DOC_ID column.
    """
    csv_path = tmp_path / "transaction_master.csv"
    csv_content = (
        "INVOICE_TYPE,ENTRY_DATE,AMOUNT\n"
        "INV,2025-01-01,123.45\n"
    )
    csv_path.write_text(csv_content, encoding="utf-8")

    try:
        load_transaction_master_dataframe(str(csv_path))
        assert False, "Expected ValueError due to missing DOC_ID"
    except ValueError as exc:
        msg = str(exc)
        assert "missing required column 'DOC_ID'" in msg


def test_load_transaction_master_dataframe_raises_if_doc_id_not_unique(tmp_path):
    """
    Transaction Master CSV must have unique DOC_ID values.
    """
    csv_path = tmp_path / "transaction_master.csv"
    csv_content = (
        "DOC_ID,INVOICE_TYPE,ENTRY_DATE,AMOUNT\n"
        "1001,INV,2025-01-01,123.45\n"
        "1001,INV,2025-01-02,200.00\n"
    )
    csv_path.write_text(csv_content, encoding="utf-8")

    try:
        load_transaction_master_dataframe(str(csv_path))
        assert False, "Expected ValueError due to duplicate DOC_ID values"
    except ValueError as exc:
        msg = str(exc)
        assert "duplicate DOC_ID" in msg


def test_load_transaction_master_dataframe_loads_valid_file(tmp_path):
    """
    Valid Transaction Master CSV with unique DOC_ID values
    should load into a non-empty DataFrame.
    """
    csv_path = tmp_path / "transaction_master.csv"
    csv_content = (
        "DOC_ID,INVOICE_TYPE,ENTRY_DATE,AMOUNT\n"
        "1001,INV,2025-01-01,123.45\n"
        "1002,CRN,2025-01-02,67.89\n"
    )
    csv_path.write_text(csv_content, encoding="utf-8")

    df = load_transaction_master_dataframe(str(csv_path))

    assert not df.empty
    assert df.shape == (2, 4)
    assert list(df.columns) == ["DOC_ID", "INVOICE_TYPE", "ENTRY_DATE", "AMOUNT"]
    assert df["DOC_ID"].tolist() == [1001, 1002]

    # ---------- Tests for detect_changed_rows ----------


def test_detect_changed_rows_returns_empty_if_originals_empty():
    originals_df = pd.DataFrame()
    tm_df = pd.DataFrame(
        {
            "DOC_ID": [1, 2],
            "AMOUNT": [100.0, 200.0],
        }
    )

    changed = detect_changed_rows(originals_df, tm_df, compare_columns=["AMOUNT"])
    assert changed.empty


def test_detect_changed_rows_returns_empty_if_tm_empty():
    originals_df = pd.DataFrame(
        {
            "DOC_ID": [1, 2],
            "AMOUNT": [100.0, 200.0],
        }
    )
    tm_df = pd.DataFrame()

    changed = detect_changed_rows(originals_df, tm_df, compare_columns=["AMOUNT"])
    assert changed.empty


def test_detect_changed_rows_raises_if_required_columns_missing():
    originals_df = pd.DataFrame({"DOC_ID": [1], "AMOUNT": [100.0]})
    tm_df = pd.DataFrame({"DOC_ID": [1], "OTHER": [999]})

    try:
        detect_changed_rows(originals_df, tm_df, compare_columns=["AMOUNT"])
        assert False, "Expected ValueError due to missing AMOUNT column in TM"
    except ValueError as exc:
        msg = str(exc)
        assert "missing columns" in msg


def test_detect_changed_rows_no_differences():
    originals_df = pd.DataFrame(
        {
            "DOC_ID": [1, 2],
            "AMOUNT": [100.0, 200.0],
            "STATUS_TEXT": ["Created", "Created"],
        }
    )
    tm_df = pd.DataFrame(
        {
            "DOC_ID": [1, 2],
            "AMOUNT": [100.0, 200.0],
            "STATUS_TEXT": ["Created", "Created"],
        }
    )

    changed = detect_changed_rows(
        originals_df,
        tm_df,
        compare_columns=["AMOUNT", "STATUS_TEXT"],
    )

    assert changed.empty


def test_detect_changed_rows_detects_single_change():
    originals_df = pd.DataFrame(
        {
            "DOC_ID": [1, 2],
            "AMOUNT": [100.0, 200.0],
            "STATUS_TEXT": ["Created", "Created"],
        }
    )
    tm_df = pd.DataFrame(
        {
            "DOC_ID": [1, 2],
            "AMOUNT": [150.0, 200.0],          # DOC_ID 1 amount changed
            "STATUS_TEXT": ["Created", "Paid"],
        }
    )

    changed = detect_changed_rows(
        originals_df,
        tm_df,
        compare_columns=["AMOUNT", "STATUS_TEXT"],
    )

    # DOC_ID 1: AMOUNT changed; DOC_ID 2: STATUS_TEXT changed
    assert not changed.empty
    assert set(changed["DOC_ID"].tolist()) == {1, 2}
    # Check the suffixes exist
    assert "AMOUNT_ORIG" in changed.columns
    assert "AMOUNT_CURR" in changed.columns
    assert "STATUS_TEXT_ORIG" in changed.columns
    assert "STATUS_TEXT_CURR" in changed.columns
    # Check one value explicitly
    row1 = changed[changed["DOC_ID"] == 1].iloc[0]
    assert row1["AMOUNT_ORIG"] == 100.0
    assert row1["AMOUNT_CURR"] == 150.0


def test_detect_changed_rows_treats_nan_equals_nan_as_no_change():
    originals_df = pd.DataFrame(
        {
            "DOC_ID": [1, 2],
            "AMOUNT": [None, 200.0],
        }
    )
    tm_df = pd.DataFrame(
        {
            "DOC_ID": [1, 2],
            "AMOUNT": [None, 250.0],
        }
    )

    changed = detect_changed_rows(
        originals_df,
        tm_df,
        compare_columns=["AMOUNT"],
    )

    # DOC_ID 1: None vs None -> no change
    # DOC_ID 2: 200 vs 250 -> change
    assert set(changed["DOC_ID"].tolist()) == {2}


def test_filter_posted_changes_returns_empty_when_input_empty():
    """
    If the changed_df is empty, the filter should return an empty DataFrame.
    """
    empty_df = pd.DataFrame()
    result = filter_posted_changes(empty_df)

    assert result.empty
    assert isinstance(result, pd.DataFrame)


def test_filter_posted_changes_keeps_only_posted():
    """
    Only rows with STATUS_TEXT_CURR == 'Posted' (case-insensitive)
    should be kept.
    """
    changed_df = pd.DataFrame({
        "DOC_ID": [1, 2, 3],
        "STATUS_TEXT_ORIG": ["Created", "Created", "Created"],
        "STATUS_TEXT_CURR": ["Posted", "Cancelled", "posted"],  # 1 and 3 qualify
    })

    result = filter_posted_changes(changed_df)

    # DOC_ID 1 and 3 should remain
    assert set(result["DOC_ID"].tolist()) == {1, 3}
    # Ensure the shape matches expected rows
    assert result.shape[0] == 2


def test_filter_posted_changes_raises_if_status_column_missing():
    """
    If STATUS_TEXT_CURR is missing, the function should raise ValueError.
    """
    changed_df = pd.DataFrame({
        "DOC_ID": [1],
        "STATUS_TEXT_ORIG": ["Created"],
        # STATUS_TEXT_CURR is intentionally missing
    })

    try:
        filter_posted_changes(changed_df)
        assert False, "Expected ValueError due to missing STATUS_TEXT_CURR"
    except ValueError as exc:
        msg = str(exc)
        assert "STATUS_TEXT_CURR" in msg
    
def test_build_changed_output_rows_returns_empty_when_no_new_changes():
    new_changes_df = pd.DataFrame()
    tm_df = pd.DataFrame(
        {
            "DOC_ID": [1],
            "LAYOUT_ID": ["abcd"],
            "ENTRY_DATE": ["2025-01-01T00:00"],
            "POSTING_DATE": ["2025-01-02T00:00"],
            "PO_LAST_UPDATED": ["2025-01-03T10:00:00"],
            "ENTRY_DATE_AND_TIME": ["2025-01-01T09:00:00"],
        }
    )

    output = build_changed_output_rows(new_changes_df, tm_df)

    assert output.empty
    assert isinstance(output, pd.DataFrame)

def test_build_changed_output_rows_single_docid():
    # Synthetic new_changes_df mimicking detect_changed_rows output
    new_changes_df = pd.DataFrame(
        {
            "DOC_ID": [8756207],
            "DOC_DATE_ORIG": ["2024-12-31T00:00"],
            "DOC_DATE_CURR": ["2024-12-31T00:00"],
            "INVOICE_TYPE_ORIG": ["ZPO_INV"],
            "INVOICE_TYPE_CURR": ["ZPO_INV"],
            "COMPANY_CODE_ORIG": [1000],
            "COMPANY_CODE_CURR": [1000],
            "VENDOR_NUM_ORIG": [3019338],
            "VENDOR_NUM_CURR": [3019338],
            "VENDOR_NAME_1_ORIG": ["FRESENIUS MEDICAL CARE AUST P/L"],
            "VENDOR_NAME_1_CURR": ["FRESENIUS MEDICAL CARE AUST P/L"],
            "VENDOR_NAME_2_ORIG": [None],
            "VENDOR_NAME_2_CURR": [None],
            "ABN_ORIG": ["80067557877"],
            "ABN_CURR": ["80067557877"],
            "PO_NUM_ORIG": ["4304352813"],
            "PO_NUM_CURR": ["4304352813"],
            "INVOICE_NUMBER_ORIG": ["2993523274"],
            "INVOICE_NUMBER_CURR": ["2993523274"],
            "AMOUNT_ORIG": [96096],
            "AMOUNT_CURR": [96096],
            "STATUS_TEXT_ORIG": ["Created"],
            "STATUS_TEXT_CURR": ["Cancelled"],
        }
    )

    tm_df = pd.DataFrame(
        {
            "DOC_ID": [8756207],
            "LAYOUT_ID": ["f0e4fc08d2464b8ab66f6884b605972e"],
            "ENTRY_DATE": ["2025-01-01T00:00"],
            "POSTING_DATE": ["2025-01-06T00:00"],
            "PO_LAST_UPDATED": ["2025-06-19T09:56:31"],
            "ENTRY_DATE_AND_TIME": ["2025-01-03T07:07:33"],
        }
    )

    output = build_changed_output_rows(new_changes_df, tm_df)

    # One row, with the expected columns and mappings
    assert output.shape[0] == 1

    expected_cols = [
        "LAYOUT_ID",
        "DOCID",
        "INDEX_DATE",
        "BUDAT",
        "O_BLDAT",
        "BLDAT",
        "O_DOCTYPE",
        "DOCTYPE",
        "O_BUKRS",
        "BUKRS",
        "O_LIFNR",
        "LIFNR",
        "O_Vend_Name",
        "VEND_NAME",
        "O_Vend_Name2",
        "VEND_NAME2",
        "M_ABN",
        "O_VENDOR_VAT_NO",
        "VENDOR_VAT_NO",
        "O_EBELN",
        "EBELN",
        "O_XBLNR",
        "XBLNR",
        "O_RMWWR",
        "RMWWR",
        "O_OBJTXT",
        "OBJTXT",
        "Issue",
        "IssueRectifiedDate",
        "PO_LASTCHANGEDATETIME",
        "Invoice_Entry_Time_Stamp",
    ]
    assert list(output.columns) == expected_cols

    row = output.iloc[0]
    assert row["LAYOUT_ID"] == "f0e4fc08d2464b8ab66f6884b605972e"
    assert row["DOCID"] == 8756207
    assert row["INDEX_DATE"] == "2025-01-01T00:00"
    assert row["BUDAT"] == "2025-01-06T00:00"
    assert row["O_BLDAT"] == "2024-12-31T00:00"
    assert row["BLDAT"] == "2024-12-31T00:00"
    assert row["O_OBJTXT"] == "Created"
    assert row["OBJTXT"] == "Cancelled"
    assert row["Issue"] == "Cancelled"
    assert row["IssueRectifiedDate"] == "2025-06-19T09:56:31"
    assert row["PO_LASTCHANGEDATETIME"] == "2025-06-19T09:56:31"
    assert row["Invoice_Entry_Time_Stamp"] == "2025-01-03T07:07:33"

def test_build_changed_output_rows_raises_when_tm_missing_required_cols():
    new_changes_df = pd.DataFrame(
        {
            "DOC_ID": [1],
            "DOC_DATE_ORIG": ["2025-01-01"],
            "DOC_DATE_CURR": ["2025-01-02"],
            "INVOICE_TYPE_ORIG": ["INV"],
            "INVOICE_TYPE_CURR": ["INV"],
            "COMPANY_CODE_ORIG": [1000],
            "COMPANY_CODE_CURR": [1000],
            "VENDOR_NUM_ORIG": [123],
            "VENDOR_NUM_CURR": [123],
            "VENDOR_NAME_1_ORIG": ["A"],
            "VENDOR_NAME_1_CURR": ["A"],
            "VENDOR_NAME_2_ORIG": [""],
            "VENDOR_NAME_2_CURR": [""],
            "ABN_ORIG": ["111"],
            "ABN_CURR": ["111"],
            "PO_NUM_ORIG": ["PO1"],
            "PO_NUM_CURR": ["PO1"],
            "INVOICE_NUMBER_ORIG": ["INV1"],
            "INVOICE_NUMBER_CURR": ["INV1"],
            "AMOUNT_ORIG": [10.0],
            "AMOUNT_CURR": [20.0],
            "STATUS_TEXT_ORIG": ["Created"],
            "STATUS_TEXT_CURR": ["Posted"],
        }
    )

    # TM missing LAYOUT_ID, ENTRY_DATE, etc.
    tm_df = pd.DataFrame({"DOC_ID": [1]})

    try:
        build_changed_output_rows(new_changes_df, tm_df)
        assert False, "Expected ValueError due to missing TM columns"
    except ValueError as exc:
        msg = str(exc)
        assert "missing required columns" in msg

def test_run_changed_data_capture_end_to_end(tmp_path):
    """
    End-to-end test of run_changed_data_capture with:
    - Originals containing DOC_ID 1 and 2
    - TM where:
        * DOC_ID 1 changed and is Posted
        * DOC_ID 2 changed but is not Posted
    - No existing Changed Data CSV

    Expected:
        Only DOC_ID 1 is appended to the Changed Data CSV.
    """
    # Paths inside tmp_path
    originals_path = tmp_path / "Original_Invoice_Data_CSV.csv"
    tm_path = tmp_path / "transaction_master.csv"
    changed_path = tmp_path / "Changed_Data.csv"

    # Originals: first snapshot
    originals_csv = (
        "DOC_ID,DOC_DATE,INVOICE_TYPE,COMPANY_CODE,VENDOR_NUM,"
        "VENDOR_NAME_1,VENDOR_NAME_2,ABN,PO_NUM,INVOICE_NUMBER,AMOUNT,STATUS_TEXT\n"
        "1,2025-01-01,INV,1000,3001,Vendor A,,111,PO1,INV1,100.0,Created\n"
        "2,2025-01-01,INV,1000,3002,Vendor B,,222,PO2,INV2,200.0,Created\n"
    )
    originals_path.write_text(originals_csv, encoding="utf-8")

    # Transaction Master: current snapshot
    # DOC_ID 1: amount and status changed, Posted
    # DOC_ID 2: amount changed, but status is Cancelled
    tm_csv = (
        "DOC_ID,DOC_DATE,INVOICE_TYPE,COMPANY_CODE,VENDOR_NUM,"
        "VENDOR_NAME_1,VENDOR_NAME_2,ABN,PO_NUM,INVOICE_NUMBER,AMOUNT,STATUS_TEXT,"
        "LAYOUT_ID,ENTRY_DATE,POSTING_DATE,PO_LAST_UPDATED,ENTRY_DATE_AND_TIME\n"
        "1,2025-01-01,INV,1000,3001,Vendor A,,111,PO1,INV1,150.0,Posted,"
        "LAY1,2025-01-05T00:00,2025-01-06T00:00,2025-01-07T10:00:00,2025-01-05T09:00:00\n"
        "2,2025-01-01,INV,1000,3002,Vendor B,,222,PO2,INV2,250.0,Cancelled,"
        "LAY2,2025-01-05T00:00,2025-01-06T00:00,2025-01-07T10:00:00,2025-01-05T09:00:00\n"
    )
    tm_path.write_text(tm_csv, encoding="utf-8")

    # No existing Changed_Data.csv (start fresh)
    assert not changed_path.exists()

    # Run the capture
    rows_written = run_changed_data_capture(
        transaction_master_csv=str(tm_path),
        originals_csv=str(originals_path),
        changed_csv=str(changed_path),
    )

    # Only DOC_ID 1 should be written
    assert rows_written == 1
    assert changed_path.exists()

    # Inspect the resulting Changed Data CSV
    changed_df = pd.read_csv(changed_path)

    # Expect one row, DOCID == 1, and status 'Posted'
    assert changed_df.shape[0] == 1
    assert changed_df.loc[0, "DOCID"] == 1
    assert changed_df.loc[0, "OBJTXT"] == "Posted"  # STATUS_TEXT_CURR
    assert changed_df.loc[0, "O_OBJTXT"] == "Created"
    # Amounts should differ original vs current
    assert changed_df.loc[0, "O_RMWWR"] == 100.0
    assert changed_df.loc[0, "RMWWR"] == 150.0
