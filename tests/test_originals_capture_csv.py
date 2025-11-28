from datetime import datetime ,timedelta
from pathlib import Path
import pandas as pd

from Utils.originals_capture_csv import run_originals_capture


def _recent_date_str(days_ago: int = 1) -> str:
    """
    Helper: returns an ISO date string 'YYYY-MM-DD' for a day in the recent past.
    """
    d = datetime.today().date() - timedelta(days=days_ago)
    return d.strftime("%Y-%m-%d")


def _old_date_str(days_ago: int = 60) -> str:
    """
    Helper: returns an ISO date string far enough in the past to fall
    outside the default 30-day window.
    """
    d = datetime.today().date() - timedelta(days=days_ago)
    return d.strftime("%Y-%m-%d")


def test_first_run_appends_all_recent(tmp_path):
    """
    Scenario: Originals file does not exist yet.
    - Transaction Master has 3 recent invoices.
    - We run originals capture once.
    Expectation:
    - All 3 DOC_IDs are appended.
    """
    tm_path: Path = tmp_path / "transaction_master.csv"
    originals_path: Path = tmp_path / "Original_Invoice_Data.csv"

    recent = _recent_date_str(1)

    rows = [
        {
            "DOC_ID": "000000000001",
            "INVOICE_TYPE": "ZPO_INV",
            "ENTRY_DATE": recent,
            "COMPANY_CODE": "1000",
            "DOC_DATE": recent,
            "INVOICE_NUMBER": "INV-1",
            "AMOUNT": "100.00",
            "VENDOR_NUM": "V001",
            "VENDOR_NAME_1": "ACME",
            "VENDOR_NAME_2": "",
            "PO_NUM": "PO1",
            "ABN": "123",
            "DSS_DOWNLOAD_DATE": recent,
            "STATUS_TEXT": "Posted",
        },
        {
            "DOC_ID": "000000000002",
            "INVOICE_TYPE": "ZPO_INV",
            "ENTRY_DATE": recent,
            "COMPANY_CODE": "1000",
            "DOC_DATE": recent,
            "INVOICE_NUMBER": "INV-2",
            "AMOUNT": "200.00",
            "VENDOR_NUM": "V002",
            "VENDOR_NAME_1": "BETA",
            "VENDOR_NAME_2": "",
            "PO_NUM": "PO2",
            "ABN": "456",
            "DSS_DOWNLOAD_DATE": recent,
            "STATUS_TEXT": "Posted",
        },
        {
            "DOC_ID": "000000000003",
            "INVOICE_TYPE": "ZPO_INV",
            "ENTRY_DATE": recent,
            "COMPANY_CODE": "1000",
            "DOC_DATE": recent,
            "INVOICE_NUMBER": "INV-3",
            "AMOUNT": "300.00",
            "VENDOR_NUM": "V003",
            "VENDOR_NAME_1": "GAMMA",
            "VENDOR_NAME_2": "",
            "PO_NUM": "PO3",
            "ABN": "789",
            "DSS_DOWNLOAD_DATE": recent,
            "STATUS_TEXT": "Posted",
        },
    ]

    # Write Transaction Master CSV
    pd.DataFrame(rows).to_csv(tm_path, index=False)

    # Run capture with default 30-day window
    written = run_originals_capture(
        transaction_master_csv=str(tm_path),
        originals_csv=str(originals_path),
        days_back=30,
    )

    # We expect all 3 to be considered new
    assert written == 3

    # Originals file should now contain exactly 3 rows
    orig = pd.read_csv(originals_path, dtype=str)
    assert len(orig) == 3
    assert sorted(orig["DOC_ID"].tolist()) == [
        "000000000001",
        "000000000002",
        "000000000003",
    ]


def test_second_run_is_idempotent(tmp_path):
    """
    Scenario: Originals already contains the recent invoices.
    - First run appends 2 rows.
    - Second run with the same Transaction Master should append 0.
    Expectation:
    - First run written == 2
    - Second run written == 0
    - Originals row count remains 2
    """
    tm_path: Path = tmp_path / "transaction_master.csv"
    originals_path: Path = tmp_path / "Original_Invoice_Data.csv"

    recent = _recent_date_str(1)

    rows = [
        {
            "DOC_ID": "000000000010",
            "INVOICE_TYPE": "ZPO_INV",
            "ENTRY_DATE": recent,
            "COMPANY_CODE": "1000",
            "DOC_DATE": recent,
            "INVOICE_NUMBER": "INV-10",
            "AMOUNT": "10.00",
            "VENDOR_NUM": "V010",
            "VENDOR_NAME_1": "DELTA",
            "VENDOR_NAME_2": "",
            "PO_NUM": "PO10",
            "ABN": "111",
            "DSS_DOWNLOAD_DATE": recent,
            "STATUS_TEXT": "Posted",
        },
        {
            "DOC_ID": "000000000011",
            "INVOICE_TYPE": "ZPO_INV",
            "ENTRY_DATE": recent,
            "COMPANY_CODE": "1000",
            "DOC_DATE": recent,
            "INVOICE_NUMBER": "INV-11",
            "AMOUNT": "11.00",
            "VENDOR_NUM": "V011",
            "VENDOR_NAME_1": "EPSILON",
            "VENDOR_NAME_2": "",
            "PO_NUM": "PO11",
            "ABN": "222",
            "DSS_DOWNLOAD_DATE": recent,
            "STATUS_TEXT": "Posted",
        },
    ]

    pd.DataFrame(rows).to_csv(tm_path, index=False)

    # First run: originals file does not exist yet
    written1 = run_originals_capture(
        transaction_master_csv=str(tm_path),
        originals_csv=str(originals_path),
        days_back=30,
    )
    assert written1 == 2

    # Second run: with the same TM file, there should be no new DOC_IDs
    written2 = run_originals_capture(
        transaction_master_csv=str(tm_path),
        originals_csv=str(originals_path),
        days_back=30,
    )
    assert written2 == 0

    # Row count should stay 2
    orig = pd.read_csv(originals_path, dtype=str)
    assert len(orig) == 2
    assert sorted(orig["DOC_ID"].tolist()) == [
        "000000000010",
        "000000000011",
    ]


def test_date_filter_skips_old_invoices(tmp_path):
    """
    Scenario: Transaction Master has one old invoice and one recent invoice.
    - ENTRY_DATE for one row is > 60 days ago.
    - ENTRY_DATE for the other is yesterday.
    Expectation with days_back=30:
    - Only the recent invoice is appended.
    """
    tm_path: Path = tmp_path / "transaction_master.csv"
    originals_path: Path = tmp_path / "Original_Invoice_Data.csv"

    recent = _recent_date_str(1)
    old = _old_date_str(60)

    rows = [
        {
            "DOC_ID": "000000000020",          # old invoice
            "INVOICE_TYPE": "ZPO_INV",
            "ENTRY_DATE": old,
            "COMPANY_CODE": "1000",
            "DOC_DATE": old,
            "INVOICE_NUMBER": "INV-20",
            "AMOUNT": "20.00",
            "VENDOR_NUM": "V020",
            "VENDOR_NAME_1": "OLD_VENDOR",
            "VENDOR_NAME_2": "",
            "PO_NUM": "PO20",
            "ABN": "333",
            "DSS_DOWNLOAD_DATE": old,
            "STATUS_TEXT": "Posted",
        },
        {
            "DOC_ID": "000000000021",          # recent invoice
            "INVOICE_TYPE": "ZPO_INV",
            "ENTRY_DATE": recent,
            "COMPANY_CODE": "1000",
            "DOC_DATE": recent,
            "INVOICE_NUMBER": "INV-21",
            "AMOUNT": "21.00",
            "VENDOR_NUM": "V021",
            "VENDOR_NAME_1": "RECENT_VENDOR",
            "VENDOR_NAME_2": "",
            "PO_NUM": "PO21",
            "ABN": "444",
            "DSS_DOWNLOAD_DATE": recent,
            "STATUS_TEXT": "Posted",
        },
    ]

    pd.DataFrame(rows).to_csv(tm_path, index=False)

    written = run_originals_capture(
        transaction_master_csv=str(tm_path),
        originals_csv=str(originals_path),
        days_back=30,   # 30-day window
    )

    # Only the recent one should be considered in-window
    assert written == 1

    orig = pd.read_csv(originals_path, dtype=str)
    assert len(orig) == 1
    assert orig["DOC_ID"].iloc[0] == "000000000021"
