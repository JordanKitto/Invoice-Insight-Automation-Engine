import pandas as pd
from Utils.originals_store import normalize_columns, REQUIRED_COLS

def test_normalize_columns_maps_and_orders_headers():
    raw = pd.DataFrame([
        {"docid": "A1", "vend_name": "Acme", "vend_id": "V1", "index_date": "2025-11-01"},
        {"docid": "A2", "vend_name": "Globex", "vend_id": "V2", "index_date": "2025-11-02"},
    ])

    out = normalize_columns(raw)

    # DOCID is present and at index 0
    assert "DOCID" in out.columns
    assert list(out.columns)[0] == "DOCID"

    # Only known columns are kept; order must follow REQUIRED_COLS where present
    assert set(out.columns).issubset(set(REQUIRED_COLS))
    expected_prefix = ["DOCID", "VEND_ID", "VEND_NAME", "INDEX_DATE"]
    assert list(out.columns)[:4] == expected_prefix

    
