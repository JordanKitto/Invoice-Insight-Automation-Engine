# relates to: tests/test_step3_append_idempotent.py
import os
import sqlite3
import tempfile
import gc
import pandas as pd

from Utils.originals_store import (
    ensure_db,
    append_new_from_csv,
    load_existing_docids,
)

def _make_csv(folder, rows):
    path = os.path.join(folder, "transaction_master.csv")
    pd.DataFrame(rows).to_csv(path, index=False)  # FIX: index, not indexx
    return path

def test_append_only_new_and_idempotent():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "originals.db")
        out_dir = os.path.join(tmp, "Output_Files")
        os.makedirs(out_dir, exist_ok=True)

        ensure_db(db_path)

        # Day 1: two unique DOCIDs plus an in-file duplicate of A2
        csv1 = _make_csv(out_dir, [
            {"DOCID": "A1", "VEND_ID": "V1"},
            {"DOCID": "A2", "VEND_ID": "V2"},
            {"DOCID": "A2", "VEND_ID": "V2"},
        ])

        inserted1 = append_new_from_csv(csv1, db_path=db_path, chunksize=2_000)
        assert inserted1 == 2

        # Re-run same file: zero new inserts
        inserted2 = append_new_from_csv(csv1, db_path=db_path, chunksize=2_000)
        assert inserted2 == 0

        # Day 2: one existing and one new DOCID
        csv2 = _make_csv(out_dir, [
            {"DOCID": "A2", "VEND_ID": "V2"},
            {"DOCID": "A3", "VEND_ID": "V3"},
        ])

        inserted3 = append_new_from_csv(csv2, db_path=db_path, chunksize=2_000)
        assert inserted3 == 1

        # Verify stored set
        ids = load_existing_docids(db_path)
        assert ids == {"A1", "A2", "A3"}

        # Ensure file handles are released before TemporaryDirectory cleanup on Windows
        gc.collect()
