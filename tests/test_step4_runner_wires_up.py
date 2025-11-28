import os
import sqlite3
import tempfile
import pandas as pd
import time
import shutil

from Utils.originals_store import ensure_db, append_new_from_csv, DEFAULT_TABLE

def test_runner_like_flow_end_to_end():
    # Step 1: create an isolated temp directory
    tmpdir = tempfile.mkdtemp()
    try:
        # Step 2: put database INSIDE that directory
        db_path = os.path.join(tmpdir, "originals.db")

        # Step 3: create CSV in a separate safe temp folder
        out_dir = tempfile.mkdtemp()
        csv_path = os.path.join(out_dir, "transaction_master.csv")

        pd.DataFrame([{"DOCID": "Z1", "VEND_ID": "V9"}]).to_csv(csv_path, index=False)

        # Step 4: create DB
        ensure_db(db_path)

        # Step 5: insert once
        n1 = append_new_from_csv(csv_path, db_path=db_path)
        assert n1 == 1

        # Step 6: idempotent second pass
        n2 = append_new_from_csv(csv_path, db_path=db_path)
        assert n2 == 0

        # Step 7: inspect DB contents
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute(f"select count(*) from {DEFAULT_TABLE}")
            count = cur.fetchone()[0]

        assert count == 1

    finally:
        # Step 8: cleanup after giving SQLite a moment to release locks
        time.sleep(0.1)
        shutil.rmtree(tmpdir, ignore_errors=True)
        shutil.rmtree(out_dir, ignore_errors=True)
