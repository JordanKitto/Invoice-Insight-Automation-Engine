# relates to: tests/test_step1_ensure_db.py
import os
import sqlite3
import tempfile
import gc
from Utils.originals_store import ensure_db, DEFAULT_TABLE

def test_ensure_db_creates_file_and_table():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "originals.db")

        # Act
        ensure_db(db_path)

        # Assert: file created
        assert os.path.exists(db_path)

        # Assert: table exists and make sure all handles are closed explicitly
        conn = sqlite3.connect(db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "select name from sqlite_master where type='table' and name=?",
                (DEFAULT_TABLE,),
            )
            assert cur.fetchone() is not None
        finally:
            cur.close()
            conn.close()

        # On Windows, force GC so the file handle is fully released before cleanup
        gc.collect()
