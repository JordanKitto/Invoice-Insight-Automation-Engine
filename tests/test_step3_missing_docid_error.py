# tests/test_step3_missing_docid_error.py
import os
import tempfile
import gc
import pandas as pd
import pytest

from Utils.originals_store import ensure_db, append_new_from_csv

def test_missing_docid_raises_clear_error():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "originals.db")
        out_dir = os.path.join(tmp, "Output_Files")
        os.makedirs(out_dir, exist_ok=True)

        ensure_db(db_path)

        # Intentionally omit DOCID column
        csv_path = os.path.join(out_dir, "transaction_master.csv")
        pd.DataFrame([{"VEND_ID": "V1"}]).to_csv(csv_path, index=False)

        with pytest.raises(ValueError) as ex:
            append_new_from_csv(csv_path, db_path=db_path)

        assert "DOCID column is required" in str(ex.value)

        # Help Windows release any lingering handles before temp cleanup
        gc.collect()
