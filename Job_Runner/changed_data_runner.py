# File: Job_Runner/changed_data_runner.py

import os

from Utils.changed_data_csv import run_changed_data_capture
from Utils.pretty_print import sub


class ChangedDataJob:
    """
    Job wrapper for the Changed Data capture process.

    This job is CSV-only and does not depend on the Oracle connection
    directly, but we accept `db` in run(...) for consistency with other jobs.
    """

    def __init__(
        self,
        tm_csv: str = os.path.join("Output_Files", "transaction_master.csv"),
        originals_csv: str = os.path.join(
            "Output_Files", "Original_Invoice_Data_CSV.csv"
        ),
        changed_csv: str = os.path.join("Output_Files", "Change_Invoice_Data_CSV.csv"),
    ) -> None:
        self.tm_csv = tm_csv
        self.originals_csv = originals_csv
        self.changed_csv = changed_csv

    def run(self, db=None) -> int:
        """
        Execute the Changed Data capture.

        Parameters
        ----------
        db : Core.database.OracleConnection or None
            Unused, present only for API compatibility with other jobs.

        Returns
        -------
        int
            Number of new rows appended to the Changed Data CSV.
        """
        rows = run_changed_data_capture(
            transaction_master_csv=self.tm_csv,
            originals_csv=self.originals_csv,
            changed_csv=self.changed_csv,
        )
        sub(f"[ChangedDataJob] Completed. New rows appended: {rows}")
        return rows
