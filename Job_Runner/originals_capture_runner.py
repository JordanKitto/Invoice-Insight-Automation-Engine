# File: Job_Runner/originals_capture_runner.py

import os
import sys

from Utils.pretty_print import sub
from Utils.originals_capture_csv import run_originals_capture

# 1. Ensure project root is on PYTHONPATH BEFORE importing Utils
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

print(f"[runner] Project root on sys.path: {ROOT}")


class OriginalsCaptureJob:
    """
    PIOR job wrapper for the Originals CSV capture.

    It assumes:
    - Transaction Master CSV is written to Output_Files/transaction_master.csv
    - Originals CSV lives at Output_Files/Original_Invoice_Data_CSV.csv
    """

    def __init__(
        self,
        tm_csv: str = os.path.join("Output_Files", "transaction_master.csv"),
        originals_csv: str = os.path.join(
            "Output_Files", "Original_Invoice_Data_CSV.csv"
        ),
        days_back: int = 30,
    ):
        self.tm_csv = tm_csv
        self.originals_csv = originals_csv
        self.days_back = days_back

    def run(self, db=None) -> int:
        """
        db argument is accepted for consistency with other jobs,
        but not used because this job only deals with CSVs.

        Returns
        -------
        int
            Number of rows written to Originals.
        """
        written = run_originals_capture(
            transaction_master_csv=self.tm_csv,
            originals_csv=self.originals_csv,
            days_back=self.days_back,
        )
        sub(f"[OriginalsCaptureJob] Completed. Rows written: {written}")
        return written


def main():
    """
    Allow this runner to be executed directly from the command line.
    """
    print("[runner] main() starting")

    job = OriginalsCaptureJob(
        tm_csv=os.path.join("Output_Files", "transaction_master.csv"),
        originals_csv=os.path.join(
            "Output_Files", "Original_Invoice_Data_CSV.csv"
        ),
        days_back=31,  # adjust window if needed
    )
    job.run(db=None)

    print("[runner] main() finished")


if __name__ == "__main__":
    print("[runner] __name__ == '__main__' -> calling main()")
    main()
