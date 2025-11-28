import os
import time

from Job_Runner.vendor_master_runner import VendorMasterJob
from Job_Runner.transaction_master_runner import TransactionMasterJob
from Job_Runner.layout_master_runner import LayoutMasterJob
from Job_Runner.originals_capture_runner import OriginalsCaptureJob
from Job_Runner.changed_data_runner import ChangedDataJob
from Core.database import OracleConnection
from Config.db_config import DB_CONFIG
from Utils.pretty_print import step_header, sub


def run_step(step_name, runner_function, db):
    """
    Run a DB-driven step and wait for Output_Files/done.txt as a completion
    signal, with a short timeout. Used for Vendor, Transaction, and Layout.
    """
    step_header(f"STEP: {step_name}")

    # Run the job
    runner_function(db)

    done_file = os.path.join("Output_Files", "done.txt")
    timeout = 5  # seconds
    waited = 0

    while not os.path.exists(done_file) and waited < timeout:
        time.sleep(1)
        waited += 1

    if os.path.exists(done_file):
        sub(f"[orchestrator] {step_name} completed successfully.")
        os.remove(done_file)
        return True

    sub(f"[orchestrator] Timeout waiting for {done_file}. Skipping remaining steps.")
    return False


def main():
    db = OracleConnection(DB_CONFIG)
    print("Connecting to DB")

    try:
        db.connect()
        print("DB connection successful")

        vendor_job = VendorMasterJob()
        transaction_job = TransactionMasterJob()
        layout_job = LayoutMasterJob()
        originals_job = OriginalsCaptureJob()
        changed_job = ChangedDataJob()

        # # 1. Vendor Master (DB-driven, uses done.txt)
        if not run_step("Vendor Master Export", vendor_job.run, db):
            return

        # # # 2. Transaction Master (DB-driven, uses done.txt)
        if not run_step("Transaction Master Export", transaction_job.run, db):
            return

        # # 3. Originals Capture (CSV-only; no done.txt, header printed inside)
        originals_job.run(db)

        # # 4. Changed Data Capture (CSV-only; no done.txt, header printed inside)
        changed_job.run(db)

        # # # 5. Layout Master (DB-driven, uses done.txt)
        if not run_step("Layout Master Export", layout_job.run, db):
            return

    finally:
        try:
            db.close()
        except Exception:
            pass
        print("DB connection closed.")


if __name__ == "__main__":
    main()
