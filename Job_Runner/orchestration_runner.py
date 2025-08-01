from Job_Runner.vendor_master_runner import VendorMasterJob
from Job_Runner.transaction_master_runner import TransactionMasterJob
from Core.database import OracleConnection
from Config.db_config import DB_CONFIG

import os
import time

def __init__(self, db):
    self.db = db

def run_step(step_name, runner_function):
    print(f"Starting step: {step_name}")
    runner_function()

    # Wait for corresponding done.txt
    done_file = os.path.join("Output_Files", "done.txt")
    timeout = 5 # seconds
    waited = 0
    while not os.path.exists(done_file) and waited < timeout:
        print(f"\rWaiting for {done_file}...")
        time.sleep(1)
        waited += 1

    if os.path.exists(done_file):
        print(f"{step_name} completed successfully.\n")
        os.remove(done_file)
    else:
        print(f"Timeout waiting for {done_file} - skipping remaining steps")
        return False
    return True

def main():
    db = OracleConnection(DB_CONFIG)
    print("Connecting to DB")
    try:
        db.connect()
        print("DB connection successful")

        if not run_step("Vendor Master Export", VendorMasterJob(), db):
            return
        if not run_step("Transaction Master Export", TransactionMasterJob(), db):
            return
    finally:
        db.close
        print("DB connection closed.")
    
if __name__ == "__main__":
    main()

