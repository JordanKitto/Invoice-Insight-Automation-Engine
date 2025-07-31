from Config.db_config import DB_CONFIG
from Core.database import OracleConnection
from Utils.export import export_to_csv
from Utils.timer import ElapsedTimer
from Utils.progress import ProgressTracker
from Utils.flag_file import Flagfile

import os
import time

# Load SQL query text from a file path
def load_sql(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

# Main entry point for the batch export job
def main():

    # remove done.txt file if it already exists
    done_path = os.path.join("Output_files", "done.txt")
    Flagfile.remove(done_path)

    print("🔌 Connecting to Oracle DB...")
    db = OracleConnection(DB_CONFIG)

    timer = ElapsedTimer()
    timer.start()

    try:
        # Establish database connection
        db.connect()
        print("✅ Connected. Running query...")

        # Load main SQL file from the SQL directory
        sql_path = os.path.join("SQL", "transaction_master.sql")
        query = load_sql(sql_path)
        print("Running query from", sql_path)

        # Run COUNT(*) version of query to estimate total rows
        count_query = OracleConnection.build_count_query(query)
        _, count_rows = db.run_query(count_query)
        total_expected_rows = count_rows[0][0]
        print(f"🧮 Total expected rows: {total_expected_rows:,}")

        progress = ProgressTracker(total_rows=total_expected_rows)

        # Prepare output file
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_file = f"transaction_master.csv"

        total_rows_processed = 0
        first_batch = True

    
        # Batch fetch and export loop
        for columns, rows in db.run_in_batches(query, batch_size=10000):
            export_to_csv(
                columns,
                rows,
                filename=output_file,
                append=not first_batch,
                log_progress=False
            )

            total_rows_processed += len(rows)
            first_batch = False

            # Update progress tracker
            progress.update(total_rows_processed)

        # Finalize
        timer.stop()
        progress.finish()
        print(f"\n📊 Rows exported: {total_rows_processed:,}")
        print(f"⏱️ Elapsed time: {timer.get_elapsed_time()}")
        
        # Run done.txt function after script has been exported
        if total_rows_processed == total_expected_rows:
            done_path = os.path.join("Output_files", "done.txt")
            Flagfile.create(path=done_path, message="VENDOR MASTER EXPORT COMPLETE")
            
    except Exception as e:
        print("❌ Error:", e)

    finally:
        db.close()
        # Remove existing done.txt before run starts
        print("🔒 Connection closed.")

# Script execution entry point
if __name__ == "__main__":
    main()
