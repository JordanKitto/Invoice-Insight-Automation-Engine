from Config.db_config import DB_CONFIG
from Core.database import OracleConnection
from Utils.export import export_to_csv
from Utils.timer import ElapsedTimer
from Utils.progress import ProgressTracker

import os
import time

# Load SQL query text from a file path
def load_sql(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

# Main entry point for the batch export job
def main():
    print("🔌 Connecting to Oracle DB...")
    db = OracleConnection(DB_CONFIG)

    try:
        # Establish database connection
        db.connect()
        print("✅ Connected. Running query...")

        # Load main SQL file from the SQL directory
        sql_path = os.path.join("SQL", "vendor_master.sql")
        query = load_sql(sql_path)
        print("Running query from", sql_path)

        # Run COUNT(*) version of query to estimate total rows for progress tracking
        count_query = OracleConnection.build_count_query(query)
        count_columns, count_rows = db.run_query(count_query)
        total_expected_rows = count_rows[0][0]  # Extract COUNT value
        print(f"🧮 Total expected rows: {total_expected_rows}")

        # Start elapsed timer in background
        timer = ElapsedTimer()
        timer.start()

        # Set up progress tracker with known total rows
        progress = ProgressTracker(total_rows=total_expected_rows)

        # Prepare output file with timestamp
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_file = f"vendor_master_{timestamp}.csv"

        total_rows = 0
        first_batch = True

        # Fetch and export data in batches
        for columns, rows in db.run_in_batches(query, batch_size=10000):
            export_to_csv(
                columns,
                rows,
                filename=output_file,
                append=not first_batch,  # Append from second batch onwards
                log_progress=False
            )
            total_rows += len(rows)
            first_batch = False

            # Update progress bar with current row count
            progress.update(total_rows)

        # Stop progress indicators
        timer.stop()
        progress.finish()
        print(f"\n📊 Rows: {total_rows}")

    except Exception as e:
        print("❌ Error:", e)

    finally:
        db.close()
        print("🔒 Connection closed.")

# Script execution entry point
if __name__ == "__main__":
    main()
