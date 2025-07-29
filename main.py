from Config.db_config import DB_CONFIG
from Core.database import OracleConnection
from Utils.export import export_to_csv
from Utils.timer import ElapsedTimer
from Utils.progress import ProgressTracker

import os
import time

def load_sql(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

def main():
    print("🔌 Connecting to Oracle DB...")
    db = OracleConnection(DB_CONFIG)

    try:
        db.connect()
        print("✅ Connected. Running query...")
        # Load the main SQL query
        sql_path = os.path.join("SQL", "vendor_master.sql")
        query = load_sql(sql_path)
        print("Running query from", sql_path)

        # Build COUNT query using static method from OracleConnection
        count_query = OracleConnection.build_count_query(query)
        # Execute COUNT query
        count_columns, count_rows = db.run_query(count_query)
        total_expected_rows = count_rows[0][0]  # COUNT(*) returns one row with one value
        print(f"🧮 Total expected rows: {total_expected_rows}")

        timer = ElapsedTimer()
        timer.start()

        progress = ProgressTracker(total_rows=total_expected_rows)

        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_file = f"vendor_master_{timestamp}.csv"

        total_rows = 0
        first_batch = True

        for columns, rows in db.run_in_batches(query, batch_size=10000):
            export_to_csv(columns, rows, filename=output_file, append=not first_batch, log_progress=False)
            total_rows += len(rows)
            first_batch = False
            # Update progress bar
            progress.update(total_rows)

        timer.stop()
        progress.stop()
        print(f"\n📊 Rows: {total_rows}")

    except Exception as e:
        print("❌ Error:", e)

    finally:
        db.close()
        print("🔒 Connection closed.")

if __name__ == "__main__":
    main()
