from Config.db_config import DB_CONFIG
from Core.database import OracleConnection
from Utils.export import export_to_csv
from Utils.progress import ProgressTracker
from Utils.flag_file import Flagfile
from Utils.timer import ElapsedTimer
import os
import time


class TransactionMasterJob:
    def __init__(self):
        self.sql_file = os.path.join("SQL", "transaction_master.sql")
        self.output_file = "transaction_master.csv"

    def run(self):
        print("Starting Transaction Master job...")
        # set up db connection with pre built config
        db = OracleConnection(DB_CONFIG)
        # start measuring how long the job takes to run
        timer = ElapsedTimer()
        timer.start()

        try:
            db.connect()
            print("✅ Connected to Oracle.")

            with open(self.sql_file, "r", encoding="utf-8") as f:
                query = f.read()
                print(f"📄 Loaded SQL from {self.sql_file}")

                count_query = OracleConnection.build_count_query(query)
                _, count_rows = db.run_query(count_query)
                total_expected_rows = count_rows[0][0]
                print(f"🧮 Estimated total rows: {total_expected_rows:,}")

                progress = ProgressTracker(total_rows=total_expected_rows)
                output_file = self.output_file
                total_rows_processed = 0
                first_batch = True

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
                    progress.update(total_rows_processed)

                timer.stop()
                progress.finish()
                print(f"\n📊 Rows exported: {total_rows_processed:,}")
                print(f"⏱️ Elapsed time: {timer.get_elapsed_time()}")

                if total_rows_processed == total_expected_rows:
                    Flagfile.create(path=os.path.join("Output_Files", "done.txt"),
                                    message="TRANSACTION MASTER COMPLETE")
        except Exception as e:
            print("❌ Vendor Master Error:", e)
        finally:
            db.close()
            print("🔒 Transaction Master Connection closed.")

def main():
    job = TransactionMasterJob()
    job.run()


if __name__ == "__main__":
    main()
        
                        




