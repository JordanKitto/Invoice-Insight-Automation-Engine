from Config.db_config import DB_CONFIG
from Core.database import OracleConnection
import os
import time
import threading
import sys

class ElapsedTimer(threading.Thread):
    def __init__(self):
        super().__init__()
        self.start_time = time.time()
        self.running = True
        self.daemon = True

    def run(self):
        while self.running:
            elapsed = time.time() - self.start_time
            mins, secs = divmod(int(elapsed), 60)
            millis = int((elapsed - int(elapsed)) * 1000)
            sys.stdout.write(f"\rElapsed time: {mins:02d}:{secs:02d}:{millis:03}")
            sys.stdout.flush()
            time.sleep(0.05)
            
    def stop(self):
        self.running = False
        time.sleep(0.1)
        sys.stdout.write("\n")
        sys.stdout.flush()

def load_sql(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

def main():
    print("Connecting to Oracle DB...")
    db = OracleConnection(DB_CONFIG)
    try:
        db.connect()
        print("Connection Successful...")

        # Load SQL from file

        sql_path = os.path.join("SQL", "vendor_master.sql")
        query = load_sql(sql_path)

        print("Running query from: ", sql_path)
        timer = ElapsedTimer()
        timer.start()
        columns, rows = db.run_query(query)



        # print("Columns:", columns)
        # print("Rows: ", rows) # Show just a preview

        timer.stop()
        print(f"Rows returned: {len(rows)}")

    except Exception as e:
        print("Error: ", e)

    finally:
        db.close()
        print("Connection closed.")


if __name__ == "__main__":
    main()