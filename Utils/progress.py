import time
import sys

class ProgressTracker:
    def __init__(self, total_rows: int):
        self.total_rows = total_rows
        self.rows_processed = 0
        self.start_time = time.time()

    def update(self, batch_size: int):
        self.rows_processed += batch_size
        elapsed = time.time() - self.start_time
        percent = (self.rows_processed / self.total_rows) * 100 if self.total_rows else 0
        rate = self.rows_processed / elapsed if elapsed > 0 else 0
        remaining = (self.total_rows - self.rows_processed) / rate if rate > 0 else 0

        mins, secs = divmod(int(remaining), 60)
        millis = int((remaining - int(remaining)) * 1000)
        
        sys.stdout.write(
            f"\rProgress: {percent:5.1f}% | Rows: {self.rows_processed:,}/{self.total_rows:,} | "
            f"ETA: {mins:02d}:{secs:02d}:{millis:03}"
        )
        sys.stdout.flush()

    def finish(self):
        sys.stdout.write("\n")
        sys.stdout.flush()
