import time
import sys

# ProgressTracker is used to monitor and display progress during batch data processing.
# It provides real-time updates on:
#   - The percentage of total rows processed
#   - The number of rows processed vs. total
#   - The estimated time remaining (ETA) to complete processing
# It is typically called during each batch fetch to display continuous terminal feedback.
class ProgressTracker:
    def __init__(self, total_rows: int):
        self.total_rows = total_rows  # Total number of rows expected to be processed
        self.rows_processed = 0  # Counter for how many rows have been processed so far
        self.start_time = time.time()  # Timestamp marking the beginning of processing

    def update(self, current_total_rows: int):
        self.rows_processed = min(current_total_rows, self.total_rows)  # Clamp to avoid overflow
        elapsed = time.time() - self.start_time
        percent = (self.rows_processed / self.total_rows) * 100 if self.total_rows else 0
        rate = self.rows_processed / elapsed if elapsed > 0 else 0
        remaining = (self.total_rows - self.rows_processed) / rate if rate > 0 else 0

        mins, secs = divmod(int(remaining), 60)
        millis = int((remaining - int(remaining)) * 1000)

        print(
            f"\rRows: {self.rows_processed:,}/{self.total_rows:,} "
            f"ETA: {mins:02d}:{secs:02d}:{millis:03d}",
            end=""
        )
        sys.stdout.flush()  # Ensure immediate output

    def finish(self):
        # Move to a new line once processing is finished
        sys.stdout.write("\n")
        sys.stdout.flush()
