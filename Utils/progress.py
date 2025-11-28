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
        self.rows_processed = min(current_total_rows, self.total_rows)
        elapsed = time.time() - self.start_time

        # Elapsed time
        elapsed_mins, elapsed_secs = divmod(int(elapsed), 60)

        # ETA
        rate = self.rows_processed / elapsed if elapsed > 0 else 0
        remaining = (self.total_rows - self.rows_processed) / rate if rate > 0 else 0
        eta_mins, eta_secs = divmod(int(remaining), 60)
        
        print(
        f"Elapsed time: {elapsed_mins:02}:{elapsed_secs:02} | "
        f"Rows collected: {self.rows_processed:,} / {self.total_rows:,} | "
        f"ETA: {eta_mins:02}:{eta_secs:02}",
        end="\r",
        flush=True)
        sys.stdout.flush()  # Ensure immediate output

    def finish(self):
        # Move to a new line once processing is finished
        sys.stdout.write("\n")
        sys.stdout.flush()
