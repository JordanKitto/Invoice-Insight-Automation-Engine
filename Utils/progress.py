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

    def update(self, batch_size: int):
        self.rows_processed += batch_size  # Increment rows processed by the current batch size
        elapsed = time.time() - self.start_time  # Time elapsed since processing began
        percent = (self.rows_processed / self.total_rows) * 100 if self.total_rows else 0  # Completion %
        rate = self.rows_processed / elapsed if elapsed > 0 else 0  # Rows per second processing rate
        remaining = (self.total_rows - self.rows_processed) / rate if rate > 0 else 0  # Estimated time remaining

        # Convert remaining time to minutes, seconds, milliseconds
        mins, secs = divmod(int(remaining), 60)
        millis = int((remaining - int(remaining)) * 1000)
        
        # Write progress line to terminal (overwrite each time with \r)
        sys.stdout.write(
            f"\rProgress: {percent:5.1f}% | Rows: {self.rows_processed:,}/{self.total_rows:,} | "
            f"ETA: {mins:02d}:{secs:02d}:{millis:03}"
        )
        sys.stdout.flush()  # Ensure immediate output

    def finish(self):
        # Move to a new line once processing is finished
        sys.stdout.write("\n")
        sys.stdout.flush()
