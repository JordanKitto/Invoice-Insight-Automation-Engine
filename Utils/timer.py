import time

class ElapsedTimer:
    def __init__(self):
        self.start_time = None
        self.end_time = None

    def start(self):
        self.start_time = time.time()

    def stop(self):
        self.end_time = time.time()

    def get_elapsed_time(self):
        if self.start_time is None or self.end_time is None:
            return "00:00:000"
        elapsed = self.end_time - self.start_time
        mins, secs = divmod(int(elapsed), 60)
        millis = int((elapsed - int(elapsed)) * 1000)
        return f"{mins:02d}:{secs:02d}"
