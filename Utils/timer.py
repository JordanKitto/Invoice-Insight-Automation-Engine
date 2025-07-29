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

        