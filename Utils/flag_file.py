import os

# The purpose of this class is to generate a .txt file named "done" that will be generated on successfully completeion of the SQL script
# Once the done.txt file is generated a Power Automate script will trigger off its creation thats used to automatically refresh Power Bi Reports

class Flagfile:
    @staticmethod
    def create(path="done.txt", message="SUCCESS"):
        with open(path, "w", encoding="utf-8") as f:
            f.write(message)
            print("done.txt created")

    @staticmethod
    def remove(path="done.txt"):
        if os.path.exists(path):
            os.remove(path)
