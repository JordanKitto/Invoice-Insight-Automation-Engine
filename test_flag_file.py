from Utils.flag_file import Flagfile

# Remove any existing flag
Flagfile.remove("done.txt")

# Create a new flag with a message
Flagfile.create(path="done.txt", message="Test run complete.")

# Confirm the file was created
with open("done.txt", "r", encoding="utf-8") as f:
    contents = f.read()

print("Contents of done.txt:", contents)
