import os
from dotenv import load_dotenv

# load .env file
env_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path="Config/.env")


# Extract values into a dictonary
DB_CONFIG = {
    "hostname": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 1521)),
    "service_name": os.getenv("DB_SERVICE"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
}

