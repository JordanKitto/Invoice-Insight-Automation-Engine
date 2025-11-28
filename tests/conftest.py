import pytest # need to install library (pip install pytest)
import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from Config.db_config import DB_CONFIG
from Core.database import OracleConnection

# Point to the SQL directory for later tests
SQL_DIR = pathlib.Path(__file__).resolve().parents[1] / "SQL"

@pytest.fixture(scope="session")
def sql_dir():
    assert SQL_DIR.exists(), f"SQL Folder not found at {SQL_DIR}"
    return SQL_DIR

@pytest.fixture(scope="session")
def db():
    """
    One real Oracle connection for the whole test session.
    Uses your existing OracleConnection and DB_CONFIG.
    """

    conn = OracleConnection(DB_CONFIG)
    conn.connect()
    try:
        yield conn
    finally:
        conn.close()

