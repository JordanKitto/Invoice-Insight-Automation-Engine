import re
from pathlib import Path

TARGETS = [
    "layout_master.sql",
    "transaction_master.sql",
    "vendor_master.sql"
]

def _is_select(sql: str) -> bool:
    return re.match(r"^\s*(with|select)\b", sql, re.IGNORECASE) is not None

def _limit1(sql: str) -> str:
    """
    wrap any SELECT to return a single row.
    Avoids full scans while hitting hte parser, optimiser, and permissions.
    """
    s = sql.strip().rstrip(";")

    if not re.search(r"\b(WHERE|HAVING|GROUP BY|ORDER BY)\b", s, re.IGNORECASE):
        # Inject WHERE ROWNUM <= 1 before any potential ORDER BY/GROUP BY etc.
        # Note: the test query you provided already had a WHERE clause, so this path likely isn't taken.
        return f"{s} WHERE ROWNUM <= 1"
    
    if re.search(r"\bORDER BY\b", s, re.IGNORECASE):
        return re.sub(r"(\bORDER BY\b)", r" AND ROWNUM <= 1 \1", s, flags=re.IGNORECASE)
    else:
        # If no ORDER BY, just append it to the end of the existing WHERE clause
        return f"{s} AND ROWNUM <= 1"

def test_each_sql_compiles_and_returns_one_row(db, sql_dir):
    files = [Path(sql_dir, name) for name in TARGETS if Path(sql_dir, name).exists()]
    if not files:
        # Repo may not have all scripts yet
        return
    for path in files:
        sql = path.read_text(encoding="utf-8")
        assert _is_select(sql), f"{path.name} must start with WITH or SELECT"
        smoke_sql = _limit1(sql)
        # Use a direct curosr to avoid fetching all rows via run_query
        cur = db.conn.cursor()
        cur.execute(smoke_sql)
        row = cur.fetchone()
        cur.close()
        assert row is not None, f"No rows for {path.name}"
