# Test driver, credentials, network and session are good
def test_can_select_1(db):
    cur = db.conn.cursor()
    cur.execute("Select 1 from dual")
    row = cur.fetchone()
    cur.close()
    assert row is not None
    