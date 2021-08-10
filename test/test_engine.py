from sqlalchemy import text, func, select, literal_column, column, literal

def test_connect(sess):
    (x,) = sess.execute("select 1").fetchone()
    assert x == 1
