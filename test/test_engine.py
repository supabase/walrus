from sqlalchemy import column, func, literal, literal_column, select, text


def test_connect(sess):
    (x,) = sess.execute("select 1").fetchone()
    assert x == 1
